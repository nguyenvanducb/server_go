package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/mongo"
)

const WebsocketURL = "wss://openapi.tcbs.com.vn/ws/thesis/v1/stream/normal"

// ✅ Thêm mapStock để tích lũy dữ liệu theo symbol
var (
	mapStock    = make(map[string]map[string]interface{})
	mapStockMux sync.RWMutex
)

// Kết nối tới WebSocket của TCBS và xử lý dữ liệu
func startWebSocket(symbols []string, token string, db *mongo.Database) {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(WebsocketURL, nil)
		if err != nil {
			log.Printf("❌ Lỗi kết nối WebSocket TCBS: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Println("✅ Đã kết nối WebSocket TCBS")

		// ✅ Sử dụng UltraFastBatchManager thay vì OptimizedBatchManager
		bm := NewUltraFastBatchManager(db.Collection(Collection))
		obm := NewUltraFastOrderBatchManager(db.Collection(CollectionOrder))

		// ✅ Cleanup khi đóng connection - sử dụng goroutine để không block
		cleanupDone := make(chan bool)
		defer func() {
			go func() {
				log.Println("🔄 Đang cleanup batch managers...")
				bm.Close()
				obm.Close()
				log.Println("✅ Cleanup hoàn tất")
				cleanupDone <- true
			}()

			// ✅ Đợi cleanup hoàn tất với timeout
			select {
			case <-cleanupDone:
				// Cleanup completed
			case <-time.After(5 * time.Second):
				log.Println("⚠️ Cleanup timeout")
			}
		}()

		// Gửi thông tin xác thực
		base64Token := base64.StdEncoding.EncodeToString([]byte(token))
		authMsg := fmt.Sprintf("d|a|||%s", base64Token)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(authMsg)); err != nil {
			log.Printf("❌ Lỗi gửi auth message: %v", err)
			conn.Close()
			continue
		}

		// Gửi yêu cầu subscribe mã cổ phiếu
		subMsg := fmt.Sprintf("d|s|tk|bp+bi+tm+op+fe|%s", strings.Join(symbols, ","))
		if err := conn.WriteMessage(websocket.TextMessage, []byte(subMsg)); err != nil {
			log.Printf("❌ Lỗi gửi subscribe message: %v", err)
			conn.Close()
			continue
		}

		// ✅ Goroutine để gửi ping định kỳ
		pingDone := make(chan struct{})
		go func() {
			defer close(pingDone)
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if err := conn.WriteMessage(websocket.TextMessage, []byte("d|p|||")); err != nil {
						log.Printf("❌ Lỗi gửi ping: %v", err)
						return
					}
				case <-pingDone:
					return
				}
			}
		}()

		// ✅ Đọc dữ liệu liên tục với error handling
		connectionBroken := false
		messageCount := 0
		startTime := time.Now()

		for !connectionBroken {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ Mất kết nối WebSocket TCBS: %v", err)
				connectionBroken = true
				break
			}

			messageCount++

			// ✅ Log stats mỗi 1000 messages
			if messageCount%1000 == 0 {
				duration := time.Since(startTime)
				rate := float64(messageCount) / duration.Seconds()
				log.Printf("📊 WebSocket Stats: %d messages in %v (%.2f msgs/sec)", messageCount, duration, rate)
			}

			// ✅ Xử lý message trong goroutine riêng để không block read loop
			go handleTCBSMessage(string(msg), bm, obm)
		}

		// ✅ Cleanup khi connection bị đứt
		close(pingDone)
		conn.Close()

		// ✅ Đợi một chút trước khi reconnect
		log.Println("🔄 Sẽ thử kết nối lại sau 3 giây...")
		time.Sleep(3 * time.Second)
	}
}

// ✅ Xử lý dữ liệu với UltraFastBatchManager
func handleTCBSMessage(message string, bm *UltraFastBatchManager, obm *UltraFastOrderBatchManager) {
	if strings.HasPrefix(message, "d|0|") {
		// Tin nhắn xác thực hoặc không chứa dữ liệu
		return
	}

	// ✅ Lấy code từ đầu message (3 ký tự đầu)
	if len(message) < 4 {
		return
	}
	code := message[:3]

	start := strings.Index(message, "{")
	end := strings.LastIndex(message, "}")
	if start == -1 || end == -1 || start >= end {
		return
	}

	jsonStr := message[start : end+1]

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		log.Printf("❌ Lỗi parse JSON: %v", err)
		return
	}

	// ✅ Thêm timestamp với millisecond precision
	data["time"] = time.Now()
	data["receivedAt"] = time.Now().UnixMilli() // ✅ Thêm timestamp dạng số

	// ✅ Áp dụng logic mapData với UltraFastBatchManager
	mapData(code, data, bm, obm)

	// ✅ Gửi tới client đang kết nối qua WebSocket server (port 9999) - non-blocking
	select {
	case broadcast <- []byte(jsonStr):
		// Successfully sent to broadcast channel
	default:
		// Broadcast channel full, skip this message to avoid blocking
		// ✅ Comment để tránh spam log
		// log.Println("⚠️ Broadcast channel full, skipping message")
	}
}

// ✅ Hàm mapData với UltraFastBatchManager
func mapData(code string, jsonData map[string]interface{}, bm *UltraFastBatchManager, obm *UltraFastOrderBatchManager) {
	symbolRaw, exists := jsonData["symbol"]
	if !exists {
		// ✅ Không log cho mỗi message thiếu symbol để tránh spam
		return
	}

	symbol, ok := symbolRaw.(string)
	if !ok {
		return
	}

	// ✅ Optimized lock với RLock cho read operations
	mapStockMux.RLock()
	_, found := mapStock[symbol]
	mapStockMux.RUnlock()

	mapStockMux.Lock()
	defer mapStockMux.Unlock()

	// ✅ Cập nhật mapStock - tích lũy dữ liệu
	if !found {
		// Tạo mới nếu chưa có symbol
		mapStock[symbol] = make(map[string]interface{})
		for k, v := range jsonData {
			mapStock[symbol][k] = v
		}
	} else {
		// Merge dữ liệu mới vào dữ liệu cũ
		for k, v := range jsonData {
			mapStock[symbol][k] = v
		}
	}

	// ✅ Tạo bản copy để gửi đến batch managers (tránh race condition)
	stockData := make(map[string]interface{})
	for k, v := range mapStock[symbol] {
		stockData[k] = v
	}

	// ✅ Lưu vào stock_code collection (tất cả message) - ultra fast, non-blocking
	bm.Add(stockData)

	// ✅ CHỈ KHI code là "s|6" mới lưu vào orders collection
	if code == "s|6" {
		// Tạo bản copy riêng cho order data
		orderData := make(map[string]interface{})
		for k, v := range stockData {
			orderData[k] = v
		}
		obm.Add(orderData)
		// ✅ Giảm log để tránh spam
		// log.Printf("📥 Đưa vào batch Order cho symbol: %s", symbol)
	}
}

// ✅ Graceful shutdown function với UltraFastBatchManager
func gracefulShutdown(bm *UltraFastBatchManager, obm *UltraFastOrderBatchManager) {
	log.Println("🛑 Graceful shutdown initiated...")

	// ✅ Create a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan bool, 2)

	// ✅ Shutdown batch managers in parallel
	if bm != nil {
		go func() {
			bm.Close()
			done <- true
		}()
	} else {
		done <- true
	}

	if obm != nil {
		go func() {
			obm.Close()
			done <- true
		}()
	} else {
		done <- true
	}

	// ✅ Wait for both to complete or timeout
	completed := 0
	for completed < 2 {
		select {
		case <-done:
			completed++
		case <-ctx.Done():
			log.Println("⚠️ Graceful shutdown timeout")
			return
		}
	}

	log.Println("✅ Graceful shutdown completed")
}
