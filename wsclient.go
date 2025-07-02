package main

import (
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

		// ✅ Sử dụng optimized batch managers
		bm := NewOptimizedBatchManager(db.Collection(Collection))
		obm := NewOptimizedOrderBatchManager(db.Collection(CollectionOrder))

		// ✅ Cleanup khi đóng connection
		defer func() {
			log.Println("🔄 Đang cleanup batch managers...")
			bm.Close()
			obm.Close()
			log.Println("✅ Cleanup hoàn tất")
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
		for !connectionBroken {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ Mất kết nối WebSocket TCBS: %v", err)
				connectionBroken = true
				break
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

// ✅ Xử lý dữ liệu với optimized managers
func handleTCBSMessage(message string, bm *OptimizedBatchManager, obm *OptimizedOrderBatchManager) {
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

	// Thêm timestamp
	data["time"] = time.Now()

	// ✅ Áp dụng logic mapData với optimized managers
	mapData(code, data, bm, obm)

	// ✅ Gửi tới client đang kết nối qua WebSocket server (port 8888)
	select {
	case broadcast <- []byte(jsonStr):
		// Successfully sent to broadcast channel
	default:
		// Broadcast channel full, skip this message to avoid blocking
		log.Println("⚠️ Broadcast channel full, skipping message")
	}
}

// ✅ Hàm mapData với optimized managers
func mapData(code string, jsonData map[string]interface{}, bm *OptimizedBatchManager, obm *OptimizedOrderBatchManager) {
	symbolRaw, exists := jsonData["symbol"]
	if !exists {
		log.Println("❌ Không có trường 'symbol'")
		return
	}

	symbol, ok := symbolRaw.(string)
	if !ok {
		log.Println("❌ 'symbol' không phải kiểu string")
		return
	}

	mapStockMux.Lock()
	defer mapStockMux.Unlock() // ✅ Sử dụng defer để đảm bảo unlock

	// ✅ Cập nhật mapStock - tích lũy dữ liệu
	if _, found := mapStock[symbol]; !found {
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

	// ✅ Lưu vào stock_code collection (tất cả message) - non-blocking
	bm.Add(stockData)

	// ✅ CHỈ KHI code là "s|6" mới lưu vào orders collection
	if code == "s|6" {
		// Tạo bản copy riêng cho order data
		orderData := make(map[string]interface{})
		for k, v := range stockData {
			orderData[k] = v
		}
		obm.Add(orderData)
		log.Printf("📥 Đưa vào batch Order cho symbol: %s", symbol)
	}
}

// ✅ Graceful shutdown function (optional - để cleanup khi app shutdown)
func gracefulShutdown(bm *OptimizedBatchManager, obm *OptimizedOrderBatchManager) {
	log.Println("🛑 Graceful shutdown initiated...")

	// Wait for all pending operations to complete
	if bm != nil {
		bm.Close()
	}
	if obm != nil {
		obm.Close()
	}

	log.Println("✅ Graceful shutdown completed")
}
