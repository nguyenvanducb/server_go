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

		bm := &BatchManager{coll: db.Collection(Collection)}
		obm := &OrderBatchManager{coll: db.Collection(CollectionOrder)}

		// Gửi thông tin xác thực
		base64Token := base64.StdEncoding.EncodeToString([]byte(token))
		authMsg := fmt.Sprintf("d|a|||%s", base64Token)
		conn.WriteMessage(websocket.TextMessage, []byte(authMsg))

		// Gửi yêu cầu subscribe mã cổ phiếu
		subMsg := fmt.Sprintf("d|s|tk|bp+bi+tm+op+fe|%s", strings.Join(symbols, ","))
		conn.WriteMessage(websocket.TextMessage, []byte(subMsg))

		// Gửi ping mỗi 10s
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				conn.WriteMessage(websocket.TextMessage, []byte("d|p|||"))
			}
		}()

		// Đọc dữ liệu liên tục
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ Mất kết nối WebSocket TCBS: %v", err)
				conn.Close()
				break
			}
			handleTCBSMessage(string(msg), bm, obm)
		}
	}
}

// ✅ Xử lý dữ liệu với logic mapStock như code cũ
func handleTCBSMessage(message string, bm *BatchManager, obm *OrderBatchManager) {
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
		log.Println("❌ Lỗi parse JSON:", err)
		return
	}

	// Thêm timestamp
	data["time"] = time.Now()

	// ✅ Áp dụng logic mapData như code cũ
	mapData(code, data, bm, obm)

	// Gửi tới client đang kết nối qua WebSocket server (port 8888)
	broadcast <- []byte(jsonStr)
}

// ✅ Hàm mapData - tích lũy dữ liệu theo symbol như code cũ
func mapData(code string, jsonData map[string]interface{}, bm *BatchManager, obm *OrderBatchManager) {
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

	// ✅ Lưu vào stock_code collection (tất cả message)
	bm.Add(mapStock[symbol])

	// ✅ CHỈ KHI code là "s|6" mới lưu vào orders collection
	if code == "s|6" {
		// Tạo bản copy để tránh race condition
		orderData := make(map[string]interface{})
		for k, v := range mapStock[symbol] {
			orderData[k] = v
		}
		obm.Add(orderData)
		log.Printf("📥 Đưa vào batch Order cho symbol: %s", symbol)
	}
	mapStockMux.Unlock()
}
