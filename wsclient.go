package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/mongo"
)

const WebsocketURL = "wss://openapi.tcbs.com.vn/ws/thesis/v1/stream/normal"

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

// Xử lý dữ liệu JSON từ WebSocket TCBS
func handleTCBSMessage(message string, bm *BatchManager, obm *OrderBatchManager) {
	if strings.HasPrefix(message, "d|0|") {
		// Tin nhắn không chứa dữ liệu
		return
	}

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

	data["time"] = time.Now()

	// Lưu vào MongoDB
	bm.Add(data)
	if strings.HasPrefix(message, "s|6") {
		obm.Add(data)
	}

	// Gửi tới client đang kết nối qua WebSocket server (port 8888)
	broadcast <- []byte(jsonStr)
}
