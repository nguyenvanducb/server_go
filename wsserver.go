package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// Danh sách tất cả client đang kết nối
var clients = make(map[*websocket.Conn]bool)

// Channel để truyền dữ liệu từ WebSocket TCBS đến client frontend
var broadcast = make(chan []byte)

// Cấu hình nâng cấp HTTP thành WebSocket
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Cho phép tất cả nguồn (bạn có thể giới hạn sau nếu cần)
	},
}

// Xử lý kết nối mới từ client
func handleConnections(w http.ResponseWriter, r *http.Request) {
	// w là http.ResponseWriter: cánh cổng để bạn trả dữ liệu lại cho client
	// r là *http.Request: request từ client gửi đến

	// Nâng cấp HTTP lên WebSocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("❌ Lỗi khi nâng cấp kết nối:", err)
		return
	}
	defer ws.Close()

	// Lưu client mới vào danh sách
	clients[ws] = true
	log.Printf("📡 Client kết nối: %v (Tổng: %d)", ws.RemoteAddr(), len(clients))

	// Lắng nghe message từ client (không xử lý nội dung)
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			log.Printf("⚠️ Client ngắt kết nối: %v", ws.RemoteAddr())
			delete(clients, ws)
			break
		}
	}
}

// Gửi dữ liệu đến tất cả client
func handleMessages() {
	for {
		msg := <-broadcast
		for client := range clients {
			err := client.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("⚠️ Lỗi gửi đến client %v: %v", client.RemoteAddr(), err)
				client.Close()
				delete(clients, client)
			}
		}
	}
}

// Hàm khởi chạy WebSocket server
func startWebSocketServer() {
	// 👉 Khi có yêu cầu HTTP đến đường dẫn /ws, server sẽ gọi hàm handleConnections để xử lý.
	http.HandleFunc("/ws", handleConnections)

	go handleMessages()

	log.Println("🟢 WebSocket server đang chạy tại ws://localhost:9999/ws")
	if err := http.ListenAndServe(":9999", nil); err != nil {
		log.Fatal("❌ Lỗi khi chạy WebSocket server:", err)
	}
}
