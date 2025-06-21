package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func main() {
	// ✅ Khởi tạo danh sách mã chứng khoán từ các nhóm
	initSymbols()

	// ✅ Nhận OTP từ người dùng
	otp := getUserInput("📥 Nhập OTP: ")

	// ✅ Lấy access token từ TCBS
	token, err := GetAccessToken("10000717062-85bbf26d-7365-414f-ba3f-956a122c726b", otp)
	if err != nil {
		log.Fatal("❌ Lỗi lấy access token:", err)
	}

	// ✅ Kết nối MongoDB
	client := connectMongoDB()
	db := client.Database(DBName)

	// ✅ Bắt đầu WebSocket server để phục vụ client kết nối đến localhost:8888/ws
	go startWebSocketServer()

	// ✅ Kết nối đến WebSocket TCBS và xử lý dữ liệu
	go startWebSocket(allSymbols, token, db)

	// ✅ Giữ chương trình chạy vĩnh viễn
	select {}
}

// Nhập từ bàn phím
func getUserInput(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	return input[:len(input)-1]
}
