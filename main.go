package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoDBURI   = "mongodb+srv://hoangminhtri99:Triminh96@cluster0.lu5ww.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
	DBName       = "moneyflow"
	Collection   = "stock_code"
	WebsocketURL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
)

// Struct chứa dữ liệu giao dịch từ WebSocket
type TradeMessage struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	TradeID   int64  `json:"t"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
}

func main() {
	// 1. Kết nối MongoDB
	clientOptions := options.Client().ApplyURI(MongoDBURI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal("❌ Lỗi khi kết nối MongoDB:", err)
	}
	defer client.Disconnect(context.TODO())

	// Kiểm tra kết nối MongoDB
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal("❌ Không thể ping đến MongoDB:", err)
	}
	fmt.Println("✅ Kết nối thành công đến MongoDB!")

	collection := client.Database(DBName).Collection(Collection)

	// 2. Kết nối WebSocket và lắng nghe
	for {
		fmt.Println("🔄 Kết nối lại WebSocket...")

		conn, _, err := websocket.DefaultDialer.Dial(WebsocketURL, nil)
		if err != nil {
			fmt.Println("❌ Không thể kết nối WebSocket:", err)
			time.Sleep(5 * time.Second) // Thử lại sau 5s
			continue
		}
		fmt.Println("✅ Kết nối WebSocket thành công!")

		// Gửi ping định kỳ để giữ kết nối sống
		go func() {
			for {
				time.Sleep(30 * time.Second)
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					fmt.Println("⚠️ Lỗi gửi ping:", err)
					conn.Close()
					return
				}
			}
		}()

		// Lắng nghe tin nhắn WebSocket
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				fmt.Println("🔌 Mất kết nối WebSocket:", err)
				conn.Close()
				break // Thoát khỏi vòng lặp để thử kết nối lại
			}

			var trade TradeMessage
			if err := json.Unmarshal(message, &trade); err != nil {
				fmt.Println("❌ Lỗi giải mã JSON:", err)
				continue
			}

			// Chỉ cập nhật nếu là BTCUSDT
			if trade.Symbol != "BTCUSDT" {
				continue
			}

			// Cập nhật dữ liệu vào MongoDB
			filter := bson.M{"symbol": trade.Symbol}
			update := bson.M{
				"$set": bson.M{
					"event_type": trade.EventType,
					"event_time": trade.EventTime,
					"trade_id":   trade.TradeID,
					"price":      trade.Price,
					"quantity":   trade.Quantity,
				},
			}
			opts := options.Update().SetUpsert(true)

			_, err = collection.UpdateOne(context.TODO(), filter, update, opts)
			if err != nil {
				fmt.Println("❌ Lỗi khi cập nhật dữ liệu:", err)
			} else {
				fmt.Println("✅ Đã cập nhật giao dịch cho", trade.Symbol)
			}

			// Giảm tải kết nối
			time.Sleep(300 * time.Millisecond)
		}
	}
}
