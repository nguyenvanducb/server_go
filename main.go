package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoDBURI   = "mongodb+srv://ducnguyen95hust:24Eh8M7ZfwWbecf7@cluster0.dvd5q.mongodb.net/"
	DBName       = "moneyflow"
	Collection   = "orders"
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
		panic("❌ Lỗi khi kết nối MongoDB: " + err.Error())
	}
	defer client.Disconnect(context.TODO())

	// Kiểm tra kết nối
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		panic("❌ Không thể ping đến MongoDB: " + err.Error())
	}
	fmt.Println("✅ Kết nối thành công đến MongoDB!")

	// Kết nối WebSocket tới Binance
	conn, _, err := websocket.DefaultDialer.Dial(WebsocketURL, nil)
	if err != nil {
		panic("❌ Không thể kết nối WebSocket: " + err.Error())
	}
	defer conn.Close()
	fmt.Println("✅ Kết nối WebSocket thành công!")

	collection := client.Database(DBName).Collection(Collection)

	// Đọc tin nhắn WebSocket liên tục
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("❌ Lỗi khi đọc dữ liệu từ WebSocket:", err)
			continue
		}

		// Giải mã JSON vào struct
		var tradeMessage TradeMessage
		err = json.Unmarshal(message, &tradeMessage)
		if err != nil {
			fmt.Println("❌ Lỗi giải mã JSON:", err)
			continue
		}

		// Lưu vào MongoDB
		tradeData := bson.D{
			{"event_type", tradeMessage.EventType},
			// {"event_time", tradeMessage.Timestamp},
			{"symbol", tradeMessage.Symbol},
			// {"trade_id", tradeMessage.Timestamp}, // Không chắc ID, tạm dùng timestamp
			{"price", tradeMessage.Price},
			{"quantity", tradeMessage.Quantity},
		}

		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		_, err = collection.InsertOne(context.TODO(), tradeData)
		if err != nil {
			fmt.Println("❌ Lỗi khi lưu giao dịch:", err)
		} else {
			fmt.Println("✅ Đã lưu giao dịch:", tradeData)
		}
	}
}
