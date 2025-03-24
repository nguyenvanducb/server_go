package main

import (
	"context"
	"encoding/json"
	"fmt"
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
			fmt.Println("🔌 Mất kết nối: %w", err)
		}

		var trade TradeMessage
		if err := json.Unmarshal(message, &trade); err != nil {
			fmt.Println("❌ Lỗi giải mã JSON:", err)
			continue
		}

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

		// Giảm tốc độ nhận dữ liệu
		time.Sleep(300 * time.Millisecond)
	}

}
