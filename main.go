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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoDBURI      = "mongodb+srv://hoangminhtri99:ster0.lu5ww.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
	DBName          = "moneyflow"
	Collection      = "stock_code"
	CollectionOrder = "orders"
	WebsocketURL    = "wss://openapi.tcbs.com.vn/ws/thesis/v1/stream/normal"
	BatchSize       = 2
)

var stockGroups = [][]string{
	{"ACB", "BCM", "BID", "CTG", "TCB", "VCB", "VHM", "VIB", "SSI", "STB"},
	{"FPT", "GAS", "GVR", "HDB", "HPG", "SAB", "SHB", "SSB", "TPB", "BVH"},
	{"LPB", "MBB", "MSN", "MWG", "PLX", "VIC", "VJC", "VNM", "VPB", "VRE"},
}

type BatchManager struct {
	mutex sync.Mutex
	data  []interface{}
	coll  *mongo.Collection
}

type OrderBatchManager struct {
	mutex sync.Mutex
	data  []interface{}
	coll  *mongo.Collection
}

func (bm *BatchManager) Add(data map[string]interface{}) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	bm.data = append(bm.data, data)
	if len(bm.data) >= BatchSize {
		bm.save()
	}
}

func (bm *BatchManager) save() {
	if len(bm.data) == 0 {
		return
	}
	temp := bm.data
	bm.data = nil

	var writes []mongo.WriteModel
	for _, d := range temp {
		doc, ok := d.(map[string]interface{})
		if !ok {
			continue
		}
		symbol, ok := doc["symbol"].(string)
		if !ok {
			continue
		}
		filter := bson.M{"symbol": symbol}
		update := bson.M{"$set": doc}
		writes = append(writes, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	if len(writes) > 0 {
		_, err := bm.coll.BulkWrite(context.TODO(), writes)
		if err != nil {
			log.Println("❌ Lỗi cập nhật batch:", err)
		} else {
			log.Printf("✅ Đã cập nhật %d bản ghi\n", len(temp))
		}
	}
}

func (obm *OrderBatchManager) Add(data map[string]interface{}) {
	obm.mutex.Lock()
	defer obm.mutex.Unlock()
	obm.data = append(obm.data, data)
	if len(obm.data) >= 1 {
		obm.save()
	}
}

func (obm *OrderBatchManager) save() {
	if len(obm.data) == 0 {
		return
	}
	temp := obm.data
	obm.data = nil
	_, err := obm.coll.InsertMany(context.TODO(), temp)
	if err != nil {
		log.Println("❌ Lỗi insert Order:", err)
	} else {
		log.Printf("📥 Đã insert %d bản ghi Order\n", len(temp))
	}
}

func startWebSocketGroup(group []string, token string, db *mongo.Database) {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(WebsocketURL, nil)
		if err != nil {
			log.Printf("❌ WS lỗi %v: %v", group, err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("✅ WS kết nối nhóm: %v\n", group)

		bm := &BatchManager{coll: db.Collection(Collection)}
		obm := &OrderBatchManager{coll: db.Collection(CollectionOrder)}

		base64Token := base64.StdEncoding.EncodeToString([]byte(token))
		authMsg := fmt.Sprintf("d|a|||%s", base64Token)
		conn.WriteMessage(websocket.TextMessage, []byte(authMsg))

		subMsg := fmt.Sprintf("d|s|tk|bp+bi+tm+op+fe|%s", strings.Join(group, ","))
		conn.WriteMessage(websocket.TextMessage, []byte(subMsg))

		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				conn.WriteMessage(websocket.TextMessage, []byte("d|p|||"))
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("🔥 Mất kết nối nhóm %v: %v", group, err)
				conn.Close()
				break
			}
			handleMessageForGroup(string(msg), bm, obm)
		}
	}
}

func handleMessageForGroup(message string, bm *BatchManager, obm *OrderBatchManager) {
	if strings.HasPrefix(message, "d|0|") {
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
		return
	}
	data["time"] = time.Now()
	bm.Add(data)

	if len(message) >= 3 {
		code := message[:3]
		if code == "s|6" {
			obm.Add(data)
		}
	}
}

func connectMongoDB() *mongo.Client {
	opt := options.Client().ApplyURI(MongoDBURI).SetServerSelectionTimeout(10 * time.Second).SetSocketTimeout(30 * time.Second)
	client, err := mongo.Connect(context.TODO(), opt)
	if err != nil {
		log.Fatal("❌ Mongo connect lỗi:", err)
	}
	if err := client.Ping(context.TODO(), nil); err != nil {
		log.Fatal("❌ Mongo ping lỗi:", err)
	}
	log.Println("✅ MongoDB connected")
	return client
}

func getUserInput(prompt string) string {
	fmt.Print(prompt)
	var input string
	fmt.Scanln(&input)
	return input
}

func main() {
	otp := getUserInput("📥 Nhập OTP: ")
	token, err := GetAccessToken("10000717062-85bbf26d-7365-414f-ba3f-956a122c726b", otp)
	if err != nil {
		log.Fatal("❌ Lỗi token:", err)
	}
	client := connectMongoDB()
	db := client.Database(DBName)
	for _, group := range stockGroups {
		go startWebSocketGroup(group, token, db)
	}
	select {} // Giữ chương trình chạy
} // bạn cần thêm hàm GetAccessToken từ file cũ hoặc import từ file riêng
