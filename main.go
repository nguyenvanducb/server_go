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

// Map lưu trữ dữ liệu stock theo symbol, thread-safe
type StockMap struct {
	mutex sync.RWMutex
	data  map[string]map[string]interface{}
}

func NewStockMap() *StockMap {
	return &StockMap{
		data: make(map[string]map[string]interface{}),
	}
}

func (sm *StockMap) Update(symbol string, newData map[string]interface{}) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if _, exists := sm.data[symbol]; !exists {
		sm.data[symbol] = make(map[string]interface{})
	}

	// Cập nhật dữ liệu
	for k, v := range newData {
		sm.data[symbol][k] = v
	}
}

func (sm *StockMap) Get(symbol string) (map[string]interface{}, bool) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	data, exists := sm.data[symbol]
	if !exists {
		return nil, false
	}

	// Tạo bản copy để tránh race condition
	result := make(map[string]interface{})
	for k, v := range data {
		result[k] = v
	}
	return result, true
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
	stockMap := NewStockMap()

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

		// Ping goroutine
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				if err := conn.WriteMessage(websocket.TextMessage, []byte("d|p|||")); err != nil {
					log.Printf("❌ Ping lỗi nhóm %v: %v", group, err)
					ticker.Stop()
					return
				}
			}
		}()

		// Đọc messages
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("🔥 Mất kết nối nhóm %v: %v", group, err)
				conn.Close()
				break
			}
			handleMessageForGroup(string(msg), bm, obm, stockMap)
		}
	}
}

func handleMessageForGroup(message string, bm *BatchManager, obm *OrderBatchManager, stockMap *StockMap) {
	// Bỏ qua authentication response
	if strings.HasPrefix(message, "d|0|") {
		return
	}

	// Lấy code từ đầu message (3 ký tự đầu)
	if len(message) < 20 {
		return
	}

	code := message[:3]

	// Tìm JSON trong message
	start := strings.Index(message, "{")
	end := strings.LastIndex(message, "}")
	if start == -1 || end == -1 || start >= end {
		return
	}

	jsonStr := message[start : end+1]
	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &jsonData); err != nil {
		return
	}

	// Thêm timestamp
	jsonData["time"] = time.Now()

	// Lấy symbol
	symbolRaw, exists := jsonData["symbol"]
	if !exists {
		return
	}

	symbol, ok := symbolRaw.(string)
	if !ok {
		return
	}

	// Cập nhật stockMap
	stockMap.Update(symbol, jsonData)

	// Thêm vào batch chung
	bm.Add(jsonData)

	// Kiểm tra code để quyết định có đẩy vào CollectionOrder không
	if code == "s|2" || code == "s|6" {
		// Lấy dữ liệu đầy đủ từ stockMap
		if fullData, exists := stockMap.Get(symbol); exists {
			obm.Add(fullData)
			log.Printf("📥 Đưa vào batch Order (code: %s): %s\n", code, symbol)
		}
	}
}

func connectMongoDB() *mongo.Client {
	opt := options.Client().
		ApplyURI(MongoDBURI).
		SetServerSelectionTimeout(10 * time.Second).
		SetSocketTimeout(30 * time.Second).
		SetMaxPoolSize(100).
		SetMinPoolSize(5).
		SetHeartbeatInterval(10 * time.Second)

	var client *mongo.Client
	var err error

	// Retry connection up to 3 times
	for i := 0; i < 3; i++ {
		client, err = mongo.Connect(context.TODO(), opt)
		if err == nil {
			break
		}
		log.Printf("❌ Mongo connect lỗi (lần %d): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		log.Fatal("❌ Không thể kết nối MongoDB sau 3 lần thử:", err)
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
	fmt.Printf("✅ Bạn đã nhập: %s\n", otp)

	token, err := GetAccessToken("10000717062-85bbf26d-7365-414f-ba3f-956a122c726b", otp)
	if err != nil {
		log.Fatal("❌ Lỗi token:", err)
	}

	client := connectMongoDB()
	defer client.Disconnect(context.TODO())

	db := client.Database(DBName)

	// Start WebSocket connections for each group
	for i, group := range stockGroups {
		go startWebSocketGroup(group, token, db)
		log.Printf("🚀 Khởi động WebSocket nhóm %d: %v", i+1, group)
	}

	log.Println("🎯 Tất cả WebSocket đã khởi động. Nhấn Ctrl+C để thoát...")
	select {} // Giữ chương trình chạy
}

// Bạn cần thêm hàm GetAccessToken từ file cũ hoặc import từ file riêng
