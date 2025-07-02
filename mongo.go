package main

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoDBURI      = "mongodb://admin:abc123@localhost:27017/admin"
	DBName          = "moneyflow"
	Collection      = "stock_code"
	CollectionOrder = "orders"
	BatchSize       = 2
)

// Kết nối đến MongoDB
func connectMongoDB() *mongo.Client {
	opt := options.Client().
		ApplyURI(MongoDBURI).
		SetServerSelectionTimeout(10 * time.Second).
		SetSocketTimeout(30 * time.Second)

	client, err := mongo.Connect(context.TODO(), opt)
	if err != nil {
		log.Fatal("❌ Lỗi kết nối MongoDB:", err)
	}

	if err := client.Ping(context.TODO(), nil); err != nil {
		log.Fatal("❌ MongoDB không phản hồi:", err)
	}

	log.Println("✅ Đã kết nối MongoDB thành công")
	return client
}

// ---------------------------
// Quản lý lưu batch stock code
// ---------------------------
type BatchManager struct {
	mutex sync.Mutex
	data  []interface{}
	coll  *mongo.Collection
}

func NewBatchManager(coll *mongo.Collection) *BatchManager {
	return &BatchManager{
		data: make([]interface{}, 0),
		coll: coll,
	}
}

func (bm *BatchManager) Add(data map[string]interface{}) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	// ✅ Tạo bản copy để tránh reference đến mapStock gốc
	copyData := make(map[string]interface{})
	for k, v := range data {
		copyData[k] = v
	}

	bm.data = append(bm.data, copyData)
	if len(bm.data) >= BatchSize {
		bm.save()
	}
}

func (bm *BatchManager) save() {
	if len(bm.data) == 0 {
		return
	}
	temp := bm.data
	bm.data = make([]interface{}, 0) // ✅ Initialize properly

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
			log.Println("❌ Lỗi cập nhật batch stock:", err)
		} else {
			log.Printf("✅ Đã cập nhật %d bản ghi stock\n", len(temp))
		}
	}
}

// ---------------------------
// Quản lý insert orders
// ---------------------------
type OrderBatchManager struct {
	mutex         sync.Mutex
	data          []interface{}
	coll          *mongo.Collection
	bufferedBest  map[string]map[string]interface{}
	bufferFlushed bool // ✅ để tránh flush nhiều lần
}

// ✅ Constructor function to properly initialize the OrderBatchManager
func NewOrderBatchManager(coll *mongo.Collection) *OrderBatchManager {
	return &OrderBatchManager{
		data:          make([]interface{}, 0),
		coll:          coll,
		bufferedBest:  make(map[string]map[string]interface{}), // ✅ Initialize the map!
		bufferFlushed: false,
	}
}

func (obm *OrderBatchManager) Add(data map[string]interface{}) {
	obm.mutex.Lock()
	defer obm.mutex.Unlock()

	vnLoc, _ := time.LoadLocation("Asia/Ho_Chi_Minh")
	now := time.Now().In(vnLoc)

	cutoffStart := time.Date(now.Year(), now.Month(), now.Day(), 14, 30, 0, 0, vnLoc)
	cutoffEnd := time.Date(now.Year(), now.Month(), now.Day(), 14, 46, 0, 0, vnLoc)

	symbol, _ := data["symbol"].(string)
	matchQtty := parseInt(data["matchQtty"])

	// ⏳ Trong khoảng 14:30–14:46: lưu tạm vào bộ nhớ
	if now.After(cutoffStart) && now.Before(cutoffEnd) {
		old, exists := obm.bufferedBest[symbol]
		if !exists || matchQtty > parseInt(old["matchQtty"]) {
			// ✅ Create a copy of the data to avoid reference issues
			copyData := make(map[string]interface{})
			for k, v := range data {
				copyData[k] = v
			}
			obm.bufferedBest[symbol] = copyData
			log.Printf("🔄 [BUFFER] Cập nhật order tốt nhất cho %s (matchQtty: %d)", symbol, matchQtty)
		} else {
			log.Printf("➖ [BUFFER] Bỏ qua order thấp hơn cho %s", symbol)
		}
		return
	}

	// ✅ Sau 14:46: flush 1 lần nếu chưa flush
	if now.After(cutoffEnd) && !obm.bufferFlushed {
		obm.flushBuffered()
		obm.bufferFlushed = true
	}

	// ✅ Create a copy of the data before adding
	copyData := make(map[string]interface{})
	for k, v := range data {
		copyData[k] = v
	}

	// ✅ Thêm bản mới sau khi đã flush
	obm.data = append(obm.data, copyData)
	if len(obm.data) >= 1 {
		obm.save()
	}
}

func (obm *OrderBatchManager) flushBuffered() {
	if len(obm.bufferedBest) == 0 {
		log.Println("ℹ️ Không có gì để flush từ buffer.")
		return
	}

	log.Printf("🚀 Flush %d bản ghi từ buffer vào MongoDB", len(obm.bufferedBest))
	var temp []interface{}
	for _, doc := range obm.bufferedBest {
		temp = append(temp, doc)
	}
	obm.bufferedBest = make(map[string]map[string]interface{}) // reset

	if len(temp) > 0 {
		_, err := obm.coll.InsertMany(context.TODO(), temp)
		if err != nil {
			log.Println("❌ Lỗi khi flush buffer:", err)
		} else {
			log.Printf("✅ Đã insert %d bản ghi từ buffer", len(temp))
		}
	}
}

func (obm *OrderBatchManager) save() {
	if len(obm.data) == 0 {
		return
	}
	temp := obm.data
	obm.data = make([]interface{}, 0) // ✅ Initialize properly

	_, err := obm.coll.InsertMany(context.TODO(), temp)
	if err != nil {
		log.Println("❌ Lỗi insert order:", err)
	} else {
		log.Printf("📥 Đã insert %d bản ghi order\n", len(temp))
	}
}

// ✅ Add method to safely flush remaining data when shutting down
func (obm *OrderBatchManager) Flush() {
	obm.mutex.Lock()
	defer obm.mutex.Unlock()

	// Flush buffered data first
	if !obm.bufferFlushed {
		obm.flushBuffered()
	}
	// Then flush remaining data
	obm.save()
}

func parseInt(val interface{}) int64 {
	switch v := val.(type) {
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return i
		}
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	}
	return 0
}
