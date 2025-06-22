package main

import (
	"context"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoDBURI      = "mongodb://localhost:27017"
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
			log.Printf("✅ Đã cập nhật %d bản ghi stock\n", len(temp))
		}
	}
}

// ---------------------------
// Quản lý insert orders
// ---------------------------
type OrderBatchManager struct {
	mutex sync.Mutex
	data  []interface{}
	coll  *mongo.Collection
}

func (obm *OrderBatchManager) Add(data map[string]interface{}) {
	obm.mutex.Lock()
	defer obm.mutex.Unlock()

	obm.data = append(obm.data, data)
	if len(obm.data) >= 1 { // Chèn từng bản ghi luôn
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
		log.Println("❌ Lỗi insert order:", err)
	} else {
		log.Printf("📥 Đã insert %d bản ghi order\n", len(temp))
	}
}
