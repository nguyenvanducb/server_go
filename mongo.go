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
	BatchSize       = 500                    // ✅ Max batch size - chỉ để tránh memory overflow
	FlushInterval   = 200 * time.Millisecond // ✅ Balanced: responsive + efficient I/O
)

// ✅ Optimized connection với connection pooling
func connectMongoDB() *mongo.Client {
	opt := options.Client().
		ApplyURI(MongoDBURI).
		SetServerSelectionTimeout(10 * time.Second).
		SetSocketTimeout(30 * time.Second).
		SetMaxPoolSize(100). // ✅ Tăng connection pool
		SetMinPoolSize(10).  // ✅ Maintain minimum connections
		SetMaxConnIdleTime(30 * time.Second)

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

// ✅ Optimized BatchManager với channel-based processing
type OptimizedBatchManager struct {
	dataChan chan map[string]interface{}
	coll     *mongo.Collection
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewOptimizedBatchManager(coll *mongo.Collection) *OptimizedBatchManager {
	ctx, cancel := context.WithCancel(context.Background())
	bm := &OptimizedBatchManager{
		dataChan: make(chan map[string]interface{}, 1000), // ✅ Buffered channel
		coll:     coll,
		ctx:      ctx,
		cancel:   cancel,
	}

	// ✅ Start background goroutine for batch processing
	bm.wg.Add(1)
	go bm.processBatches()

	return bm
}

func (bm *OptimizedBatchManager) Add(data map[string]interface{}) {
	// ✅ Non-blocking send với select
	select {
	case bm.dataChan <- data:
		// Successfully added to channel
	default:
		// Channel full, log warning but don't block
		log.Println("⚠️ Batch channel full, dropping data")
	}
}

func (bm *OptimizedBatchManager) processBatches() {
	defer bm.wg.Done()

	batch := make([]interface{}, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case data := <-bm.dataChan:
			// ✅ Copy data để tránh reference issues
			copyData := make(map[string]interface{})
			for k, v := range data {
				copyData[k] = v
			}
			batch = append(batch, copyData)

			// ✅ Flush ngay khi có data đầu tiên và đã qua FlushInterval
			// Hoặc khi đạt BatchSize (để tránh memory overflow)
			if len(batch) >= BatchSize {
				bm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
				ticker.Reset(FlushInterval) // ✅ Reset timer
			}

		case <-ticker.C:
			// ✅ Flush định kỳ - đây là trigger chính
			if len(batch) > 0 {
				bm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
			}

		case <-bm.ctx.Done():
			// ✅ Flush remaining data before shutdown
			if len(batch) > 0 {
				bm.flushBatch(batch)
			}
			return
		}
	}
}

func (bm *OptimizedBatchManager) flushBatch(batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	// ✅ Prepare bulk operations
	writes := make([]mongo.WriteModel, 0, len(batch))
	for _, d := range batch {
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
		writes = append(writes, mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true))
	}

	if len(writes) > 0 {
		// ✅ Bulk write với options tối ưu
		opts := options.BulkWrite().
			SetOrdered(false). // ✅ Unordered for better performance
			SetBypassDocumentValidation(true)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := bm.coll.BulkWrite(ctx, writes, opts)
		if err != nil {
			log.Printf("❌ Lỗi bulk write stock: %v", err)
		} else {
			log.Printf("✅ Bulk write thành công %d records", len(writes))
		}
	}
}

func (bm *OptimizedBatchManager) Close() {
	close(bm.dataChan)
	bm.cancel()
	bm.wg.Wait()
}

// ✅ Optimized OrderBatchManager
type OptimizedOrderBatchManager struct {
	dataChan      chan map[string]interface{}
	coll          *mongo.Collection
	bufferedBest  sync.Map // ✅ Concurrent map thay vì mutex
	bufferFlushed bool
	mutex         sync.RWMutex
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewOptimizedOrderBatchManager(coll *mongo.Collection) *OptimizedOrderBatchManager {
	ctx, cancel := context.WithCancel(context.Background())
	obm := &OptimizedOrderBatchManager{
		dataChan: make(chan map[string]interface{}, 1000),
		coll:     coll,
		ctx:      ctx,
		cancel:   cancel,
	}

	obm.wg.Add(1)
	go obm.processBatches()

	return obm
}

func (obm *OptimizedOrderBatchManager) Add(data map[string]interface{}) {
	vnLoc, _ := time.LoadLocation("Asia/Ho_Chi_Minh")
	now := time.Now().In(vnLoc)

	cutoffStart := time.Date(now.Year(), now.Month(), now.Day(), 14, 30, 0, 0, vnLoc)
	cutoffEnd := time.Date(now.Year(), now.Month(), now.Day(), 14, 46, 0, 0, vnLoc)

	symbol, _ := data["symbol"].(string)
	matchQtty := parseInt(data["matchQtty"])

	// ✅ Buffer logic với concurrent map
	if now.After(cutoffStart) && now.Before(cutoffEnd) {
		copyData := make(map[string]interface{})
		for k, v := range data {
			copyData[k] = v
		}

		// ✅ Thread-safe update using sync.Map
		if oldVal, exists := obm.bufferedBest.Load(symbol); exists {
			if old, ok := oldVal.(map[string]interface{}); ok {
				if matchQtty > parseInt(old["matchQtty"]) {
					obm.bufferedBest.Store(symbol, copyData)
				}
			}
		} else {
			obm.bufferedBest.Store(symbol, copyData)
		}
		return
	}

	// ✅ Flush buffer sau 14:46
	obm.mutex.Lock()
	if now.After(cutoffEnd) && !obm.bufferFlushed {
		go obm.flushBuffered() // ✅ Async flush
		obm.bufferFlushed = true
	}
	obm.mutex.Unlock()

	// ✅ Send to channel
	select {
	case obm.dataChan <- data:
	default:
		log.Println("⚠️ Order channel full, dropping data")
	}
}

func (obm *OptimizedOrderBatchManager) processBatches() {
	defer obm.wg.Done()

	batch := make([]interface{}, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case data := <-obm.dataChan:
			copyData := make(map[string]interface{})
			for k, v := range data {
				copyData[k] = v
			}
			batch = append(batch, copyData)

			// ✅ Flush ngay khi đạt BatchSize (safety limit)
			if len(batch) >= BatchSize {
				obm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
				ticker.Reset(FlushInterval) // ✅ Reset timer
			}

		case <-ticker.C:
			// ✅ Flush định kỳ - trigger chính cho low-latency
			if len(batch) > 0 {
				obm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
			}

		case <-obm.ctx.Done():
			if len(batch) > 0 {
				obm.flushBatch(batch)
			}
			return
		}
	}
}

func (obm *OptimizedOrderBatchManager) flushBatch(batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.InsertMany().SetOrdered(false) // ✅ Unordered insert
	_, err := obm.coll.InsertMany(ctx, batch, opts)
	if err != nil {
		log.Printf("❌ Lỗi insert orders: %v", err)
	} else {
		log.Printf("📥 Insert thành công %d orders", len(batch))
	}
}

func (obm *OptimizedOrderBatchManager) flushBuffered() {
	var temp []interface{}

	// ✅ Collect all buffered data
	obm.bufferedBest.Range(func(key, value interface{}) bool {
		temp = append(temp, value)
		return true
	})

	if len(temp) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := obm.coll.InsertMany(ctx, temp)
		if err != nil {
			log.Printf("❌ Lỗi flush buffer: %v", err)
		} else {
			log.Printf("🚀 Flush thành công %d records từ buffer", len(temp))
		}
	}

	// ✅ Clear buffer
	obm.bufferedBest = sync.Map{}
}

func (obm *OptimizedOrderBatchManager) Close() {
	close(obm.dataChan)
	obm.cancel()
	obm.wg.Wait()
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
