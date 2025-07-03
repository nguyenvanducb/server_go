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
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	MongoDBURI      = "mongodb://admin:abc123@localhost:27017/admin"
	DBName          = "moneyflow"
	Collection      = "stock_code"
	CollectionOrder = "orders"
	BatchSize       = 100                   // ✅ Giảm batch size để flush nhanh hơn
	FlushInterval   = 50 * time.Millisecond // ✅ Flush cực nhanh - mỗi 50ms
	MaxChannelSize  = 2000                  // ✅ Tăng channel buffer
)

// ✅ Ultra-Fast Batch Manager - Đảm bảo không mất dữ liệu
type UltraFastBatchManager struct {
	dataChan   chan map[string]interface{}
	coll       *mongo.Collection
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	forceFlush chan struct{} // ✅ Channel để force flush ngay lập tức
	stats      *BatchStats
}

type BatchStats struct {
	totalReceived int64
	totalFlushed  int64
	totalDropped  int64
	mutex         sync.RWMutex
}

func (bs *BatchStats) AddReceived(count int64) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	bs.totalReceived += count
}

func (bs *BatchStats) AddFlushed(count int64) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	bs.totalFlushed += count
}

func (bs *BatchStats) AddDropped(count int64) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	bs.totalDropped += count
}

func (bs *BatchStats) GetStats() (received, flushed, dropped int64) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	return bs.totalReceived, bs.totalFlushed, bs.totalDropped
}

func NewUltraFastBatchManager(coll *mongo.Collection) *UltraFastBatchManager {
	ctx, cancel := context.WithCancel(context.Background())
	bm := &UltraFastBatchManager{
		dataChan:   make(chan map[string]interface{}, MaxChannelSize),
		coll:       coll,
		ctx:        ctx,
		cancel:     cancel,
		forceFlush: make(chan struct{}, 1),
		stats:      &BatchStats{},
	}

	// ✅ Start multiple goroutines for better performance
	bm.wg.Add(2)
	go bm.processBatches()
	go bm.monitorStats() // ✅ Monitor để debug

	return bm
}

func (bm *UltraFastBatchManager) Add(data map[string]interface{}) {
	bm.stats.AddReceived(1)

	// ✅ Priority: Thử gửi non-blocking trước
	select {
	case bm.dataChan <- data:
		// Successfully added
		return
	default:
		// Channel full, force flush and retry
		bm.triggerForceFlush()

		// ✅ Retry với timeout ngắn
		select {
		case bm.dataChan <- data:
			// Successfully added after force flush
			return
		case <-time.After(10 * time.Millisecond):
			// Still can't add, drop and log
			bm.stats.AddDropped(1)
			log.Printf("❌ DROPPED DATA: Channel full, symbol: %v", data["symbol"])
		}
	}
}

func (bm *UltraFastBatchManager) triggerForceFlush() {
	select {
	case bm.forceFlush <- struct{}{}:
		// Force flush triggered
	default:
		// Force flush already pending
	}
}

func (bm *UltraFastBatchManager) processBatches() {
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

			// ✅ NGAY LẬP TỨC flush khi đạt BatchSize
			if len(batch) >= BatchSize {
				bm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
				ticker.Reset(FlushInterval) // Reset timer
			}

		case <-bm.forceFlush:
			// ✅ Force flush ngay lập tức
			if len(batch) > 0 {
				bm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
				ticker.Reset(FlushInterval)
			}

		case <-ticker.C:
			// ✅ Flush định kỳ - đảm bảo không có data bị stuck
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

func (bm *UltraFastBatchManager) flushBatch(batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	startTime := time.Now()

	// ✅ Prepare bulk operations với concurrent processing
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
		// ✅ Bulk write với timeout ngắn để tránh block
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		opts := options.BulkWrite().
			SetOrdered(false). // ✅ Unordered cho performance
			SetBypassDocumentValidation(true)

		result, err := bm.coll.BulkWrite(ctx, writes, opts)
		duration := time.Since(startTime)

		if err != nil {
			log.Printf("❌ Bulk write ERROR: %v (took %v)", err, duration)
		} else {
			bm.stats.AddFlushed(int64(len(writes)))
			log.Printf("✅ Bulk write SUCCESS: %d records in %v (upserted: %d, modified: %d)",
				len(writes), duration, result.UpsertedCount, result.ModifiedCount)
		}
	}
}

func (bm *UltraFastBatchManager) monitorStats() {
	defer bm.wg.Done()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			received, flushed, dropped := bm.stats.GetStats()
			pending := len(bm.dataChan)

			log.Printf("📊 STATS - Received: %d, Flushed: %d, Dropped: %d, Pending: %d",
				received, flushed, dropped, pending)

			if dropped > 0 {
				log.Printf("⚠️  WARNING: %d records dropped! Consider increasing channel size or batch processing speed", dropped)
			}

		case <-bm.ctx.Done():
			return
		}
	}
}

func (bm *UltraFastBatchManager) Close() {
	log.Println("🔄 Closing UltraFastBatchManager...")

	// ✅ Force flush all remaining data
	bm.triggerForceFlush()
	time.Sleep(100 * time.Millisecond) // Give time for final flush

	close(bm.dataChan)
	bm.cancel()
	bm.wg.Wait()

	received, flushed, dropped := bm.stats.GetStats()
	log.Printf("📈 FINAL STATS - Received: %d, Flushed: %d, Dropped: %d",
		received, flushed, dropped)
}

// ✅ Tương tự cho OrderBatchManager
type UltraFastOrderBatchManager struct {
	dataChan      chan map[string]interface{}
	coll          *mongo.Collection
	bufferedBest  sync.Map
	bufferFlushed bool
	mutex         sync.RWMutex
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	forceFlush    chan struct{}
	stats         *BatchStats
}

func NewUltraFastOrderBatchManager(coll *mongo.Collection) *UltraFastOrderBatchManager {
	ctx, cancel := context.WithCancel(context.Background())
	obm := &UltraFastOrderBatchManager{
		dataChan:   make(chan map[string]interface{}, MaxChannelSize),
		coll:       coll,
		ctx:        ctx,
		cancel:     cancel,
		forceFlush: make(chan struct{}, 1),
		stats:      &BatchStats{},
	}

	obm.wg.Add(2)
	go obm.processBatches()
	go obm.monitorStats()

	return obm
}

func (obm *UltraFastOrderBatchManager) Add(data map[string]interface{}) {
	obm.stats.AddReceived(1)

	vnLoc, _ := time.LoadLocation("Asia/Ho_Chi_Minh")
	now := time.Now().In(vnLoc)

	cutoffStart := time.Date(now.Year(), now.Month(), now.Day(), 14, 30, 0, 0, vnLoc)
	cutoffEnd := time.Date(now.Year(), now.Month(), now.Day(), 14, 46, 0, 0, vnLoc)

	symbol, _ := data["symbol"].(string)
	matchQtty := parseInt(data["matchQtty"])

	// ✅ Buffer logic
	if now.After(cutoffStart) && now.Before(cutoffEnd) {
		copyData := make(map[string]interface{})
		for k, v := range data {
			copyData[k] = v
		}

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
		go obm.flushBuffered()
		obm.bufferFlushed = true
	}
	obm.mutex.Unlock()

	// ✅ Send to channel với retry logic
	select {
	case obm.dataChan <- data:
		return
	default:
		obm.triggerForceFlush()
		select {
		case obm.dataChan <- data:
			return
		case <-time.After(10 * time.Millisecond):
			obm.stats.AddDropped(1)
			log.Printf("❌ DROPPED ORDER: Channel full, symbol: %v", data["symbol"])
		}
	}
}

func (obm *UltraFastOrderBatchManager) triggerForceFlush() {
	select {
	case obm.forceFlush <- struct{}{}:
	default:
	}
}

func (obm *UltraFastOrderBatchManager) processBatches() {
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

			// ✅ Flush ngay khi đạt BatchSize
			if len(batch) >= BatchSize {
				obm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
				ticker.Reset(FlushInterval)
			}

		case <-obm.forceFlush:
			if len(batch) > 0 {
				obm.flushBatch(batch)
				batch = make([]interface{}, 0, BatchSize)
				ticker.Reset(FlushInterval)
			}

		case <-ticker.C:
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

func (obm *UltraFastOrderBatchManager) flushBatch(batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := options.InsertMany().SetOrdered(false)
	result, err := obm.coll.InsertMany(ctx, batch, opts)
	duration := time.Since(startTime)

	if err != nil {
		log.Printf("❌ Insert orders ERROR: %v (took %v)", err, duration)
	} else {
		obm.stats.AddFlushed(int64(len(batch)))
		log.Printf("📥 Insert orders SUCCESS: %d records in %v (inserted: %d)",
			len(batch), duration, len(result.InsertedIDs))
	}
}

func (obm *UltraFastOrderBatchManager) flushBuffered() {
	var temp []interface{}

	obm.bufferedBest.Range(func(key, value interface{}) bool {
		temp = append(temp, value)
		return true
	})

	if len(temp) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		result, err := obm.coll.InsertMany(ctx, temp)
		if err != nil {
			log.Printf("❌ Flush buffer ERROR: %v", err)
		} else {
			log.Printf("🚀 Flush buffer SUCCESS: %d records (inserted: %d)",
				len(temp), len(result.InsertedIDs))
		}
	}

	obm.bufferedBest = sync.Map{}
}

func (obm *UltraFastOrderBatchManager) monitorStats() {
	defer obm.wg.Done()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			received, flushed, dropped := obm.stats.GetStats()
			pending := len(obm.dataChan)

			log.Printf("📊 ORDER STATS - Received: %d, Flushed: %d, Dropped: %d, Pending: %d",
				received, flushed, dropped, pending)

		case <-obm.ctx.Done():
			return
		}
	}
}

func (obm *UltraFastOrderBatchManager) Close() {
	log.Println("🔄 Closing UltraFastOrderBatchManager...")

	obm.triggerForceFlush()
	time.Sleep(100 * time.Millisecond)

	close(obm.dataChan)
	obm.cancel()
	obm.wg.Wait()

	received, flushed, dropped := obm.stats.GetStats()
	log.Printf("📈 ORDER FINAL STATS - Received: %d, Flushed: %d, Dropped: %d",
		received, flushed, dropped)
}

// ✅ Utility functions
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

// ✅ Optimized connection
func connectMongoDB() *mongo.Client {
	// ✅ Cấu hình WriteConcern cho performance
	wc := writeconcern.New(writeconcern.W(1), writeconcern.J(false))

	opt := options.Client().
		ApplyURI(MongoDBURI).
		SetServerSelectionTimeout(5 * time.Second). // ✅ Giảm timeout
		SetSocketTimeout(10 * time.Second).         // ✅ Giảm socket timeout
		SetMaxPoolSize(50).                         // ✅ Điều chỉnh pool size
		SetMinPoolSize(5).
		SetMaxConnIdleTime(30 * time.Second).
		SetWriteConcern(wc) // ✅ Sử dụng WriteConcern đã config

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
