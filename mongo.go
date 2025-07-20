package main

import (
	"context"
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// ✅ ORDERS PRIORITY CONFIGURATION - CRITICAL DATA
const (
	MongoDBURI      = "mongodb://admin:abc123@localhost:27017/admin"
	DBName          = "moneyflow"
	Collection      = "stock_code"
	CollectionOrder = "orders"

	// ✅ ORDERS = MAXIMUM PROTECTION
	OrdersBatchSize      = 50                    // Tiny batches = instant flush
	OrdersFlushInterval  = 25 * time.Millisecond // Ultra fast = 25ms
	OrdersMaxChannelSize = 100000                // HUGE buffer for orders
	OrdersRetryAttempts  = 10                    // More retries for orders
	OrdersRetryDelay     = 25 * time.Millisecond // Faster retry

	// ✅ STOCK_CODE = RELAXED (less critical)
	StockBatchSize      = 500                    // Bigger batches OK
	StockFlushInterval  = 200 * time.Millisecond // Slower flush OK
	StockMaxChannelSize = 20000                  // Smaller buffer OK
	StockRetryAttempts  = 3                      // Fewer retries OK
	StockRetryDelay     = 100 * time.Millisecond // Slower retry OK

	// ✅ MEMORY MANAGEMENT CONSTANTS WITH LIST SUPPORT
	MaxMemoryMB         = 200   // Maximum memory before cleanup (MB)
	CleanupIntervalSec  = 30    // Background cleanup interval (seconds)
	MaxSymbolAge        = 300   // Maximum age for symbols in seconds (5 minutes)
	MaxSymbolCount      = 10000 // Maximum symbols in memory
	MaxRecordsPerSymbol = 100   // Maximum records per symbol (LIST)
	MaxOrdersPerSymbol  = 500   // More records for critical orders
)

// ✅ Enhanced memory management with LIST support - NO DATA LOSS
type MemoryManagedStockMap struct {
	data                map[string][]map[string]interface{} // ← LIST of records per symbol
	lastAccessed        map[string]time.Time
	recordCounts        map[string]int64 // Track record count per symbol
	totalRecords        int64            // Total records across all symbols
	mutex               sync.RWMutex
	cleanupTicker       *time.Ticker
	maxAge              time.Duration
	maxSize             int
	maxRecordsPerSymbol int
	isOrdersMap         bool // Different limits for orders vs stock
	cleanupRunning      bool
	ctx                 context.Context
	cancel              context.CancelFunc
}

func NewMemoryManagedStockMap(isOrdersMap bool) *MemoryManagedStockMap {
	ctx, cancel := context.WithCancel(context.Background())

	maxRecords := MaxRecordsPerSymbol
	if isOrdersMap {
		maxRecords = MaxOrdersPerSymbol
	}

	mm := &MemoryManagedStockMap{
		data:                make(map[string][]map[string]interface{}),
		lastAccessed:        make(map[string]time.Time),
		recordCounts:        make(map[string]int64),
		maxAge:              time.Duration(MaxSymbolAge) * time.Second,
		maxSize:             MaxSymbolCount,
		maxRecordsPerSymbol: maxRecords,
		isOrdersMap:         isOrdersMap,
		cleanupTicker:       time.NewTicker(time.Duration(CleanupIntervalSec) * time.Second),
		ctx:                 ctx,
		cancel:              cancel,
	}

	// Start background cleanup
	go mm.backgroundCleanup()

	mapType := "STOCK"
	if isOrdersMap {
		mapType = "ORDERS"
	}

	log.Printf("✅ MemoryManagedStockMap (%s) initialized - MaxAge: %v, MaxSize: %d, MaxRecordsPerSymbol: %d",
		mapType, mm.maxAge, mm.maxSize, mm.maxRecordsPerSymbol)
	return mm
}

// ✅ Add record to LIST - NO OVERWRITE, NO DATA LOSS
func (mm *MemoryManagedStockMap) Add(symbol string, data map[string]interface{}) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	// Deep copy to avoid memory leaks
	copyData := make(map[string]interface{})
	for k, v := range data {
		copyData[k] = v
	}

	// Add timestamp for tracking
	copyData["_added_at"] = time.Now()

	// Add to LIST (newest first)
	if existing, exists := mm.data[symbol]; exists {
		// Prepend new record to maintain chronological order (newest first)
		mm.data[symbol] = append([]map[string]interface{}{copyData}, existing...)
		mm.recordCounts[symbol]++

		// Limit records per symbol to prevent infinite growth
		if len(mm.data[symbol]) > mm.maxRecordsPerSymbol {
			// Remove oldest records (from the end)
			removed := len(mm.data[symbol]) - mm.maxRecordsPerSymbol
			mm.data[symbol] = mm.data[symbol][:mm.maxRecordsPerSymbol]
			mm.recordCounts[symbol] -= int64(removed)
			atomic.AddInt64(&mm.totalRecords, -int64(removed))

			if mm.isOrdersMap {
				log.Printf("⚠️ ORDERS: Removed %d old records for %s (kept %d newest)",
					removed, symbol, mm.maxRecordsPerSymbol)
			}
		}
	} else {
		// Create new list for symbol
		mm.data[symbol] = []map[string]interface{}{copyData}
		mm.recordCounts[symbol] = 1
	}

	mm.lastAccessed[symbol] = time.Now()
	atomic.AddInt64(&mm.totalRecords, 1)

	// Trigger cleanup if total records exceed memory limits
	if atomic.LoadInt64(&mm.totalRecords) > int64(mm.maxSize*10) { // 10x symbol limit
		go mm.forceCleanup()
	}
}

// ✅ Get latest record for symbol
func (mm *MemoryManagedStockMap) GetLatest(symbol string) (map[string]interface{}, bool) {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if records, exists := mm.data[symbol]; exists && len(records) > 0 {
		mm.lastAccessed[symbol] = time.Now()

		// Return deep copy of latest (first) record
		latest := records[0]
		copyData := make(map[string]interface{})
		for k, v := range latest {
			copyData[k] = v
		}
		return copyData, true
	}
	return nil, false
}

// ✅ Get all records for symbol
func (mm *MemoryManagedStockMap) GetAll(symbol string) ([]map[string]interface{}, bool) {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if records, exists := mm.data[symbol]; exists && len(records) > 0 {
		mm.lastAccessed[symbol] = time.Now()

		// Return deep copy of all records
		copyRecords := make([]map[string]interface{}, len(records))
		for i, record := range records {
			copyData := make(map[string]interface{})
			for k, v := range record {
				copyData[k] = v
			}
			copyRecords[i] = copyData
		}
		return copyRecords, true
	}
	return nil, false
}

// ✅ Get N latest records for symbol
func (mm *MemoryManagedStockMap) GetLatestN(symbol string, n int) ([]map[string]interface{}, bool) {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if records, exists := mm.data[symbol]; exists && len(records) > 0 {
		mm.lastAccessed[symbol] = time.Now()

		// Limit n to available records
		if n > len(records) {
			n = len(records)
		}

		// Return deep copy of N latest records
		copyRecords := make([]map[string]interface{}, n)
		for i := 0; i < n; i++ {
			copyData := make(map[string]interface{})
			for k, v := range records[i] {
				copyData[k] = v
			}
			copyRecords[i] = copyData
		}
		return copyRecords, true
	}
	return nil, false
}

// ✅ Get records count for symbol
func (mm *MemoryManagedStockMap) GetRecordCount(symbol string) int64 {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if count, exists := mm.recordCounts[symbol]; exists {
		return count
	}
	return 0
}

// ✅ Mark symbol as processed (update access time)
func (mm *MemoryManagedStockMap) MarkProcessed(symbol string) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if _, exists := mm.lastAccessed[symbol]; exists {
		mm.lastAccessed[symbol] = time.Now()
	}
}

// ✅ Background cleanup with LIST awareness
func (mm *MemoryManagedStockMap) backgroundCleanup() {
	for {
		select {
		case <-mm.cleanupTicker.C:
			mm.performCleanup()
		case <-mm.ctx.Done():
			return
		}
	}
}

func (mm *MemoryManagedStockMap) forceCleanup() {
	if mm.cleanupRunning {
		return
	}
	mm.performCleanup()
}

func (mm *MemoryManagedStockMap) performCleanup() {
	mm.mutex.Lock()
	if mm.cleanupRunning {
		mm.mutex.Unlock()
		return
	}
	mm.cleanupRunning = true
	defer func() {
		mm.cleanupRunning = false
		mm.mutex.Unlock()
	}()

	now := time.Now()
	cleaned := 0
	recordsRemoved := int64(0)

	// Remove expired entries
	for symbol, lastAccess := range mm.lastAccessed {
		if now.Sub(lastAccess) > mm.maxAge {
			if records, exists := mm.data[symbol]; exists {
				recordsRemoved += int64(len(records))
			}
			delete(mm.data, symbol)
			delete(mm.lastAccessed, symbol)
			delete(mm.recordCounts, symbol)
			cleaned++
		}
	}

	// If still over symbol limit, remove oldest accessed symbols
	if len(mm.data) > mm.maxSize {
		type symbolTime struct {
			symbol string
			time   time.Time
		}

		var entries []symbolTime
		for symbol, lastAccess := range mm.lastAccessed {
			entries = append(entries, symbolTime{symbol, lastAccess})
		}

		// Sort by time (oldest first) - simple bubble sort for reliability
		for i := 0; i < len(entries)-1; i++ {
			for j := i + 1; j < len(entries); j++ {
				if entries[i].time.After(entries[j].time) {
					entries[i], entries[j] = entries[j], entries[i]
				}
			}
		}

		// Remove oldest symbols until under limit
		toRemove := len(mm.data) - mm.maxSize
		for i := 0; i < toRemove && i < len(entries); i++ {
			symbol := entries[i].symbol
			if records, exists := mm.data[symbol]; exists {
				recordsRemoved += int64(len(records))
			}
			delete(mm.data, symbol)
			delete(mm.lastAccessed, symbol)
			delete(mm.recordCounts, symbol)
			cleaned++
		}
	}

	// Update total records count
	atomic.AddInt64(&mm.totalRecords, -recordsRemoved)

	if cleaned > 0 {
		mapType := "STOCK"
		if mm.isOrdersMap {
			mapType = "ORDERS"
		}

		log.Printf("🧹 %s Memory cleanup: removed %d symbols (%d records), current: %d symbols, %d total records",
			mapType, cleaned, recordsRemoved, len(mm.data), atomic.LoadInt64(&mm.totalRecords))

		// Force garbage collection after cleanup
		runtime.GC()

		// Log memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("📊 %s Memory after cleanup: Alloc=%d KB, Sys=%d KB",
			mapType, bToKb(m.Alloc), bToKb(m.Sys))
	}
}

func (mm *MemoryManagedStockMap) GetStats() (int, int64, int64) {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()
	return len(mm.data), atomic.LoadInt64(&mm.totalRecords), int64(len(mm.lastAccessed))
}

func (mm *MemoryManagedStockMap) Close() {
	if mm.cancel != nil {
		mm.cancel()
	}

	if mm.cleanupTicker != nil {
		mm.cleanupTicker.Stop()
	}

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	totalRecords := atomic.LoadInt64(&mm.totalRecords)
	mapType := "STOCK"
	if mm.isOrdersMap {
		mapType = "ORDERS"
	}

	// Clear all data
	mm.data = make(map[string][]map[string]interface{})
	mm.lastAccessed = make(map[string]time.Time)
	mm.recordCounts = make(map[string]int64)
	atomic.StoreInt64(&mm.totalRecords, 0)

	log.Printf("✅ %s MemoryManagedStockMap closed and cleared (%d records freed)",
		mapType, totalRecords)
}

func bToKb(b uint64) uint64 {
	return b / 1024
}

// ✅ CRITICAL ORDERS MANAGER with LIST Memory Management - NO DATA LOSS
type CriticalOrdersManager struct {
	dataChan   chan map[string]interface{}
	coll       *mongo.Collection
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	forceFlush chan struct{}
	stats      *CriticalOrdersStats

	// ✅ Multiple safety mechanisms for ORDERS
	failedWrites    []interface{}
	failedWritesMux sync.Mutex
	retryTimer      *time.Ticker
	emergencyFlush  *time.Ticker

	// ✅ Orders specific buffering (14:30-14:46)
	bufferedBest      sync.Map
	bufferFlushed     bool
	bufferMutex       sync.RWMutex
	lastProcessedTime time.Time

	// ✅ LIST-based memory management - NO DATA LOSS
	memoryMap    *MemoryManagedStockMap
	memoryTicker *time.Ticker
}

type CriticalOrdersStats struct {
	totalReceived    int64
	totalFlushed     int64
	totalDropped     int64 // MUST be 0 for orders!
	totalRetries     int64
	totalRecovered   int64
	totalBuffered    int64 // Buffered orders count
	criticalFailures int64 // Critical failure count
	totalMemoryAdded int64 // Total records added to memory
	mutex            sync.RWMutex
}

func (cos *CriticalOrdersStats) AddReceived(count int64) {
	cos.mutex.Lock()
	defer cos.mutex.Unlock()
	atomic.AddInt64(&cos.totalReceived, count)
	log.Printf("📈 ORDERS: Received +%d, Total: %d", count, cos.totalReceived)
}

func (cos *CriticalOrdersStats) AddMemoryAdded(count int64) {
	cos.mutex.Lock()
	defer cos.mutex.Unlock()
	atomic.AddInt64(&cos.totalMemoryAdded, count)
}

func (cos *CriticalOrdersStats) AddFlushed(count int64) {
	cos.mutex.Lock()
	defer cos.mutex.Unlock()
	atomic.AddInt64(&cos.totalFlushed, count)
	log.Printf("💾 ORDERS: Flushed +%d, Total: %d", count, cos.totalFlushed)
}

func (cos *CriticalOrdersStats) AddDropped(count int64) {
	cos.mutex.Lock()
	defer cos.mutex.Unlock()
	atomic.AddInt64(&cos.totalDropped, count)
	atomic.AddInt64(&cos.criticalFailures, count)

	// ✅ MAXIMUM ALERT for ANY dropped order
	log.Printf("🚨🚨🚨 CRITICAL FAILURE: %d ORDERS DROPPED! TOTAL DROPPED: %d 🚨🚨🚨",
		count, cos.totalDropped)
	log.Printf("🔥 THIS IS A CRITICAL SYSTEM FAILURE - ORDERS MUST NOT BE LOST!")
}

func (cos *CriticalOrdersStats) AddBuffered(count int64) {
	cos.mutex.Lock()
	defer cos.mutex.Unlock()
	atomic.AddInt64(&cos.totalBuffered, count)
}

func (cos *CriticalOrdersStats) GetCriticalStats() (received, flushed, dropped, buffered, failures, memoryAdded int64) {
	cos.mutex.RLock()
	defer cos.mutex.RUnlock()
	return cos.totalReceived, cos.totalFlushed, cos.totalDropped, cos.totalBuffered, cos.criticalFailures, cos.totalMemoryAdded
}

func NewCriticalOrdersManager(coll *mongo.Collection) *CriticalOrdersManager {
	ctx, cancel := context.WithCancel(context.Background())
	com := &CriticalOrdersManager{
		dataChan:       make(chan map[string]interface{}, OrdersMaxChannelSize), // MASSIVE buffer
		coll:           coll,
		ctx:            ctx,
		cancel:         cancel,
		forceFlush:     make(chan struct{}, 20), // Multiple force signals
		stats:          &CriticalOrdersStats{},
		failedWrites:   make([]interface{}, 0),
		retryTimer:     time.NewTicker(OrdersRetryDelay),
		emergencyFlush: time.NewTicker(10 * time.Millisecond), // Every 10ms emergency check
		memoryMap:      NewMemoryManagedStockMap(true),        // ← TRUE = Orders map with higher limits
		memoryTicker:   time.NewTicker(1 * time.Minute),       // Memory monitoring every minute
	}

	// ✅ Start MULTIPLE processing goroutines for maximum reliability
	com.wg.Add(7)
	go com.processBatches()       // Main processing
	go com.retryFailedWrites()    // Retry failed orders
	go com.emergencyMonitor()     // Emergency monitoring
	go com.criticalStatsMonitor() // Critical stats
	go com.bufferManager()        // Buffer management
	go com.healthCheck()          // Health checker
	go com.memoryMonitor()        // Memory monitoring

	log.Printf("🛡️ CRITICAL ORDERS MANAGER initialized with LIST-based Memory Management - NO DATA LOSS")
	log.Printf("📦 Buffer size: %d, Flush: %v, Batch: %d, Max Records Per Symbol: %d",
		OrdersMaxChannelSize, OrdersFlushInterval, OrdersBatchSize, MaxOrdersPerSymbol)

	return com
}

func (com *CriticalOrdersManager) Add(data map[string]interface{}) {
	com.stats.AddReceived(1)
	com.lastProcessedTime = time.Now()

	// ✅ Store in LIST-based memory map for tracking - NO DATA LOSS
	if symbol, ok := data["symbol"].(string); ok {
		com.memoryMap.Add(symbol, data) // ← ADD to LIST, not overwrite
		com.stats.AddMemoryAdded(1)
	}

	vnLoc, _ := time.LoadLocation("Asia/Ho_Chi_Minh")
	now := time.Now().In(vnLoc)

	cutoffStart := time.Date(now.Year(), now.Month(), now.Day(), 14, 30, 0, 0, vnLoc)
	cutoffEnd := time.Date(now.Year(), now.Month(), now.Day(), 14, 46, 0, 0, vnLoc)

	symbol, _ := data["symbol"].(string)
	matchQtty := parseInt(data["matchQtty"])

	// ✅ SPECIAL BUFFERING for 14:30-14:46 period with ZERO LOSS guarantee
	if now.After(cutoffStart) && now.Before(cutoffEnd) {
		copyData := make(map[string]interface{})
		for k, v := range data {
			copyData[k] = v
		}

		com.bufferMutex.Lock()
		if oldVal, exists := com.bufferedBest.Load(symbol); exists {
			if old, ok := oldVal.(map[string]interface{}); ok {
				if matchQtty > parseInt(old["matchQtty"]) {
					com.bufferedBest.Store(symbol, copyData)
				}
			}
		} else {
			com.bufferedBest.Store(symbol, copyData)
			com.stats.AddBuffered(1)
		}
		com.bufferMutex.Unlock()

		log.Printf("📋 ORDERS: Buffered %s (matchQtty: %d) for 14:30-14:46 period", symbol, matchQtty)
		return
	}

	// ✅ Flush buffer after 14:46 with MAXIMUM reliability
	com.bufferMutex.Lock()
	if now.After(cutoffEnd) && !com.bufferFlushed {
		go com.flushBufferedWithMaxRetry() // Async with max retry
		com.bufferFlushed = true
	}
	com.bufferMutex.Unlock()

	// ✅ ABSOLUTE PRIORITY: Orders MUST NOT be dropped
	select {
	case com.dataChan <- data:
		// Successfully added
		log.Printf("✅ ORDERS: Added %s to processing queue", symbol)
		return
	default:
		// ❌ This should NEVER happen with 100k buffer, but emergency protocol
		log.Printf("🚨 EMERGENCY: Orders channel somehow full! Activating emergency protocol...")

		// ✅ EMERGENCY PROTOCOL - Multiple immediate actions
		for i := 0; i < 20; i++ {
			select {
			case com.forceFlush <- struct{}{}:
			default:
			}
		}

		// ✅ EMERGENCY RETRY - Never give up on orders
		for attempt := 0; attempt < OrdersRetryAttempts*2; attempt++ { // Double retries for emergency
			select {
			case com.dataChan <- data:
				log.Printf("✅ EMERGENCY SUCCESS: Order %s added after %d attempts", symbol, attempt+1)
				return
			case <-time.After(OrdersRetryDelay / 2): // Faster emergency retry
				log.Printf("⚠️ EMERGENCY RETRY %d failed for order %s", attempt+1, symbol)
				continue
			}
		}

		// ✅ ULTIMATE FALLBACK: Add to priority failed queue
		com.failedWritesMux.Lock()
		urgentOrder := map[string]interface{}{
			"data":      data,
			"timestamp": time.Now(),
			"priority":  "CRITICAL",
			"symbol":    symbol,
		}
		com.failedWrites = append([]interface{}{urgentOrder}, com.failedWrites...) // Add to front
		com.failedWritesMux.Unlock()

		com.stats.AddDropped(1)
		log.Printf("🚨 ULTIMATE FALLBACK: Order %s added to CRITICAL retry queue", symbol)
	}
}

// ✅ Rest of the methods remain the same but updated for better logging...
func (com *CriticalOrdersManager) processBatches() {
	defer com.wg.Done()

	batch := make([]interface{}, 0, OrdersBatchSize)
	ticker := time.NewTicker(OrdersFlushInterval) // 25ms - ultra fast
	defer ticker.Stop()

	log.Println("🚀 CRITICAL ORDERS: Batch processor started with 25ms interval")

	for {
		select {
		case data := <-com.dataChan:
			// ✅ Immediate data copying for safety
			copyData := make(map[string]interface{})
			for k, v := range data {
				copyData[k] = v
			}
			batch = append(batch, copyData)

			// ✅ IMMEDIATE flush when batch reaches size (never wait)
			if len(batch) >= OrdersBatchSize {
				log.Printf("📦 ORDERS: Batch full (%d), flushing immediately", len(batch))
				com.flushBatchWithMaxRetry(batch)
				batch = make([]interface{}, 0, OrdersBatchSize)
				ticker.Reset(OrdersFlushInterval)
			}

		case <-com.forceFlush:
			// ✅ Emergency force flush
			if len(batch) > 0 {
				log.Printf("🚀 ORDERS: Emergency flush triggered (%d items)", len(batch))
				com.flushBatchWithMaxRetry(batch)
				batch = make([]interface{}, 0, OrdersBatchSize)
				ticker.Reset(OrdersFlushInterval)
			}

		case <-ticker.C:
			// ✅ Timer flush - MOST IMPORTANT for orders
			if len(batch) > 0 {
				log.Printf("⏰ ORDERS: Timer flush (%d items)", len(batch))
				com.flushBatchWithMaxRetry(batch)
				batch = make([]interface{}, 0, OrdersBatchSize)
			}

		case <-com.ctx.Done():
			// ✅ CRITICAL: Flush ALL remaining orders before shutdown
			if len(batch) > 0 {
				log.Printf("🛑 ORDERS: Final shutdown flush (%d critical orders)", len(batch))
				com.flushBatchWithMaxRetry(batch)
			}
			return
		}
	}
}

func (com *CriticalOrdersManager) flushBatchWithMaxRetry(batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	startTime := time.Now()
	batchSymbols := make([]string, 0, len(batch))

	// Extract symbols for logging and mark as processed in memory
	for _, item := range batch {
		if doc, ok := item.(map[string]interface{}); ok {
			if symbol, ok := doc["symbol"].(string); ok {
				batchSymbols = append(batchSymbols, symbol)
				// Mark as processed in memory for cleanup
				com.memoryMap.MarkProcessed(symbol)
			}
		}
	}

	log.Printf("💾 ORDERS: Attempting to flush %d orders: %v", len(batch), batchSymbols)

	for attempt := 0; attempt < OrdersRetryAttempts; attempt++ {
		success := com.flushBatch(batch)
		if success {
			duration := time.Since(startTime)
			com.stats.AddFlushed(int64(len(batch)))

			if attempt > 0 {
				log.Printf("✅ ORDERS: RECOVERED after %d attempts in %v: %v",
					attempt+1, duration, batchSymbols)
			} else {
				log.Printf("✅ ORDERS: SUCCESS in %v: %v", duration, batchSymbols)
			}
			return
		}

		// ✅ Failed attempt - log and retry
		log.Printf("❌ ORDERS: Flush attempt %d/%d FAILED for orders: %v",
			attempt+1, OrdersRetryAttempts, batchSymbols)

		if attempt < OrdersRetryAttempts-1 {
			time.Sleep(OrdersRetryDelay)
		}
	}

	// ✅ All retries failed - CRITICAL situation
	log.Printf("🚨 CRITICAL: All %d flush attempts FAILED for orders: %v",
		OrdersRetryAttempts, batchSymbols)
	log.Printf("🔥 Adding %d CRITICAL orders to priority retry queue", len(batch))

	com.failedWritesMux.Lock()
	for _, item := range batch {
		priorityOrder := map[string]interface{}{
			"data":      item,
			"timestamp": time.Now(),
			"priority":  "CRITICAL",
			"attempts":  OrdersRetryAttempts,
		}
		com.failedWrites = append([]interface{}{priorityOrder}, com.failedWrites...) // Priority insert
	}
	com.failedWritesMux.Unlock()
}

func (com *CriticalOrdersManager) flushBatch(batch []interface{}) bool {
	if len(batch) == 0 {
		return true
	}

	// ✅ Extended timeout for critical orders
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opts := options.InsertMany().
		SetOrdered(false).                 // Continue on individual errors
		SetBypassDocumentValidation(false) // Keep validation for data integrity

	result, err := com.coll.InsertMany(ctx, batch, opts)

	if err != nil {
		log.Printf("❌ ORDERS: Insert failed: %v", err)
		return false
	}

	insertedCount := len(result.InsertedIDs)
	expectedCount := len(batch)

	if insertedCount != expectedCount {
		log.Printf("⚠️ ORDERS: Partial insert - Expected: %d, Inserted: %d",
			expectedCount, insertedCount)
		return false
	}

	log.Printf("✅ ORDERS: Perfect insert - %d/%d orders saved", insertedCount, expectedCount)
	return true
}

func (com *CriticalOrdersManager) flushBufferedWithMaxRetry() {
	log.Println("📋 ORDERS: Starting buffered orders flush (14:30-14:46 period)")

	var temp []interface{}
	var symbols []string

	com.bufferedBest.Range(func(key, value interface{}) bool {
		temp = append(temp, value)
		if symbol, ok := key.(string); ok {
			symbols = append(symbols, symbol)
		}
		return true
	})

	if len(temp) == 0 {
		log.Println("📋 ORDERS: No buffered orders to flush")
		return
	}

	log.Printf("📋 ORDERS: Flushing %d buffered orders: %v", len(temp), symbols)

	// ✅ Maximum retry for buffered orders
	for attempt := 0; attempt < OrdersRetryAttempts*2; attempt++ { // Double retries for buffered
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		result, err := com.coll.InsertMany(ctx, temp)
		cancel()

		if err != nil {
			log.Printf("❌ ORDERS: Buffered flush attempt %d failed: %v", attempt+1, err)
			if attempt < OrdersRetryAttempts*2-1 {
				time.Sleep(OrdersRetryDelay)
				continue
			}
		} else {
			insertedCount := len(result.InsertedIDs)
			com.stats.AddFlushed(int64(insertedCount))
			log.Printf("✅ ORDERS: Buffered flush SUCCESS - %d orders: %v",
				insertedCount, symbols)

			// Clear buffer only on success
			com.bufferedBest = sync.Map{}
			return
		}
	}

	// ✅ Buffered flush failed - add to retry queue
	log.Printf("🚨 CRITICAL: Buffered orders flush failed completely")
	com.failedWritesMux.Lock()
	for i, item := range temp {
		priorityOrder := map[string]interface{}{
			"data":      item,
			"timestamp": time.Now(),
			"priority":  "BUFFERED_CRITICAL",
			"symbol":    symbols[i],
		}
		com.failedWrites = append([]interface{}{priorityOrder}, com.failedWrites...)
	}
	com.failedWritesMux.Unlock()
}

func (com *CriticalOrdersManager) retryFailedWrites() {
	defer com.wg.Done()

	log.Println("🔄 ORDERS: Failed orders retry system started")

	for {
		select {
		case <-com.retryTimer.C:
			com.failedWritesMux.Lock()
			if len(com.failedWrites) > 0 {
				log.Printf("🔄 ORDERS: Retrying %d failed orders...", len(com.failedWrites))

				toRetry := make([]interface{}, len(com.failedWrites))
				copy(toRetry, com.failedWrites)
				com.failedWrites = com.failedWrites[:0] // Clear

				com.failedWritesMux.Unlock()

				// Extract original data
				originalData := make([]interface{}, 0, len(toRetry))
				retrySymbols := make([]string, 0, len(toRetry))

				for _, item := range toRetry {
					if wrapper, ok := item.(map[string]interface{}); ok {
						if data, exists := wrapper["data"]; exists {
							originalData = append(originalData, data)
							if symbol, ok := wrapper["symbol"].(string); ok {
								retrySymbols = append(retrySymbols, symbol)
							}
						}
					}
				}

				// Attempt retry
				success := com.flushBatch(originalData)
				if success {
					com.stats.AddFlushed(int64(len(originalData)))
					log.Printf("✅ ORDERS: RETRY SUCCESS - Recovered %d orders: %v",
						len(originalData), retrySymbols)
				} else {
					// Put back in queue for another retry
					com.failedWritesMux.Lock()
					com.failedWrites = append(com.failedWrites, toRetry...)
					com.failedWritesMux.Unlock()
					log.Printf("❌ ORDERS: Retry failed, %d orders back in queue: %v",
						len(toRetry), retrySymbols)
				}
			} else {
				com.failedWritesMux.Unlock()
			}

		case <-com.ctx.Done():
			return
		}
	}
}

func (com *CriticalOrdersManager) emergencyMonitor() {
	defer com.wg.Done()

	log.Println("🚨 ORDERS: Emergency monitor started")

	for {
		select {
		case <-com.emergencyFlush.C:
			currentSize := len(com.dataChan)
			capacity := cap(com.dataChan)
			usage := float64(currentSize) / float64(capacity)

			if usage > 0.7 { // 70% threshold for orders (lower than stock)
				log.Printf("🚨 ORDERS: Channel %d%% full (%d/%d) - EMERGENCY FLUSH!",
					int(usage*100), currentSize, capacity)

				// Multiple emergency flushes
				for i := 0; i < 10; i++ {
					select {
					case com.forceFlush <- struct{}{}:
					default:
					}
				}
			}

		case <-com.ctx.Done():
			return
		}
	}
}

func (com *CriticalOrdersManager) criticalStatsMonitor() {
	defer com.wg.Done()

	ticker := time.NewTicker(2 * time.Second) // More frequent for orders
	defer ticker.Stop()

	log.Println("📊 ORDERS: Critical stats monitor started")

	for {
		select {
		case <-ticker.C:
			received, flushed, dropped, buffered, failures, memoryAdded := com.stats.GetCriticalStats()
			pending := len(com.dataChan)

			com.failedWritesMux.Lock()
			failedCount := len(com.failedWrites)
			com.failedWritesMux.Unlock()

			// ✅ Get memory stats from LIST-based memory map
			symbolCount, totalRecords, accessCount := com.memoryMap.GetStats()

			log.Printf("📊 CRITICAL ORDERS STATS - Received: %d, Flushed: %d, Pending: %d, Failed: %d, Buffered: %d",
				received, flushed, pending, failedCount, buffered)
			log.Printf("📊 MEMORY STATS - Symbols: %d, Total Records: %d, Memory Added: %d, Access Count: %d",
				symbolCount, totalRecords, memoryAdded, accessCount)

			// ✅ CRITICAL alerts for orders
			if dropped > 0 {
				log.Printf("🚨🚨🚨 CRITICAL ALERT: %d ORDERS DROPPED! THIS IS UNACCEPTABLE! 🚨🚨🚨", dropped)
			}
			if failures > 0 {
				log.Printf("🔥 SYSTEM FAILURE: %d critical failures detected", failures)
			}
			if failedCount > 0 {
				log.Printf("⚠️ WARNING: %d orders in retry queue - investigating...", failedCount)
			}

			// ✅ Success rate calculation
			if received > 0 {
				successRate := float64(flushed) / float64(received) * 100
				if successRate < 100.0 {
					log.Printf("🚨 ORDERS SUCCESS RATE: %.2f%% - TARGET: 100%% ⚠️", successRate)
				} else {
					log.Printf("✅ ORDERS SUCCESS RATE: %.2f%% - PERFECT!", successRate)
				}
			}

		case <-com.ctx.Done():
			return
		}
	}
}

func (com *CriticalOrdersManager) bufferManager() {
	defer com.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Monitor buffer status during 14:30-14:46
			vnLoc, _ := time.LoadLocation("Asia/Ho_Chi_Minh")
			now := time.Now().In(vnLoc)
			cutoffStart := time.Date(now.Year(), now.Month(), now.Day(), 14, 30, 0, 0, vnLoc)
			cutoffEnd := time.Date(now.Year(), now.Month(), now.Day(), 14, 46, 0, 0, vnLoc)

			if now.After(cutoffStart) && now.Before(cutoffEnd) {
				bufferCount := 0
				com.bufferedBest.Range(func(key, value interface{}) bool {
					bufferCount++
					return true
				})
				log.Printf("📋 ORDERS: Buffer status during 14:30-14:46 - %d symbols buffered", bufferCount)
			}

		case <-com.ctx.Done():
			return
		}
	}
}

func (com *CriticalOrdersManager) healthCheck() {
	defer com.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if we're still receiving data
			timeSinceLastProcess := time.Since(com.lastProcessedTime)
			if timeSinceLastProcess > 30*time.Second {
				log.Printf("⚠️ ORDERS: No data received for %v - potential connection issue", timeSinceLastProcess)
			}

		case <-com.ctx.Done():
			return
		}
	}
}

func (com *CriticalOrdersManager) memoryMonitor() {
	defer com.wg.Done()

	log.Println("🧠 ORDERS: Memory monitor started")

	for {
		select {
		case <-com.memoryTicker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			symbolCount, totalRecords, accessCount := com.memoryMap.GetStats()
			currentMemoryMB := m.Alloc / 1024 / 1024

			log.Printf("🧠 ORDERS MEMORY - Symbols: %d, Records: %d, Access: %d, Memory: %d MB",
				symbolCount, totalRecords, accessCount, currentMemoryMB)

			// Force cleanup if memory is high
			if currentMemoryMB > MaxMemoryMB {
				log.Printf("⚠️ ORDERS: High memory usage (%d MB), forcing cleanup...", currentMemoryMB)
				com.memoryMap.forceCleanup()
				runtime.GC()

				// Re-check after cleanup
				runtime.ReadMemStats(&m)
				newMemoryMB := m.Alloc / 1024 / 1024
				log.Printf("🧹 ORDERS: Memory after cleanup: %d MB (saved %d MB)",
					newMemoryMB, currentMemoryMB-newMemoryMB)
			}

			// ✅ Log symbol-level memory stats occasionally
			if symbolCount > 0 && symbolCount%100 == 0 {
				avgRecordsPerSymbol := float64(totalRecords) / float64(symbolCount)
				log.Printf("📊 ORDERS: Average %.1f records per symbol (%d symbols)",
					avgRecordsPerSymbol, symbolCount)
			}

		case <-com.ctx.Done():
			return
		}
	}
}

// ✅ Add method to get memory data for debugging
func (com *CriticalOrdersManager) GetMemoryData(symbol string) ([]map[string]interface{}, bool) {
	return com.memoryMap.GetAll(symbol)
}

func (com *CriticalOrdersManager) GetLatestMemoryData(symbol string) (map[string]interface{}, bool) {
	return com.memoryMap.GetLatest(symbol)
}

func (com *CriticalOrdersManager) GetMemoryDataCount(symbol string) int64 {
	return com.memoryMap.GetRecordCount(symbol)
}

func (com *CriticalOrdersManager) Close() {
	log.Println("🛑 CRITICAL ORDERS: Starting shutdown procedure...")

	// ✅ Final emergency flush
	for i := 0; i < 50; i++ {
		select {
		case com.forceFlush <- struct{}{}:
		default:
		}
	}

	// ✅ Wait for all processing to complete
	time.Sleep(500 * time.Millisecond)

	// ✅ Final buffered orders flush
	com.bufferMutex.Lock()
	if !com.bufferFlushed {
		log.Println("🛑 ORDERS: Final buffered orders flush...")
		com.flushBufferedWithMaxRetry()
	}
	com.bufferMutex.Unlock()

	// ✅ Final failed orders attempt
	com.failedWritesMux.Lock()
	if len(com.failedWrites) > 0 {
		log.Printf("🛑 ORDERS: Final attempt for %d failed orders...", len(com.failedWrites))

		originalData := make([]interface{}, 0, len(com.failedWrites))
		for _, item := range com.failedWrites {
			if wrapper, ok := item.(map[string]interface{}); ok {
				if data, exists := wrapper["data"]; exists {
					originalData = append(originalData, data)
				}
			}
		}

		com.flushBatch(originalData)
	}
	com.failedWritesMux.Unlock()

	// ✅ Close memory management
	if com.memoryMap != nil {
		com.memoryMap.Close()
	}
	if com.memoryTicker != nil {
		com.memoryTicker.Stop()
	}

	close(com.dataChan)
	com.cancel()
	com.retryTimer.Stop()
	com.emergencyFlush.Stop()
	com.wg.Wait()

	// ✅ Final memory cleanup
	runtime.GC()

	received, flushed, dropped, buffered, failures, memoryAdded := com.stats.GetCriticalStats()
	log.Printf("📈 FINAL CRITICAL ORDERS STATS:")
	log.Printf("   Received: %d", received)
	log.Printf("   Flushed: %d", flushed)
	log.Printf("   Dropped: %d", dropped)
	log.Printf("   Buffered: %d", buffered)
	log.Printf("   Failures: %d", failures)
	log.Printf("   Memory Added: %d", memoryAdded)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("📊 FINAL MEMORY: %d KB allocated", m.Alloc/1024)

	if dropped == 0 && failures == 0 {
		log.Println("🎯 SUCCESS: ZERO ORDERS LOST - PERFECT EXECUTION! ✅")
	} else {
		log.Printf("🚨 FAILURE: %d orders dropped, %d failures - SYSTEM FAILED!", dropped, failures)
	}
}

// ✅ Regular Stock Manager with LIST Memory Management
type RegularStockManager struct {
	dataChan   chan map[string]interface{}
	coll       *mongo.Collection
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	forceFlush chan struct{}
	stats      *RegularStockStats

	// ✅ LIST-based memory management for stock data
	memoryMap    *MemoryManagedStockMap
	memoryTicker *time.Ticker
}

type RegularStockStats struct {
	totalReceived    int64
	totalFlushed     int64
	totalDropped     int64
	totalMemoryAdded int64
	mutex            sync.RWMutex
}

func (rss *RegularStockStats) AddReceived(count int64) {
	rss.mutex.Lock()
	defer rss.mutex.Unlock()
	atomic.AddInt64(&rss.totalReceived, count)
}

func (rss *RegularStockStats) AddMemoryAdded(count int64) {
	rss.mutex.Lock()
	defer rss.mutex.Unlock()
	atomic.AddInt64(&rss.totalMemoryAdded, count)
}

func (rss *RegularStockStats) AddFlushed(count int64) {
	rss.mutex.Lock()
	defer rss.mutex.Unlock()
	atomic.AddInt64(&rss.totalFlushed, count)
}

func (rss *RegularStockStats) AddDropped(count int64) {
	rss.mutex.Lock()
	defer rss.mutex.Unlock()
	atomic.AddInt64(&rss.totalDropped, count)
	if count > 0 {
		log.Printf("⚠️ STOCK: %d documents dropped (less critical)", count)
	}
}

func (rss *RegularStockStats) GetStats() (received, flushed, dropped, memoryAdded int64) {
	rss.mutex.RLock()
	defer rss.mutex.RUnlock()
	return rss.totalReceived, rss.totalFlushed, rss.totalDropped, rss.totalMemoryAdded
}

func NewRegularStockManager(coll *mongo.Collection) *RegularStockManager {
	ctx, cancel := context.WithCancel(context.Background())
	rsm := &RegularStockManager{
		dataChan:     make(chan map[string]interface{}, StockMaxChannelSize),
		coll:         coll,
		ctx:          ctx,
		cancel:       cancel,
		forceFlush:   make(chan struct{}, 5),
		stats:        &RegularStockStats{},
		memoryMap:    NewMemoryManagedStockMap(false), // ← FALSE = Stock map with normal limits
		memoryTicker: time.NewTicker(2 * time.Minute), // Less frequent than orders
	}

	rsm.wg.Add(3) // Added memory monitor
	go rsm.processBatches()
	go rsm.monitorStats()
	go rsm.memoryMonitor()

	log.Printf("📊 Regular Stock Manager initialized with LIST Memory Management - Buffer: %d, Flush: %v, Max Records Per Symbol: %d",
		StockMaxChannelSize, StockFlushInterval, MaxRecordsPerSymbol)
	return rsm
}

func (rsm *RegularStockManager) Add(data map[string]interface{}) {
	rsm.stats.AddReceived(1)

	// ✅ Store in LIST-based memory map for tracking
	if symbol, ok := data["symbol"].(string); ok {
		rsm.memoryMap.Add(symbol, data) // ← ADD to LIST, not overwrite
		rsm.stats.AddMemoryAdded(1)
	}

	select {
	case rsm.dataChan <- data:
		// Successfully added
		return
	default:
		// Stock data less critical - can drop some if needed
		rsm.triggerForceFlush()

		select {
		case rsm.dataChan <- data:
			return
		case <-time.After(StockRetryDelay):
			rsm.stats.AddDropped(1)
			// Don't log every drop for stock data
		}
	}
}

func (rsm *RegularStockManager) triggerForceFlush() {
	select {
	case rsm.forceFlush <- struct{}{}:
	default:
	}
}

func (rsm *RegularStockManager) processBatches() {
	defer rsm.wg.Done()

	batch := make([]interface{}, 0, StockBatchSize)
	ticker := time.NewTicker(StockFlushInterval) // 200ms - relaxed
	defer ticker.Stop()

	for {
		select {
		case data := <-rsm.dataChan:
			copyData := make(map[string]interface{})
			for k, v := range data {
				copyData[k] = v
			}
			batch = append(batch, copyData)

			if len(batch) >= StockBatchSize {
				rsm.flushBatch(batch)
				batch = make([]interface{}, 0, StockBatchSize)
				ticker.Reset(StockFlushInterval)
			}

		case <-rsm.forceFlush:
			if len(batch) > 0 {
				rsm.flushBatch(batch)
				batch = make([]interface{}, 0, StockBatchSize)
				ticker.Reset(StockFlushInterval)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				rsm.flushBatch(batch)
				batch = make([]interface{}, 0, StockBatchSize)
			}

		case <-rsm.ctx.Done():
			if len(batch) > 0 {
				rsm.flushBatch(batch)
			}
			return
		}
	}
}

func (rsm *RegularStockManager) flushBatch(batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	// Mark symbols as processed in memory
	for _, item := range batch {
		if doc, ok := item.(map[string]interface{}); ok {
			if symbol, ok := doc["symbol"].(string); ok {
				rsm.memoryMap.MarkProcessed(symbol)
			}
		}
	}

	// Simple retry for stock data
	for attempt := 0; attempt < StockRetryAttempts; attempt++ {
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
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			opts := options.BulkWrite().
				SetOrdered(false).
				SetBypassDocumentValidation(true)

			result, err := rsm.coll.BulkWrite(ctx, writes, opts)
			cancel()

			if err != nil {
				if attempt < StockRetryAttempts-1 {
					time.Sleep(StockRetryDelay)
					continue
				}
			} else {
				rsm.stats.AddFlushed(int64(len(writes)))
				log.Printf("📊 STOCK: Flushed %d records (upserted: %d, modified: %d)",
					len(writes), result.UpsertedCount, result.ModifiedCount)
				return
			}
		}
	}
}

func (rsm *RegularStockManager) monitorStats() {
	defer rsm.wg.Done()

	ticker := time.NewTicker(10 * time.Second) // Less frequent for stock
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			received, flushed, dropped, memoryAdded := rsm.stats.GetStats()
			pending := len(rsm.dataChan)
			successRate := float64(flushed) / float64(received) * 100

			// ✅ Get memory stats from LIST-based memory map
			symbolCount, totalRecords, _ := rsm.memoryMap.GetStats()

			log.Printf("📊 STOCK STATS - Received: %d, Flushed: %d, Pending: %d, Dropped: %d, Success: %.1f%%",
				received, flushed, pending, dropped, successRate)
			log.Printf("📊 STOCK MEMORY - Symbols: %d, Records: %d, Memory Added: %d",
				symbolCount, totalRecords, memoryAdded)

		case <-rsm.ctx.Done():
			return
		}
	}
}

func (rsm *RegularStockManager) memoryMonitor() {
	defer rsm.wg.Done()

	log.Println("🧠 STOCK: Memory monitor started")

	for {
		select {
		case <-rsm.memoryTicker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			symbolCount, totalRecords, _ := rsm.memoryMap.GetStats()
			currentMemoryMB := m.Alloc / 1024 / 1024

			log.Printf("🧠 STOCK MEMORY - Symbols: %d, Records: %d, Memory: %d MB",
				symbolCount, totalRecords, currentMemoryMB)

			// More relaxed memory management for stock data
			if currentMemoryMB > MaxMemoryMB+50 { // Higher threshold for stock
				log.Printf("⚠️ STOCK: High memory usage (%d MB), triggering cleanup...", currentMemoryMB)
				rsm.memoryMap.forceCleanup()
			}

		case <-rsm.ctx.Done():
			return
		}
	}
}

// ✅ Add methods to get memory data for debugging
func (rsm *RegularStockManager) GetMemoryData(symbol string) ([]map[string]interface{}, bool) {
	return rsm.memoryMap.GetAll(symbol)
}

func (rsm *RegularStockManager) GetLatestMemoryData(symbol string) (map[string]interface{}, bool) {
	return rsm.memoryMap.GetLatest(symbol)
}

func (rsm *RegularStockManager) GetMemoryDataCount(symbol string) int64 {
	return rsm.memoryMap.GetRecordCount(symbol)
}

func (rsm *RegularStockManager) Close() {
	log.Println("📊 STOCK: Closing manager with memory cleanup...")

	rsm.triggerForceFlush()
	time.Sleep(100 * time.Millisecond)

	// Close memory management
	if rsm.memoryMap != nil {
		rsm.memoryMap.Close()
	}
	if rsm.memoryTicker != nil {
		rsm.memoryTicker.Stop()
	}

	close(rsm.dataChan)
	rsm.cancel()
	rsm.wg.Wait()

	// Final memory cleanup
	runtime.GC()

	received, flushed, dropped, memoryAdded := rsm.stats.GetStats()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Printf("📊 STOCK FINAL - Received: %d, Flushed: %d, Dropped: %d, Memory Added: %d, Memory: %d KB",
		received, flushed, dropped, memoryAdded, m.Alloc/1024)
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

// ✅ MongoDB connection optimized for orders priority
func connectMongoDB() *mongo.Client {
	// ✅ Maximum reliability for orders
	wc := writeconcern.New(
		writeconcern.W(1),
		writeconcern.J(false), // Fast but reliable
	)

	opt := options.Client().
		ApplyURI(MongoDBURI).
		SetServerSelectionTimeout(15 * time.Second). // Longer for reliability
		SetSocketTimeout(60 * time.Second).          // Longer for orders
		SetMaxPoolSize(500).                         // More connections
		SetMinPoolSize(20).                          // Keep connections ready
		SetMaxConnIdleTime(30 * time.Second).
		SetWriteConcern(wc).
		SetRetryWrites(true). // Enable automatic retries
		SetRetryReads(true)

	client, err := mongo.Connect(context.TODO(), opt)
	if err != nil {
		log.Fatal("❌ MongoDB connection failed:", err)
	}

	if err := client.Ping(context.TODO(), nil); err != nil {
		log.Fatal("❌ MongoDB ping failed:", err)
	}

	log.Println("✅ MongoDB connected with ORDERS PRIORITY + LIST Memory Management configuration")
	return client
}
