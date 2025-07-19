package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

// TickerCommonData represents the actual structure of ticker data from TCBS API
type TickerCommonData struct {
	Symbol string `json:"symbol" bson:"symbol"`

	// Price levels (Các mức giá)
	CeilPrice  float64 `json:"ceilPrice" bson:"ceilPrice"`   // Giá trần
	FloorPrice float64 `json:"floorPrice" bson:"floorPrice"` // Giá sàn
	RefPrice   float64 `json:"refPrice" bson:"refPrice"`     // Giá tham chiếu

	// Bid prices and quantities (Giá mua - 3 level)
	BidPrice01 float64 `json:"bidPrice01" bson:"bidPrice01"`
	BidPrice02 float64 `json:"bidPrice02" bson:"bidPrice02"`
	BidPrice03 float64 `json:"bidPrice03" bson:"bidPrice03"`
	BidQtty01  float64 `json:"bidQtty01" bson:"bidQtty01"`
	BidQtty02  float64 `json:"bidQtty02" bson:"bidQtty02"`
	BidQtty03  float64 `json:"bidQtty03" bson:"bidQtty03"`

	// Offer prices and quantities (Giá bán - 3 level)
	OfferPrice01 float64 `json:"offerPrice01" bson:"offerPrice01"`
	OfferPrice02 float64 `json:"offerPrice02" bson:"offerPrice02"`
	OfferPrice03 float64 `json:"offerPrice03" bson:"offerPrice03"`
	OfferQtty01  float64 `json:"offerQtty01" bson:"offerQtty01"`
	OfferQtty02  float64 `json:"offerQtty02" bson:"offerQtty02"`
	OfferQtty03  float64 `json:"offerQtty03" bson:"offerQtty03"`

	// Match data (Khớp lệnh)
	MatchPrice float64 `json:"matchPrice" bson:"matchPrice"` // Giá khớp
	MatchQtty  float64 `json:"matchQtty" bson:"matchQtty"`   // Khối lượng khớp

	// Trading statistics (Thống kê giao dịch)
	Change        float64 `json:"change" bson:"change"`               // Thay đổi giá
	ChangePercent float64 `json:"changePercent" bson:"changePercent"` // Phần trăm thay đổi
	Open          float64 `json:"open" bson:"open"`                   // Giá mở cửa
	Avg           float64 `json:"avg" bson:"avg"`                     // Giá trung bình
	High          float64 `json:"high" bson:"high"`                   // Giá cao nhất
	Low           float64 `json:"low" bson:"low"`                     // Giá thấp nhất
	TotalVol      float64 `json:"totalVol" bson:"totalVol"`           // Tổng khối lượng
	TotalVal      float64 `json:"totalVal" bson:"totalVal"`           // Tổng giá trị (scientific notation)

	// Foreign trading (Giao dịch nước ngoài)
	SellForeignQtty float64 `json:"sellForeignQtty" bson:"sellForeignQtty"` // KL bán NĐT nước ngoài
	Room            float64 `json:"room" bson:"room"`                       // Room NĐT nước ngoài

	// Additional data
	IndexNumber    int     `json:"indexNumber" bson:"indexNumber"`       // Số thứ tự
	NextCeilPrice  float64 `json:"nextCeilPrice" bson:"nextCeilPrice"`   // Giá trần phiên sau
	NextFloorPrice float64 `json:"nextFloorPrice" bson:"nextFloorPrice"` // Giá sàn phiên sau
	NextRefPrice   float64 `json:"nextRefPrice" bson:"nextRefPrice"`     // Giá tham chiếu phiên sau

	// Metadata (Thêm bởi hệ thống)
	UpdatedAt      time.Time `json:"updatedAt" bson:"updatedAt"`
	CreatedAt      time.Time `json:"createdAt" bson:"createdAt"`
	DataSource     string    `json:"dataSource" bson:"dataSource"`
	ProcessingTime time.Time `json:"processingTime" bson:"processingTime"`
}

// TickerCommonsResponse represents the API response structure
type TickerCommonsResponse struct {
	Data []map[string]interface{} `json:"data"`
	Meta map[string]interface{}   `json:"meta"`
}

// CallTickerCommonsAPI calls TCBS API and saves data to MongoDB using RegularStockManager
func CallTickerCommonsAPI(token string, stockManager *RegularStockManager) error {
	log.Println("🚀 Starting TickerCommons API call...")

	url := "https://openapi.tcbs.com.vn/tartarus/v1/tickerCommons?index=2"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("❌ Lỗi tạo request: %w", err)
	}

	// Gắn Bearer token vào header
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "MoneyFlow-Ticker-Client/1.0")

	client := &http.Client{
		Timeout: 30 * time.Second, // Set timeout để tránh treo
	}

	log.Printf("📡 Calling TCBS API: %s", url)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("❌ Lỗi gửi request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("❌ Lỗi API (%d): %s", resp.StatusCode, string(bodyBytes))
	}

	// Đọc response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("❌ Lỗi đọc response body: %w", err)
	}

	// Parse JSON response
	var response TickerCommonsResponse
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return fmt.Errorf("❌ Lỗi parse JSON: %w", err)
	}

	log.Printf("✅ API response received - Data count: %d", len(response.Data))

	// Process and save each ticker data
	processedCount := 0
	errorCount := 0
	currentTime := time.Now()

	for i, rawData := range response.Data {
		// Enrich data with timestamps and metadata
		enrichedData := make(map[string]interface{})

		// Copy all original data
		for k, v := range rawData {
			enrichedData[k] = v
		}

		// Add timestamps and metadata
		enrichedData["updatedAt"] = currentTime
		enrichedData["processingTime"] = currentTime

		// Ensure createdAt exists (only set if not already present)
		if _, exists := enrichedData["createdAt"]; !exists {
			enrichedData["createdAt"] = currentTime
		}

		// Add processing metadata
		enrichedData["dataSource"] = "TCBS_TickerCommons"
		enrichedData["apiIndex"] = 2
		enrichedData["recordIndex"] = i + 1

		// Extract symbol for logging and validation
		symbol, symbolOk := enrichedData["symbol"].(string)
		matchPrice, _ := enrichedData["matchPrice"].(float64)
		matchQtty, _ := enrichedData["matchQtty"].(float64)
		change, _ := enrichedData["change"].(float64)
		changePercent, _ := enrichedData["changePercent"].(float64)

		// Validate required fields
		if !symbolOk || symbol == "" {
			log.Printf("⚠️ Record %d: Missing or invalid symbol: %+v", i+1, rawData)
			errorCount++
			continue
		}

		// Log detailed ticker info
		log.Printf("📊 Processing %s: Match=%.0f@%.0f, Change=%.1f (%.2f%%), Vol=%.0f",
			symbol, matchPrice, matchQtty, change, changePercent,
			getFloat64(enrichedData, "totalVol"))

		// Handle scientific notation in totalVal (e.g., 3.33E9)
		if totalVal, exists := enrichedData["totalVal"]; exists {
			if val, ok := totalVal.(float64); ok {
				enrichedData["totalVal"] = val
				enrichedData["totalValFormatted"] = fmt.Sprintf("%.0f", val)
			}
		}

		// Calculate additional derived fields
		if bidPrice01, exists := enrichedData["bidPrice01"].(float64); exists {
			if offerPrice01, exists2 := enrichedData["offerPrice01"].(float64); exists2 && bidPrice01 > 0 && offerPrice01 > 0 {
				spread := offerPrice01 - bidPrice01
				spreadPercent := (spread / bidPrice01) * 100
				enrichedData["bidOfferSpread"] = spread
				enrichedData["bidOfferSpreadPercent"] = spreadPercent
			}
		}

		// Add market status indicators
		if refPrice, exists := enrichedData["refPrice"].(float64); exists && refPrice > 0 {
			if matchPrice > 0 {
				if matchPrice >= getFloat64(enrichedData, "ceilPrice") {
					enrichedData["marketStatus"] = "CEILING"
				} else if matchPrice <= getFloat64(enrichedData, "floorPrice") {
					enrichedData["marketStatus"] = "FLOOR"
				} else if change > 0 {
					enrichedData["marketStatus"] = "UP"
				} else if change < 0 {
					enrichedData["marketStatus"] = "DOWN"
				} else {
					enrichedData["marketStatus"] = "UNCHANGED"
				}
			}
		}

		// Send to RegularStockManager for processing
		if stockManager != nil {
			stockManager.Add(enrichedData)
			log.Printf("✅ Added %s to stock processing queue (Match: %.0f, Vol: %.0f)",
				symbol, matchPrice, getFloat64(enrichedData, "totalVol"))
		} else {
			log.Printf("⚠️ StockManager is nil, cannot save %s", symbol)
			errorCount++
			continue
		}

		processedCount++
	}

	log.Printf("✅ TickerCommons processing completed:")
	log.Printf("   📈 Total records: %d", len(response.Data))
	log.Printf("   ✅ Processed: %d", processedCount)
	log.Printf("   ❌ Errors: %d", errorCount)
	log.Printf("   📊 Success rate: %.2f%%", float64(processedCount)/float64(len(response.Data))*100)

	// Log sample data for debugging (first record only)
	if len(response.Data) > 0 {
		sample := response.Data[0]
		log.Printf("📋 Sample ticker data for %s:", sample["symbol"])
		log.Printf("   💰 Match: %.0f @ %.0f", getFloat64Sample(sample, "matchPrice"), getFloat64Sample(sample, "matchQtty"))
		log.Printf("   📈 Change: %.1f (%.2f%%)", getFloat64Sample(sample, "change"), getFloat64Sample(sample, "changePercent"))
		log.Printf("   🎯 Bid/Offer: %.0f-%.0f", getFloat64Sample(sample, "bidPrice01"), getFloat64Sample(sample, "offerPrice01"))
		log.Printf("   📊 Volume: %.0f, Value: %.0f", getFloat64Sample(sample, "totalVol"), getFloat64Sample(sample, "totalVal"))

		// Full sample for debugging (truncated)
		sampleJSON, _ := json.MarshalIndent(sample, "", "  ")
		if len(sampleJSON) > 1000 {
			log.Printf("📄 Full sample (truncated):\n%s...", string(sampleJSON[:1000]))
		} else {
			log.Printf("📄 Full sample:\n%s", string(sampleJSON))
		}
	}

	return nil
}

// Helper function to safely get float64 values
func getFloat64(data map[string]interface{}, key string) float64 {
	if val, exists := data[key]; exists {
		if f, ok := val.(float64); ok {
			return f
		}
	}
	return 0.0
}

// Helper function for sample data logging
func getFloat64Sample(data map[string]interface{}, key string) float64 {
	if val, exists := data[key]; exists {
		if f, ok := val.(float64); ok {
			return f
		}
	}
	return 0.0
}

// CallTickerCommonsAPIWithDirectSave calls API and saves directly to MongoDB (alternative method)
func CallTickerCommonsAPIWithDirectSave(token string, collection *mongo.Collection) error {
	log.Println("🚀 Starting TickerCommons API call with direct save...")

	url := "https://openapi.tcbs.com.vn/tartarus/v1/tickerCommons?index=2"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("❌ Lỗi tạo request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("❌ Lỗi gửi request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("❌ Lỗi API (%d): %s", resp.StatusCode, string(bodyBytes))
	}

	var response TickerCommonsResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("❌ Lỗi đọc JSON: %w", err)
	}

	// Prepare documents for batch insert
	documents := make([]interface{}, 0, len(response.Data))
	currentTime := time.Now()

	for _, rawData := range response.Data {
		enrichedData := make(map[string]interface{})

		for k, v := range rawData {
			enrichedData[k] = v
		}

		enrichedData["updatedAt"] = currentTime
		if _, exists := enrichedData["createdAt"]; !exists {
			enrichedData["createdAt"] = currentTime
		}
		enrichedData["dataSource"] = "TCBS_TickerCommons"
		enrichedData["apiIndex"] = 2

		documents = append(documents, enrichedData)
	}

	if len(documents) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		result, err := collection.InsertMany(ctx, documents)
		if err != nil {
			return fmt.Errorf("❌ Lỗi lưu vào MongoDB: %w", err)
		}

		log.Printf("✅ Saved %d ticker records to MongoDB", len(result.InsertedIDs))
	}

	return nil
}

// ProcessTickerCommonsInBatches processes ticker commons data in batches for better performance
func ProcessTickerCommonsInBatches(token string, stockManager *RegularStockManager, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	log.Printf("🔄 Processing TickerCommons in batches of %d", batchSize)

	// This is a simplified version - in real implementation you might need pagination
	err := CallTickerCommonsAPI(token, stockManager)
	if err != nil {
		return fmt.Errorf("❌ Batch processing failed: %w", err)
	}

	log.Println("✅ Batch processing completed successfully")
	return nil
}

// GetTickerCommonsWithRetry calls the API with retry mechanism
func GetTickerCommonsWithRetry(token string, stockManager *RegularStockManager, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🔄 TickerCommons API attempt %d/%d", attempt, maxRetries)

		err := CallTickerCommonsAPI(token, stockManager)
		if err == nil {
			log.Printf("✅ TickerCommons API successful on attempt %d", attempt)
			return nil
		}

		lastErr = err
		log.Printf("❌ Attempt %d failed: %v", attempt, err)

		if attempt < maxRetries {
			waitTime := time.Duration(attempt) * 2 * time.Second
			log.Printf("⏳ Waiting %v before retry...", waitTime)
			time.Sleep(waitTime)
		}
	}

	return fmt.Errorf("❌ All %d attempts failed. Last error: %w", maxRetries, lastErr)
}

// Example usage function
func ExampleUsage() {
	// Kết nối MongoDB
	client := connectMongoDB()
	defer client.Disconnect(context.TODO())

	// Tạo collection cho stock_code
	db := client.Database(DBName)
	stockCollection := db.Collection(Collection)

	// Tạo RegularStockManager
	stockManager := NewRegularStockManager(stockCollection)
	defer stockManager.Close()

	// Token từ TCBS (thay bằng token thực)
	token := "your_tcbs_bearer_token_here"

	// Gọi API với retry
	err := GetTickerCommonsWithRetry(token, stockManager, 3)
	if err != nil {
		log.Printf("❌ Failed to fetch ticker commons: %v", err)
		return
	}

	log.Println("✅ TickerCommons data processing completed successfully")
}
