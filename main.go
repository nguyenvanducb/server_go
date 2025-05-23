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
	MongoDBURI      = "mongodb+srv://hoangminhtri99:Triminh96@cluster0.lu5ww.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
	DBName          = "moneyflow"
	Collection      = "stock_code"
	CollectionOrder = "orders"
	WebsocketURL    = "wss://openapi.tcbs.com.vn/ws/thesis/v1/stream/normal"
	Token           = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGlvcGVuLnRjYnMuY29tLnZuIiwiZXhwIjoxNzQ4MDUzNzg1LCJqdGkiOiIiLCJpYXQiOjE3NDc5NjczODUsInN1YiI6IjEwMDAwNzE3MDYyIiwic3ViVHlwZSI6ImN1c3RvbWVyIiwiY3VzdG9keUlEIjoiMTA1QzEyODkxNyIsInRjYnNJZCI6IjEwMDAwNzE3MDYyIiwic2Vzc2lvbklEIjoiM2Q0YThhYTctM2FiNS00NWEyLThkYTEtZjFmMGE4OGI2NTJjIiwiY2xpZW50SUQiOiIxIiwic3Vic2NyaXB0aW9uIjoiYmFzaWMiLCJzY29wZSI6WyJib25kIiwiZnVuZCIsInN0b2NrIl0sInN0ZXB1cF9leHAiOjE3NDc5OTYxODUsIm90cCI6IjYwNDU3MCIsIm90cFR5cGUiOiJUT1RQIiwib3RwU291cmNlIjoiVENJTlZFU1QiLCJvdHBTZXNzaW9uSWQiOiI3YTllYTI1MS1kMzhlLTQ0NTItODM1MC02MTQ2Zjk1NTBkNzAiLCJhY2NvdW50VHlwZSI6InByaW1hcnkiLCJhY2NvdW50X3N0YXR1cyI6IjEiLCJlbWFpbCI6ImhvYW5nbWluaHRyaTk5QGdtYWlsLmNvbSIsInJvbGVzIjpbImN1c3RvbWVyIiwiQXBwbGljYXRpb24vT1BFTl9BUElfUElMT1QiXSwiY2xpZW50X2tleSI6Ik9MMEVWdE9XTDhISUVjaC9hV240MTlMQ2tBK0p5UXBYeW1naU9pRG1pSVdRMFFGcmFkc1RjKzBpNHZvRjdmWTUifQ.YvvPmuhBMCxmYV1n7t2RD0ruZtai1m6ySYIIKgWSlu99HVGDsV75hCRTgAhvPrODkvnlKU_lFQ8K_JSfClcZhp1bUvlxT-TgDTzR-5Plsz4CMW5IwKilZSGJJ8dDWGGkXUqPoRhsh6tD6oFJF7I2VWW8MxBqep01CX98QHgXcT8dEkh7u9tmvU3VL9kwC5atz98tdYI5ntARc_fY3r-0TDK5-PjN8Vm79t8qNJrXq3K35YlwmaAEh1-_vbutnQoznB9nfZp38rT-mnQIx0byuoqEJxyb2nwzbryRk9t8Hq1VuIMzgUkXr4CoVzAHmkwoUtBfXMisQrpOOKSz7PTGbQ"
	BatchSize       = 2 // Số lượng bản ghi trong một batch
)

var (
	conn           *websocket.Conn
	dbCollection   *mongo.Collection
	timeoutSeconds = 15
	batchData      []interface{}
	batchMutex     sync.Mutex
)

var mapStock = make(map[string]map[string]interface{})
var (
	batchOrderData  []interface{}
	batchOrderMutex sync.Mutex
)
var wsWriteLock sync.Mutex
var dbCollectionOrder *mongo.Collection

func connectMongoDB() *mongo.Client {
	// Cấu hình tùy chọn kết nối MongoDB
	clientOptions := options.Client().
		ApplyURI(MongoDBURI).
		SetServerSelectionTimeout(10 * time.Second). // Tăng timeout chọn server
		SetSocketTimeout(30 * time.Second).          // Timeout cho socket
		SetMaxPoolSize(100).                         // Giới hạn số kết nối tối đa
		SetMinPoolSize(5).                           // Giữ kết nối tối thiểu
		SetHeartbeatInterval(10 * time.Second)       // Ping server để giữ kết nối

	// Thử kết nối với MongoDB
	var client *mongo.Client
	var err error

	for i := 0; i < 3; i++ { // Thử lại tối đa 3 lần
		client, err = mongo.Connect(context.TODO(), clientOptions)
		if err == nil {
			break // Kết nối thành công, thoát vòng lặp
		}
		fmt.Printf("❌ Lỗi khi kết nối MongoDB (lần %d): %v\n", i+1, err)
		time.Sleep(3 * time.Second) // Đợi 3 giây trước khi thử lại
	}

	if err != nil {
		log.Fatalf("❌ Không thể kết nối MongoDB sau 3 lần thử: %v", err)
	}

	// Kiểm tra kết nối
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatalf("❌ Không thể ping đến MongoDB: %v", err)
	}

	fmt.Println("✅ Kết nối thành công đến MongoDB!")
	return client
}

func main() {
	// Kết nối MongoDB
	// clientOptions := options.Client().ApplyURI(MongoDBURI)
	// client, err := mongo.Connect(context.TODO(), clientOptions)
	client := connectMongoDB()

	defer client.Disconnect(context.TODO())

	fmt.Println("✅ Kết nối thành công đến MongoDB!")

	dbCollection = client.Database(DBName).Collection(Collection)
	dbCollectionOrder = client.Database(DBName).Collection(CollectionOrder)

	// Kết nối WebSocket
	connectWebSocket()
}
func connectWebSocket() {
	for {
		var err error
		conn, _, err = websocket.DefaultDialer.Dial(WebsocketURL, nil)
		if err != nil {
			log.Printf("❌ Lỗi kết nối WebSocket: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		fmt.Println("✅ Kết nối WebSocket thành công!")

		// Gửi xác thực
		authenticate()

		// Lắng nghe tin nhắn từ WebSocket
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				fmt.Println("🔥 Lỗi WebSocket:", err)
				conn.Close() // Đóng kết nối cũ
				break        // Thoát vòng lặp đọc tin nhắn để thử kết nối lại
			}
			handleMessage(string(message))
		}
	}
}

func authenticate() {
	base64Token := base64.StdEncoding.EncodeToString([]byte(Token))
	authMessage := fmt.Sprintf("d|a|||%s", base64Token)
	err := conn.WriteMessage(websocket.TextMessage, []byte(authMessage))
	if err != nil {
		fmt.Println("❌ Lỗi gửi xác thực:", err)
	}
}

func handleMessage(message string) {
	if strings.HasPrefix(message, "d|33|") {
		// Cập nhật timeout từ server
		parts := strings.Split(message, "|")
		if len(parts) == 3 {
			newTimeout := parseInt(parts[2], 15)
			fmt.Printf("⏳ Cập nhật timeout: %d giây\n", newTimeout)
			timeoutSeconds = newTimeout
			restartPing()
		}
	} else if strings.HasPrefix(message, "d|0|") {
		// Xác thực thành công
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(message[4:]), &data); err == nil {
			if success, ok := data["success"].(bool); ok && success {
				fmt.Println("✅ Xác thực thành công!")
				subscribeStockPrices()
			} else {
				fmt.Println("❌ Xác thực thất bại:", data["error"])
			}
		}
	} else {
		// Xử lý JSON từ WebSocket
		processJsonData(message)
	}
}

func processJsonData(input string) {
	if len(input) < 20 {
		return
	}

	var code = input[:3]
	// println(code)

	// Tìm JSON trong chuỗi
	start := strings.Index(input, "{")
	end := strings.LastIndex(input, "}")
	if start == -1 || end == -1 || start >= end {
		fmt.Println("Không tìm thấy JSON hợp lệ.")
		return
	}

	jsonString := input[start : end+1]
	var jsonData map[string]interface{}

	if err := json.Unmarshal([]byte(jsonString), &jsonData); err != nil {
		fmt.Println("❌ Lỗi giải mã JSON:", err)
		return
	}
	// Thêm timestamp vào dữ liệu
	jsonData["time"] = time.Now()
	mapData(code, jsonData)
	// Lưu vào batch
	addToBatch(jsonData)
}

func mapData(code string, jsonData map[string]interface{}) {
	symbolRaw, exists := jsonData["symbol"]
	if !exists {
		fmt.Println("❌ Không có trường 'symbol'")
		return
	}

	symbol, ok := symbolRaw.(string)
	if !ok {
		fmt.Println("❌ 'symbol' không phải kiểu string")
		return
	}

	// Cập nhật mapStock
	if _, found := mapStock[symbol]; !found {
		mapStock[symbol] = jsonData
	} else {
		for k, v := range jsonData {
			mapStock[symbol][k] = v
		}
	}
	// fmt.Println(mapStock[symbol])

	// 👇 Nếu code là "s|6", đưa vào batch "order"
	if code == "s|6" {
		addOrderToBatch(mapStock[symbol])
		// fmt.Printf("📥 Đưa vào batch Order: %v\n", jsonData)
	}
}

func addOrderToBatch(data map[string]interface{}) {
	batchOrderMutex.Lock()
	batchOrderData = append(batchOrderData, data)

	if len(batchOrderData) >= 1 {
		saveOrderBatchToMongoDB()
	}
	batchOrderMutex.Unlock()
}

func saveOrderBatchToMongoDB() {
	if len(batchOrderData) == 0 {
		return
	}

	tempBatch := batchOrderData
	batchOrderData = nil

	_, err := dbCollectionOrder.InsertMany(context.TODO(), tempBatch)
	if err != nil {
		fmt.Println("❌ Lỗi khi insert batch vào 'order':", err)
	} else {
		fmt.Printf("📥 Đã insert %d bản ghi vào CollectionOrder.\n", len(tempBatch))
	}
}

func addToBatch(data map[string]interface{}) {
	batchMutex.Lock()
	batchData = append(batchData, data)

	// Nếu đạt batchSize, lưu vào MongoDB
	if len(batchData) >= BatchSize {
		saveBatchToMongoDB()
	}
	batchMutex.Unlock()
}
func saveBatchToMongoDB() {
	if len(batchData) == 0 {
		return
	}

	// Copy dữ liệu batch và làm rỗng batchData
	tempBatch := batchData
	batchData = nil // Xóa dữ liệu gốc để tránh ghi đè

	// Cập nhật dữ liệu theo symbol thay vì chèn mới
	var writes []mongo.WriteModel

	for _, d := range tempBatch {
		// Ép kiểu data về đúng dạng map[string]interface{}
		data, ok := d.(map[string]interface{})
		if !ok {
			fmt.Println("❌ Dữ liệu không hợp lệ, bỏ qua:", d)
			continue
		}

		// Lấy symbol
		symbol, ok := data["symbol"].(string)
		if !ok {
			fmt.Println("❌ Dữ liệu thiếu 'symbol', bỏ qua:", data)
			continue
		}

		// Tạo bộ lọc và cập nhật
		filter := bson.M{"symbol": symbol}
		update := bson.M{"$set": data}

		// Sử dụng bulk update
		writes = append(writes, mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true))
	}

	// Thực hiện cập nhật hàng loạt (bulk write)
	if len(writes) > 0 {
		_, err := dbCollection.BulkWrite(context.TODO(), writes)
		if err != nil {
			fmt.Println("❌ Lỗi khi cập nhật batch vào MongoDB:", err)
		} else {
			fmt.Printf("✅ Đã cập nhật %d bản ghi vào MongoDB.\n", len(tempBatch))
		}
	}
}

func restartPing() {
	ticker := time.NewTicker(time.Duration(timeoutSeconds-5) * time.Second)
	go func() {
		for range ticker.C {
			wsWriteLock.Lock()
			err := conn.WriteMessage(websocket.TextMessage, []byte("d|p|||"))
			wsWriteLock.Unlock()

			if err != nil {
				fmt.Println("❌ Lỗi khi gửi ping:", err)
				return // hoặc xử lý reconnect tại đây
			}
			fmt.Println("📡 Gửi ping...")
		}
	}()
}

func subscribeStockPrices() {
	subscribeMessage := "d|s|tk|bp+bi+tm+op+fe|ACB,BCM,BID,BVH,CTG,TCB,TPB,VCB,VHM,VIB"
	err := conn.WriteMessage(websocket.TextMessage, []byte(subscribeMessage))
	if err != nil {
		fmt.Println("❌ Lỗi đăng ký nhận dữ liệu:", err)
	} else {
		fmt.Println("📈 Đăng ký nhận dữ liệu cổ phiếu: ACB, SSI, HPG, MBB")
	}
}

func parseInt(str string, defaultValue int) int {
	var value int
	_, err := fmt.Sscanf(str, "%d", &value)
	if err != nil {
		return defaultValue
	}
	return value
}
