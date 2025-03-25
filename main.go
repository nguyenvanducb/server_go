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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoDBURI   = "mongodb+srv://hoangminhtri99:Triminh96@cluster0.lu5ww.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
	DBName       = "moneyflow"
	Collection   = "stock_code"
	WebsocketURL = "wss://openapi.tcbs.com.vn/ws/thesis/v1/stream/normal"
	Token        = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGlvcGVuLnRjYnMuY29tLnZuIiwiZXhwIjoxNzQyOTUzOTk3LCJqdGkiOiIiLCJpYXQiOjE3NDI4Njc1OTcsInN1YiI6IjEwMDAwNzE3MDYyIiwic3ViVHlwZSI6ImN1c3RvbWVyIiwiY3VzdG9keUlEIjoiMTA1QzEyODkxNyIsInRjYnNJZCI6IjEwMDAwNzE3MDYyIiwic2Vzc2lvbklEIjoiYzM5YTNhZGEtNjA4OC00MzU0LWFkYTgtMDgzZTgzZTk4ODAxIiwiY2xpZW50SUQiOiIxIiwic3Vic2NyaXB0aW9uIjoiYmFzaWMiLCJzY29wZSI6WyJib25kIiwiZnVuZCIsInN0b2NrIl0sInN0ZXB1cF9leHAiOjE3NDI4OTYzOTcsIm90cCI6IjczNTk2MyIsIm90cFR5cGUiOiJUT1RQIiwib3RwU291cmNlIjoiVENJTlZFU1QiLCJvdHBTZXNzaW9uSWQiOiIwMDQ4YWRiOS1mZDc5LTQwMDgtYWM0YS1kMjJlNDBjYjMwMWEiLCJhY2NvdW50VHlwZSI6InByaW1hcnkiLCJhY2NvdW50X3N0YXR1cyI6IjEiLCJlbWFpbCI6ImhvYW5nbWluaHRyaTk5QGdtYWlsLmNvbSIsInJvbGVzIjpbImN1c3RvbWVyIiwiQXBwbGljYXRpb24vT1BFTl9BUElfUElMT1QiXSwiY2xpZW50X2tleSI6Ik9MMEVWdE9XTDhISUVjaC9hV240MTlMQ2tBK0p5UXBYeW1naU9pRG1pSVdRMFFGcmFkc1RjKzBpNHZvRjdmWTUifQ.a7gzyxvCAn0oOHR87lcMReWKPkomXRoMzkrBlQzPA_2zEo-jc9yMa8KcaQDiRH4X4lWEm08onWtPCa8tpZ2wd07idJPRui5qnx5H3EtYrPzn-YqjJSgZdVZL_dNVOKD99vuKQmtC9dWVz25_4OcluzGj5k1raZKfWQG04tFzmHFi3b09dh2KI2_d-i3pcbt_Z3BeE1RMAkH1qSQI2Xo6hW2QGll0hzS5_ElO5kaFeBE1YX8L8hqbHCGam8e0KPJJAayKLAt8lOjWIt3C7zr2U_RC6GQz7UQ-iN8pbkTTznc0wOzwdc0SLEPCEOcvwinPvpjLaTpRBY7T0y0KtfuvGQ"
	BatchSize    = 10 // Số lượng bản ghi trong một batch
)

var (
	conn           *websocket.Conn
	dbCollection   *mongo.Collection
	timeoutSeconds = 15
	batchData      []interface{}
	batchMutex     sync.Mutex
)

func main() {
	// Kết nối MongoDB
	clientOptions := options.Client().ApplyURI(MongoDBURI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatalf("❌ Lỗi khi kết nối MongoDB: %v", err)
	}
	defer client.Disconnect(context.TODO())

	// Kiểm tra kết nối MongoDB
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatalf("❌ Không thể ping MongoDB: %v", err)
	}
	fmt.Println("✅ Kết nối thành công đến MongoDB!")

	dbCollection = client.Database(DBName).Collection(Collection)

	// Kết nối WebSocket
	connectWebSocket()
}

func connectWebSocket() {
	var err error
	conn, _, err = websocket.DefaultDialer.Dial(WebsocketURL, nil)
	if err != nil {
		log.Fatalf("❌ Lỗi kết nối WebSocket: %v", err)
	}
	defer conn.Close()
	fmt.Println("✅ Kết nối WebSocket thành công!")

	// Gửi xác thực
	authenticate()

	// Lắng nghe tin nhắn từ WebSocket
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("🔥 Lỗi WebSocket:", err)
			break
		}
		handleMessage(string(message))
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

	// Lưu vào batch
	addToBatch(jsonData)
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
	tempBatch := make([]interface{}, len(batchData))
	copy(tempBatch, batchData)
	batchData = nil

	// Chèn dữ liệu vào MongoDB
	_, err := dbCollection.InsertMany(context.TODO(), tempBatch)
	if err != nil {
		fmt.Println("❌ Lỗi khi lưu batch vào MongoDB:", err)
	} else {
		fmt.Printf("✅ Đã lưu %d bản ghi vào MongoDB.\n", len(tempBatch))
	}
}

func restartPing() {
	ticker := time.NewTicker(time.Duration(timeoutSeconds-5) * time.Second)
	go func() {
		for range ticker.C {
			conn.WriteMessage(websocket.TextMessage, []byte("d|p|||"))
			fmt.Println("📡 Gửi ping...")
		}
	}()
}

func subscribeStockPrices() {
	subscribeMessage := "d|s|tk|bi+tm+op|ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,LPB,MBB,MSN,MWG,PLX,SAB,SHB,SSB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
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
