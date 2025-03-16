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
	MongoDBURI   = "mongodb+srv://ducnguyen95hust:24Eh8M7ZfwWbecf7@cluster0.dvd5q.mongodb.net/"
	DBName       = "moneyflow"
	Collection   = "orders"
	WebsocketURL = "wss://openapi.tcbs.com.vn/ws/thesis/v1/stream/normal"
	Token        = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGlvcGVuLnRjYnMuY29tLnZuIiwiZXhwIjoxNzQyMjI0NzIyLCJqdGkiOiIiLCJpYXQiOjE3NDIxMzgzMjIsInN1YiI6IjEwMDAwNzE3MDYyIiwic3ViVHlwZSI6ImN1c3RvbWVyIiwiY3VzdG9keUlEIjoiMTA1QzEyODkxNyIsInRjYnNJZCI6IjEwMDAwNzE3MDYyIiwic2Vzc2lvbklEIjoiN2I4ZTAzZWUtN2QzZC00M2Y4LTgzMjAtNmZiNjgzYjhiOTAwIiwiY2xpZW50SUQiOiIxIiwic3Vic2NyaXB0aW9uIjoiYmFzaWMiLCJzY29wZSI6WyJib25kIiwiZnVuZCIsInN0b2NrIl0sInN0ZXB1cF9leHAiOjE3NDIxNjcxMjIsIm90cCI6IjA4NDYwMSIsIm90cFR5cGUiOiJUT1RQIiwib3RwU291cmNlIjoiVENJTlZFU1QiLCJvdHBTZXNzaW9uSWQiOiIyNGVmM2FmNi05ZGI5LTQyMTItODdmOC00NjA0MWUyMzU4MzEiLCJhY2NvdW50VHlwZSI6InByaW1hcnkiLCJhY2NvdW50X3N0YXR1cyI6IjEiLCJlbWFpbCI6ImhvYW5nbWluaHRyaTk5QGdtYWlsLmNvbSIsInJvbGVzIjpbImN1c3RvbWVyIiwiQXBwbGljYXRpb24vT1BFTl9BUElfUElMT1QiXSwiY2xpZW50X2tleSI6Ik9MMEVWdE9XTDhISUVjaC9hV240MTlMQ2tBK0p5UXBYeW1naU9pRG1pSVdRMFFGcmFkc1RjKzBpNHZvRjdmWTUifQ.l7EY70ApD71rEt4G7CVAsojxe5Nvtnb59wn-xifPMitsfjP4Boi7x9VmiLAotWdSzGAgH97B88gcXtVFj_yBbwG-ctM9RP138F_09yVQWMckt0eqOrYaCiVJAeUf-twQPJP3Idfn5p5AKmRRDocHPMghu6OJZwOzV7Gs64cI7SRfuyVzurdIo1MO1TcoXW1SBDYQTCBqCWHEtUxlSzB6jtV6wKBNDLWERTsY3WpwCY8GgjF6tNj5XyjUOKAPV73X5mjL0SyhlaNf-w0NO1Bj8NQ7Iav_M86SJb4Zk-n9oVCK9R-hwXSL86FscgRX5vCN72cmrYJKvB-JF37UZLCetA"
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
	subscribeMessage := "d|s|tk|bi+tm+op|ACB,SSI,HPG,MBB"
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
