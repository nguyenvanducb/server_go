package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Gọi API tickerCommons từ TCBS, có truyền token
func CallTickerCommonsAPI(token string) error {
	url := "https://openapi.tcbs.com.vn/tartarus/v1/tickerCommons?index=2"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("lỗi tạo request: %w", err)
	}

	// Gắn Bearer token vào header
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("lỗi gửi request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("lỗi API (%d): %s", resp.StatusCode, string(bodyBytes))
	}

	// Đọc và in kết quả JSON
	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("lỗi đọc JSON: %w", err)
	}

	prettyJSON, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println("✅ Dữ liệu trả về từ API tickerCommons:")
	fmt.Println(string(prettyJSON))

	return nil
}
