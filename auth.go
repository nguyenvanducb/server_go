// auth.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// AuthResponse struct để parse dữ liệu trả về từ API
type AuthResponse struct {
	Token string `json:"token"`
}

// GetAccessToken gọi API để lấy token
func GetAccessToken(apiKey, otp string) (string, error) {
	url := "https://openapi.tcbs.com.vn/gaia/v1/oauth2/openapi/token"

	payload := map[string]string{
		"apiKey": apiKey,
		"otp":    otp,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("lỗi mã hóa JSON: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return "", fmt.Errorf("lỗi tạo request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("lỗi gửi request: %v", err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("lỗi từ server: %s", body)
	}

	var result struct {
		Token string `json:"token"`
	}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return "", fmt.Errorf("lỗi giải mã phản hồi: %v", err)
	}

	return result.Token, nil
}
