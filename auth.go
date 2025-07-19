// auth.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type AuthResponse struct {
	Token string `json:"token"`
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// GetAccessToken gọi API lấy token, fallback sang token lưu nếu mã 203033
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("lỗi gửi request: %v", err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK {
		var result AuthResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("lỗi giải mã phản hồi: %v", err)
		}
		// Lưu token mới vào file
		if err := SaveTokenToFile(result.Token); err != nil {
			fmt.Println("⚠️  Không thể lưu token:", err)
		}
		return result.Token, nil
	}

	// Xử lý khi mã lỗi 203033
	var errResp ErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Code == "203033" {
		token, err := LoadTokenFromFile()
		if err != nil {
			return "", fmt.Errorf("OTP sai và không tìm thấy token cũ: %v", err)
		}
		fmt.Println("⚠️  OTP sai, dùng lại token đã lưu.")
		return token, nil
	}

	return "", fmt.Errorf("lỗi từ server: %s", body)
}
