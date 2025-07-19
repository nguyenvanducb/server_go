package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

const tokenFilePath = "token.json"

type TokenData struct {
	Token string `json:"token"`
}

// Lưu token vào file
func SaveTokenToFile(token string) error {
	data := TokenData{Token: token}
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(tokenFilePath, jsonBytes, 0644)
}

// Đọc token từ file
func LoadTokenFromFile() (string, error) {
	if _, err := os.Stat(tokenFilePath); os.IsNotExist(err) {
		return "", errors.New("token file not found")
	}
	data, err := ioutil.ReadFile(tokenFilePath)
	if err != nil {
		return "", err
	}

	var tokenData TokenData
	if err := json.Unmarshal(data, &tokenData); err != nil {
		return "", err
	}
	return tokenData.Token, nil
}
