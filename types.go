package main

// Danh sách nhóm chứng khoán theo từng nhóm
var stockGroups = [][]string{
	{"SSI"},
	{"SHB"},
	{"MWG"},
}

// Chứa tất cả mã cổ phiếu gộp từ các nhóm (biến toàn cục)
var allSymbols []string

// Gọi hàm này trong main() để khởi tạo allSymbols
func initSymbols() {
	for _, group := range stockGroups {
		allSymbols = append(allSymbols, group...)
	}
}
