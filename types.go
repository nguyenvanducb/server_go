package main

// Danh sách nhóm chứng khoán theo từng nhóm
var stockGroups = [][]string{
	{"ACB", "BCM", "BID", "CTG", "TCB", "VCB", "VHM", "VIB", "SSI", "STB"},
	{"FPT", "GAS", "GVR", "HDB", "HPG", "SAB", "SHB", "SSB", "TPB", "BVH"},
	{"LPB", "MBB", "MSN", "MWG", "PLX", "VIC", "VJC", "VNM", "VPB", "VRE"},
}

// Chứa tất cả mã cổ phiếu gộp từ các nhóm (biến toàn cục)
var allSymbols []string

// Gọi hàm này trong main() để khởi tạo allSymbols
func initSymbols() {
	for _, group := range stockGroups {
		allSymbols = append(allSymbols, group...)
	}
}
