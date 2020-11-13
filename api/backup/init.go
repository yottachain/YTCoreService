package api

var UClient *Client

type DupBlock struct {
	OriginalSize int64
	RealSize     int64
	VHP          []byte
	KEU          []byte
	VHB          []byte
}

type NODupBlock struct {
	OriginalSize int64
	RealSize     int64
	VHP          []byte
	KEU          []byte
	KED          []byte
	DATA         []byte
}

func StartServer() {

}

func StopServer() {

}
