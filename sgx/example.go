package sgx

import "os"

func Test() {

	//1.创建私钥
	key, err := NewKey("111111111111111111111", 2)
	if err != nil {
		return
	}

	//2.要写入的文件
	f, err := os.OpenFile("d:/test", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	//3.解密,data是下载到的未解密数据块
	data := []byte{}
	block := NewEncryptedBlock(data)
	err = block.Decode(key, f)
	if err != nil {
		return
	}
}
