package examples

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/yottachain/YTCoreService/env"
)

func TestWriteFile() {
	var FileSize int64 = 1024 * 1024 * 10
	data := env.MakeRandData(FileSize)
	startTime := time.Now()
	wgroup := sync.WaitGroup{}
	for ii := 0; ii < 10; ii++ {
		wgroup.Add(1)
		go LoopWriteFile(data, "test", ii, &wgroup)
	}
	wgroup.Wait()
	fmt.Printf("write 20000M,take times %d ms.\n", time.Since(startTime).Milliseconds())

}

func LoopWriteFile(data []byte, name string, id int, wg *sync.WaitGroup) error {
	defer wg.Done()
	err := WriteFile(data, fmt.Sprintf("%s-%d", name, id), 200)
	if err != nil {
		return err
	}
	fmt.Printf("write 2000M file ok\n")
	return nil
}

func WriteFile(data []byte, name string, loop int) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	for ii := 0; ii < loop; ii++ {
		_, err = f.Write(data)
	}
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
