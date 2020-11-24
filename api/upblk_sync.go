package api

import (
	"fmt"
	"sync"

	"github.com/yottachain/YTCoreService/codec"
)

type UploadBlockSync struct {
	UploadBlock
	EncBLK *codec.EncodedBlock
}

func StartUploadBlockSync(id int16, b *codec.EncodedBlock, up *UploadObject, wg *sync.WaitGroup) {
	AddSyncBlockMen(b)
	ub := UploadBlock{
		UPOBJ: up,
		ID:    id,
		WG:    wg,
	}
	syncup := &UploadBlockSync{}
	syncup.EncBLK = b
	syncup.UploadBlock = ub
	ub.logPrefix = fmt.Sprintf("[%s][%d]", ub.UPOBJ.VNU.Hex(), ub.ID)
	<-BLOCK_ROUTINE_CH
	go syncup.upload()
}

func (self *UploadBlockSync) upload() {
}
