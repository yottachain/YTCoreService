package backup

import (
	"os"

	"github.com/mr-tron/base58"
	"github.com/yottachain/YTCoreService/codec"
)

type Encoder struct {
	in     string
	prefix string
	out    string
	fc     *codec.FileEncoder
}

func NewEncoder(inpath, outpath, root string) *Encoder {
	en := &Encoder{in: inpath, out: outpath, prefix: root}
	return en
}

func (self *Encoder) GetSHA256() []byte {
	if self.fc != nil {
		return self.fc.GetVHW()
	}
	return nil
}

func (self *Encoder) GetBaseSHA256() string {
	if self.fc != nil {
		return base58.Encode(self.fc.GetVHW())
	}
	return ""
}

func (self *Encoder) GetMD5() []byte {
	if self.fc != nil {
		return self.fc.GetMD5()
	}
	return nil
}

func (self *Encoder) GetBaseMD5() string {
	if self.fc != nil {
		return base58.Encode(self.fc.GetMD5())
	}
	return ""
}

func (self *Encoder) Handle() error {
	enc, err := codec.NewFileEncoder(self.in)
	if err != nil {
		return err
	}
	defer enc.Close()
	self.fc = enc

	_, err = os.OpenFile(self.out, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	err = self.writeHead()

	for {
		_, err := enc.ReadNext()
		if err != nil {
			break
		}
	}

	return nil
}

func (self *Encoder) writeHead() error {
	return nil
}
