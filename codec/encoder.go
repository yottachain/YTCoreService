package codec

type Encoder struct {
	in     string
	prefix string
	out    string
	hash   string
}

func NewEncoder(inpath, outpath, root string) *Encoder {
	en := &Encoder{in: inpath, out: outpath, prefix: root}
	return en
}

func (self *Encoder) Handle() error {
	enc, err := NewFileEncoder(self.in)
	if err != nil {
		return err
	}
	defer enc.Close()

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
