package api

import (
	"github.com/yottachain/YTCoreService/pkt"
)

type UploadObjectToDisk struct {
	UploadObject
}

func NewUploadObjectToDisk(c *Client) *UploadObjectToDisk {
	p := &UpProgress{Length: new(int64), ReadinLength: new(int64), ReadOutLength: new(int64), WriteLength: new(int64)}
	o := &UploadObjectToDisk{}
	o.UClient = c
	o.PRO = p
	return o
}

func (self *UploadObjectToDisk) Upload() (reserr *pkt.ErrorMessage) {
	return nil
}
