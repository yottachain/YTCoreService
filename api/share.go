package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type AuthImporter struct {
	UClient    *Client
	Length     int64
	REFS       []*pkt.Refer
	KSS        [][]byte
	VHW        []byte
	Meta       []byte
	bucketName string
	filename   string
}

func (self *AuthImporter) Export(AuthorizedKey string, data []byte) *pkt.ErrorMessage {
	k := self.UClient.GetKey(AuthorizedKey)
	if k == nil {
		emsg := fmt.Sprintf("The public key '%s' of user '%s' does not exist or is not imported", AuthorizedKey, self.UClient.Username)
		logrus.Errorf("[AuthImporter]%s\n", emsg)
		return pkt.NewErrorMsg(pkt.PRIKEY_NOT_EXIST, emsg)
	}
	if data == nil || len(data) < 32+16+32 {
		logrus.Error("[AuthImporter]Auth data err.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Auth data err")
	}
	databuf := bytes.NewBuffer(data)
	size := int32(0)
	binary.Read(databuf, binary.BigEndian, &size)
	KEL := make([]byte, size)
	databuf.Read(KEL)
	KL, err := YTCrypto.ECCDecrypt(KEL, k.PrivateKey)
	if err != nil {
		logrus.Errorf("[AuthExporter]ECCDecrypt err:%s\n", err)
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	binary.Read(databuf, binary.BigEndian, &size)
	head := make([]byte, size)
	_, err = databuf.Read(head)
	if err != nil {
		logrus.Error("[AuthImporter]Read auth data err.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, err.Error())
	}
	dhead := codec.ECBDecrypt(head, KL)
	headbuf := bytes.NewBuffer(dhead)
	self.VHW = make([]byte, 32)
	headbuf.Read(self.VHW)
	self.Length = int64(0)
	binary.Read(headbuf, binary.BigEndian, &self.Length)
	self.Meta = dhead[32+8:]
	var l int64 = 0
	self.REFS = []*pkt.Refer{}
	self.KSS = [][]byte{}
	for {
		bs := make([]byte, 54)
		databuf.Read(bs)
		ref := pkt.NewRefer(bs)
		if ref == nil {
			break
		}
		l = l + ref.OriginalSize
		KS := codec.ECBDecryptNoPad(ref.KEU, KL)
		ref.KEU = codec.ECBEncryptNoPad(KS, k.AESKey)
		ref.KeyNumber = int16(k.KeyNumber)
		self.REFS = append(self.REFS, ref)
		self.KSS = append(self.KSS, KS)
	}
	if self.Length != l {
		logrus.Error("[AuthImporter]Auth data err:Unequal length.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Unequal length")
	}
	return nil
}

func (self *AuthImporter) upload() *pkt.ErrorMessage {
	up, err := NewUploadObjectAuth(self.UClient)
	if err != nil {
		return err
	}
	err = up.UploadAuthFile(self)
	if err != nil {
		return err
	}
	if up.Exist {
		err = self.checkHash()
		if err != nil {
			return err
		}
	}
	return up.writeMeta()
}

func (self *AuthImporter) checkHash() *pkt.ErrorMessage {
	do := &DownloadObject{UClient: self.UClient, Progress: &DownProgress{}}
	do.REFS = self.REFS
	do.Length = self.Length
	do.RSS = self.KSS
	reader := do.Load()
	sha256Digest := sha256.New()
	size, err := codec.CalHash(sha256Digest, reader)
	if err != nil {
		logrus.Errorf("[AuthImporter]CalHash err:%s.\n", err)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	if size != self.Length {
		logrus.Error("[AuthImporter]Auth file err:Unequal length.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Unequal length.")
	}
	hash := sha256Digest.Sum(nil)
	if !bytes.Equal(hash, self.VHW) {
		logrus.Error("[AuthImporter]Auth file err:Unequal hash.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Unequal hash.")
	}
	return nil
}

type AuthExporter struct {
	UClient *Client
	Length  int64
	REFS    []*pkt.Refer
	VHW     []byte
	Meta    []byte
}

func (self *AuthExporter) Export(AuthorizedKey string) ([]byte, *pkt.ErrorMessage) {
	KL := codec.GenerateRandomKey()
	refmap := make(map[int32]*pkt.Refer)
	for _, ref := range self.REFS {
		id := int32(ref.Id) & 0xFFFF
		refmap[id] = ref
	}
	KEL, err := YTCrypto.ECCEncrypt(KL, AuthorizedKey)
	if err != nil {
		logrus.Errorf("[AuthExporter]ECCEncrypt err:%s\n", err)
		return nil, pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	head := bytes.NewBuffer([]byte{})
	head.Write(self.VHW)
	binary.Write(head, binary.BigEndian, self.Length)
	head.Write(self.Meta)
	refs := bytes.NewBuffer([]byte{})
	var referIndex int32 = 0
	for {
		refer := refmap[referIndex]
		if refer == nil {
			break
		}
		k, ok := self.UClient.KeyMap[uint32(refer.KeyNumber)]
		if !ok {
			emsg := fmt.Sprintf("The user did not enter a private key with number%d", refer.KeyNumber)
			logrus.Errorf("[AuthExporter]%s\n", emsg)
			return nil, pkt.NewErrorMsg(pkt.PRIKEY_NOT_EXIST, emsg)
		}
		KS := codec.ECBDecryptNoPad(refer.KEU, k.AESKey)
		refer.KEU = codec.ECBEncryptNoPad(KS, KL)
		refs.Write(refer.Bytes())
		referIndex++
	}
	data := bytes.NewBuffer([]byte{})
	size := int32(len(KEL))
	binary.Write(data, binary.BigEndian, size)
	data.Write(KEL)
	ehead := codec.ECBEncrypt(head.Bytes(), KL)
	size1 := int32(len(ehead))
	binary.Write(data, binary.BigEndian, size1)
	data.Write(ehead)
	data.Write(refs.Bytes())
	return data.Bytes(), nil
}

func (self *AuthExporter) InitByKey(bucketName, filename string, version primitive.ObjectID) *pkt.ErrorMessage {
	req := &pkt.GetFileAuthReq{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.SignKey.Sign,
		KeyNumber:  &self.UClient.SignKey.KeyNumber,
		Bucketname: &bucketName,
		FileName:   &filename,
	}
	key := "/" + bucketName + "/" + filename
	if version != primitive.NilObjectID {
		i1, i2, i3, i4 := pkt.ObjectIdParam(version)
		v := &pkt.GetFileAuthReq_VersionId{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req.Versionid = v
		key = key + "/" + version.Hex()
	}
	return self.init(req, key)
}

func (self *AuthExporter) init(req proto.Message, key string) *pkt.ErrorMessage {
	startTime := time.Now()
	resp, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[AuthExporter][%s]Init ERR:%s\n", key, pkt.ToError(errmsg))
		return errmsg
	}
	dresp, OK := resp.(*pkt.GetFileAuthResp)
	if OK {
		if dresp.Length == nil || dresp.Reflist == nil || dresp.Reflist.Refers == nil || len(dresp.Reflist.Refers) == 0 {
			logrus.Errorf("[AuthExporter][%s]Init ERR:RETURN_NULL\n", key)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
		}
		if dresp.VHW == nil || len(dresp.VHW) != 32 {
			logrus.Errorf("[AuthExporter][%s]Init ERR:RETURN_ERR_VHW\n", key)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "RETURN_ERR_VHW")
		}
		self.Length = int64(*dresp.Length)
		refs := []*pkt.Refer{}
		for _, ref := range dresp.Reflist.Refers {
			r := pkt.NewRefer(ref)
			if r == nil {
				logrus.Errorf("[AuthExporter][%s]Init ERR:RETURN_NULL_REF\n", key)
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
			}
			refs = append(refs, r)
		}
		self.REFS = refs
		self.VHW = dresp.VHW
		self.Meta = dresp.Meta
	} else {
		logrus.Errorf("[AuthExporter][%s]Init ERR:RETURN_ERR_MSG\n", key)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
	logrus.Infof("[AuthExporter][%s]Init OK, length %d,num of blocks %d,take times %d ms.\n", key, self.Length,
		len(self.REFS), time.Now().Sub(startTime).Milliseconds())
	return nil
}
