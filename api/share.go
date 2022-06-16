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

func (imp *AuthImporter) Import(data []byte) *pkt.ErrorMessage {
	if data == nil || len(data) < 32+16+32 {
		logrus.Error("[AuthImporter]Auth data err.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Auth data err")
	}
	databuf := bytes.NewBuffer(data)
	pubkeyHash := make([]byte, 32)
	databuf.Read(pubkeyHash)
	k := imp.UClient.GetKey(pubkeyHash)
	if k == nil {
		emsg := fmt.Sprintf("The public key  of user '%s' does not exist or is not imported", imp.UClient.Username)
		logrus.Errorf("[AuthImporter]%s\n", emsg)
		return pkt.NewErrorMsg(pkt.PRIKEY_NOT_EXIST, emsg)
	}
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
	imp.VHW = make([]byte, 32)
	headbuf.Read(imp.VHW)
	imp.Length = int64(0)
	binary.Read(headbuf, binary.BigEndian, &imp.Length)
	imp.Meta = dhead[32+8:]
	var l int64 = 0
	imp.REFS = []*pkt.Refer{}
	imp.KSS = [][]byte{}
	for {
		bs := make([]byte, 54)
		n, _ := databuf.Read(bs)
		if n < 54 {
			break
		}
		ref := pkt.NewRefer(bs)
		l = l + ref.OriginalSize
		KS := codec.ECBDecryptNoPad(ref.KEU, KL)
		ref.KEU = codec.ECBEncryptNoPad(KS, k.AESKey)
		ref.KeyNumber = int16(k.KeyNumber)
		imp.REFS = append(imp.REFS, ref)
		imp.KSS = append(imp.KSS, KS)
	}
	if imp.Length != l {
		logrus.Error("[AuthImporter]Auth data err:Unequal length.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Unequal length")
	}
	return imp.upload()
}

func (imp *AuthImporter) upload() *pkt.ErrorMessage {
	up, err := NewUploadObjectAuth(imp.UClient)
	if err != nil {
		return err
	}
	err = up.UploadAuthFile(imp)
	if err != nil {
		return err
	}
	if up.Exist {
		err = imp.checkHash()
		if err != nil {
			return err
		}
	}
	return up.writeMeta()
}

func (imp *AuthImporter) checkHash() *pkt.ErrorMessage {
	do := &DownloadObject{UClient: imp.UClient, Progress: &DownProgress{}}
	do.REFS = imp.REFS
	do.Length = imp.Length
	do.RSS = imp.KSS
	reader := do.Load()
	sha256Digest := sha256.New()
	size, err := codec.CalHash(sha256Digest, reader)
	if err != nil {
		logrus.Errorf("[AuthImporter]CalHash err:%s.\n", err)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	if size != imp.Length {
		logrus.Error("[AuthImporter]Auth file err:Unequal length.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Unequal length.")
	}
	hash := sha256Digest.Sum(nil)
	if !bytes.Equal(hash, imp.VHW) {
		logrus.Error("[AuthImporter]Auth file err:Unequal hash.\n")
		return pkt.NewErrorMsg(pkt.BAD_FILE, "Unequal hash.")
	}
	return nil
}

type Auth struct {
	AuthExporter
	Bucket string
	Key    string
}

func (auth *Auth) MakeRefs(AuthorizedKey string) error {
	refmap := make(map[int32]*pkt.Refer)
	for _, ref := range auth.REFS {
		id := int32(ref.Id) & 0xFFFF
		refmap[id] = ref
	}
	var referIndex int32 = 0
	for {
		refer := refmap[referIndex]
		if refer == nil {
			break
		}
		k, ok := auth.UClient.KeyMap[uint32(refer.KeyNumber)]
		if !ok {
			emsg := fmt.Sprintf("The user did not enter a private key with number%d", refer.KeyNumber)
			logrus.Errorf("[Auth]%s\n", emsg)
			return pkt.ToError(pkt.NewErrorMsg(pkt.PRIKEY_NOT_EXIST, emsg))
		}
		var KS []byte
		if len(refer.KEU) == 32 {
			KS = codec.ECBDecryptNoPad(refer.KEU, k.AESKey)
		} else {
			KS = codec.ECCDecrypt(refer.KEU, k.PrivateKey)
		}
		refer.KEU = codec.ECCEncrypt(KS, AuthorizedKey)
		referIndex++
	}
	return nil
}

func (auth *Auth) LicensedTo(username, AuthorizedKey string) *pkt.ErrorMessage {
	auth.MakeRefs(AuthorizedKey)
	sn := net.GetRegSuperNode(username)
	refers := [][]byte{}
	for _, ref := range auth.REFS {
		refers = append(refers, ref.Bytes())
	}
	count := uint32(len(refers))
	list := &pkt.AuthReq_RefList{Refers: refers, Count: &count}
	size := uint64(auth.Length)
	req := &pkt.AuthReq{UserId: &auth.UClient.UserId,
		SignData:   &auth.UClient.SignKey.Sign,
		KeyNumber:  &auth.UClient.SignKey.KeyNumber,
		Bucketname: &auth.Bucket,
		FileName:   &auth.Key,
		Username:   &username,
		Pubkey:     &AuthorizedKey,
		Length:     &size,
		VHW:        auth.VHW,
		Meta:       auth.Meta,
		Reflist:    list,
	}
	resp, errmsg := net.RequestSN(req, sn, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[Auth][%s]LicensedTo %s ERR:%s\n", auth.UClient.Username, username, pkt.ToError(errmsg))
		return errmsg
	}
	_, OK := resp.(*pkt.VoidResp)
	if OK {
		logrus.Errorf("[Auth][%s]LicensedTo %s/%s/%s OK\n", auth.UClient.Username, username, auth.Bucket, auth.Key)
		return nil
	} else {
		logrus.Errorf("[Auth][%s]LicensedTo ERR:RETURN_ERR_MSG\n", auth.UClient.Username)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
}

type AuthExporter struct {
	UClient *Client
	Length  int64
	REFS    []*pkt.Refer
	VHW     []byte
	Meta    []byte
}

func (auth *AuthExporter) Export(AuthorizedKey string) ([]byte, *pkt.ErrorMessage) {
	KL := codec.GenerateRandomKey()
	refmap := make(map[int32]*pkt.Refer)
	for _, ref := range auth.REFS {
		id := int32(ref.Id) & 0xFFFF
		refmap[id] = ref
	}
	KEL, err := YTCrypto.ECCEncrypt(KL, AuthorizedKey)
	if err != nil {
		logrus.Errorf("[AuthExporter]ECCEncrypt err:%s\n", err)
		return nil, pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	head := bytes.NewBuffer([]byte{})
	head.Write(auth.VHW)
	binary.Write(head, binary.BigEndian, auth.Length)
	head.Write(auth.Meta)
	refs := bytes.NewBuffer([]byte{})
	var referIndex int32 = 0
	for {
		refer := refmap[referIndex]
		if refer == nil {
			break
		}
		k, ok := auth.UClient.KeyMap[uint32(refer.KeyNumber)]
		if !ok {
			emsg := fmt.Sprintf("The user did not enter a private key with number%d", refer.KeyNumber)
			logrus.Errorf("[AuthExporter]%s\n", emsg)
			return nil, pkt.NewErrorMsg(pkt.PRIKEY_NOT_EXIST, emsg)
		}
		var KS []byte
		if len(refer.KEU) == 32 {
			KS = codec.ECBDecryptNoPad(refer.KEU, k.AESKey)
		} else {
			KS = codec.ECCDecrypt(refer.KEU, k.PrivateKey)
		}
		refer.KEU = codec.ECBEncryptNoPad(KS, KL)
		refs.Write(refer.Bytes())
		referIndex++
	}
	data := bytes.NewBuffer([]byte{})
	bs := sha256.Sum256([]byte(AuthorizedKey))
	data.Write(bs[:])
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

func (auth *AuthExporter) InitByKey(bucketName, filename string, version primitive.ObjectID) *pkt.ErrorMessage {
	req := &pkt.GetFileAuthReq{
		UserId:     &auth.UClient.UserId,
		SignData:   &auth.UClient.SignKey.Sign,
		KeyNumber:  &auth.UClient.SignKey.KeyNumber,
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
	return auth.init(req, key)
}

func (auth *AuthExporter) init(req proto.Message, key string) *pkt.ErrorMessage {
	startTime := time.Now()
	resp, errmsg := net.RequestSN(req, auth.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
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
		auth.Length = int64(*dresp.Length)
		refs := []*pkt.Refer{}
		for _, ref := range dresp.Reflist.Refers {
			r := pkt.NewRefer(ref)
			if r == nil {
				logrus.Errorf("[AuthExporter][%s]Init ERR:RETURN_NULL_REF\n", key)
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
			}
			refs = append(refs, r)
		}
		auth.REFS = refs
		auth.VHW = dresp.VHW
		auth.Meta = dresp.Meta
	} else {
		logrus.Errorf("[AuthExporter][%s]Init ERR:RETURN_ERR_MSG\n", key)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
	logrus.Infof("[AuthExporter][%s]Init OK, length %d,num of blocks %d,take times %d ms.\n", key, auth.Length,
		len(auth.REFS), time.Since(startTime).Milliseconds())
	return nil
}
