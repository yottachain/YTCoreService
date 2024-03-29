package api

import (
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type BucketAccessor struct {
	UClient *Client
}

func BytesToBucketMetaMap(meta []byte) (map[string]string, error) {
	if meta == nil {
		return nil, errors.New("no data")
	}
	return pkt.UnmarshalMap(meta)
}

func BucketMetaMapToBytes(m map[string]string) ([]byte, error) {
	return pkt.MarshalMap(m)
}

func (buck *BucketAccessor) CreateBucket(name string, meta []byte) *pkt.ErrorMessage {
	req := &pkt.CreateBucketReqV2{
		UserId:     &buck.UClient.UserId,
		SignData:   &buck.UClient.SignKey.Sign,
		KeyNumber:  &buck.UClient.SignKey.KeyNumber,
		BucketName: &name,
		Meta:       meta,
	}
	_, errmsg := net.RequestSN(req)
	if errmsg != nil {
		logrus.Errorf("[CreateBucket][%d][%s]ERR:%s\n", buck.UClient.UserId, name, pkt.ToError(errmsg))
		return errmsg
	} else {
		logrus.Infof("[CreateBucket][%d][%s]OK.\n", buck.UClient.UserId, name)
		return nil
	}
}

func (buck *BucketAccessor) UpdateBucket(name string, meta []byte) *pkt.ErrorMessage {
	req := &pkt.UpdateBucketReqV2{
		UserId:     &buck.UClient.UserId,
		SignData:   &buck.UClient.SignKey.Sign,
		KeyNumber:  &buck.UClient.SignKey.KeyNumber,
		BucketName: &name,
		Meta:       meta,
	}
	_, errmsg := net.RequestSN(req)
	if errmsg != nil {
		logrus.Errorf("[UpdateBucket][%d][%s]ERR:%s\n", buck.UClient.UserId, name, pkt.ToError(errmsg))
		return errmsg
	} else {
		logrus.Infof("[UpdateBucket][%d][%s]OK.\n", buck.UClient.UserId, name)
		return nil
	}
}

func (buck *BucketAccessor) ListBucket() ([]string, *pkt.ErrorMessage) {
	req := &pkt.ListBucketReqV2{
		UserId:    &buck.UClient.UserId,
		SignData:  &buck.UClient.SignKey.Sign,
		KeyNumber: &buck.UClient.SignKey.KeyNumber,
	}
	resp, errmsg := net.RequestSN(req)
	if errmsg != nil {
		logrus.Errorf("[ListBucket][%d]ERR:%s\n", buck.UClient.UserId, pkt.ToError(errmsg))
		return nil, errmsg
	}
	dresp, OK := resp.(*pkt.ListBucketResp)
	if OK {
		if dresp.Buckets == nil || dresp.Buckets.Names == nil {
			return []string{}, nil
		} else {
			return dresp.Buckets.Names, nil
		}
	} else {
		logrus.Errorf("[ListBucket][%d]RETURN_ERR_MSG\n", buck.UClient.UserId)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
}

func (buck *BucketAccessor) GetBucket(name string) ([]byte, *pkt.ErrorMessage) {
	req := &pkt.GetBucketReqV2{
		UserId:     &buck.UClient.UserId,
		SignData:   &buck.UClient.SignKey.Sign,
		KeyNumber:  &buck.UClient.SignKey.KeyNumber,
		BucketName: &name,
	}
	resp, errmsg := net.RequestSN(req)
	if errmsg != nil {
		logrus.Errorf("[GetBucket][%d][%s]ERR:%s\n", buck.UClient.UserId, name, pkt.ToError(errmsg))
		return nil, errmsg
	}
	dresp, OK := resp.(*pkt.GetBucketResp)
	if OK {
		if dresp.Meta == nil {
			return []byte{}, nil
		} else {
			return dresp.Meta, nil
		}
	} else {
		logrus.Errorf("[GetBucket][%d][%s]RETURN_ERR_MSG\n", buck.UClient.UserId, name)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
}

func (buck *BucketAccessor) DeleteBucket(name string) *pkt.ErrorMessage {
	req := &pkt.DeleteBucketReqV2{
		UserId:     &buck.UClient.UserId,
		SignData:   &buck.UClient.SignKey.Sign,
		KeyNumber:  &buck.UClient.SignKey.KeyNumber,
		BucketName: &name,
	}
	_, errmsg := net.RequestSN(req)
	if errmsg != nil {
		logrus.Errorf("[DeleteBucket][%d][%s]ERR:%s\n", buck.UClient.UserId, name, pkt.ToError(errmsg))
		return errmsg
	} else {
		logrus.Infof("[DeleteBucket][%d][%s]OK.\n", buck.UClient.UserId, name)
		return nil
	}
}
