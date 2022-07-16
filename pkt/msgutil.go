package pkt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	reflect "reflect"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	proto "google.golang.org/protobuf/proto"
)

func MarshalMsgBytes(msg proto.Message) []byte {
	data, _, msgtype, err := MarshalMsg(msg)
	if err != nil {
		return ErrorMsg(SERVER_ERROR, err.Error())
	}
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(msgtype))
	return bytes.Join([][]byte{b[:], data}, []byte(""))
}

func MarshalMsg(msg proto.Message) ([]byte, string, int32, error) {
	reflectVal := reflect.ValueOf(msg)
	name := reflect.Indirect(reflectVal).Type().Name()
	msgType, err := GetMessageID(name)
	if err != nil {
		return nil, name, 0, err
	}
	res, err := proto.Marshal(msg)
	if err != nil {
		logrus.Errorf("[Packet]Marshal ERR:%s\n", err.Error())
		return nil, name, msgType, err
	}
	return res, name, msgType, nil
}

func GetMessageID(name string) (int32, error) {
	if crc16, ok := CLASS_ID_MAP[name]; ok {
		return int32(crc16), nil
	} else {
		logrus.Errorf("[Packet]Message name '%s' no registration.\n", name)
		return 0, errors.New("Message name '" + name + "' no registration.")
	}
}

func GetEmptyMessage(msgType []byte) (proto.Message, error) {
	crc := binary.BigEndian.Uint16(msgType)
	if curfunc, ok := ID_CLASS_MAP[crc]; ok {
		return curfunc(), nil
	} else {
		errmsg := fmt.Sprintf("Message type id'%d' no registration.", crc)
		logrus.Errorf("[Packet]%s\n", errmsg)
		return nil, errors.New(errmsg)
	}
}

func UnmarshalMsg(data []byte) proto.Message {
	if data == nil || len(data) < 2 {
		errmsg := "Unmarshal ERR:nil data"
		logrus.Errorf("[Packet]%s\n", errmsg)
		return NewErrorMsg(BAD_MESSAGE, errmsg)
	}
	msgType := data[0:2]
	msg, err := GetEmptyMessage(msgType)
	if err != nil {
		return NewErrorMsg(BAD_MESSAGE, err.Error())
	}
	bs := data[2:]
	err = proto.Unmarshal(bs, msg)
	if err != nil {
		errmsg := fmt.Sprintf("Unmarshal ERR:%s.", err.Error())
		logrus.Errorf("[Packet]%s\n", errmsg)
		return NewErrorMsg(BAD_MESSAGE, errmsg)
	}
	return msg
}

func NewObjectId(timestamp uint32, machineIdentifier int32, processIdentifier uint32, counter int32) primitive.ObjectID {
	var b [12]byte
	binary.BigEndian.PutUint32(b[:], timestamp)
	b[4] = byte(machineIdentifier >> 16)
	b[5] = byte(machineIdentifier >> 8)
	b[6] = byte(machineIdentifier)
	b[7] = byte(processIdentifier >> 8)
	b[8] = byte(processIdentifier)
	b[9] = byte(counter >> 16)
	b[10] = byte(counter >> 8)
	b[11] = byte(counter)
	return b
}

func ObjectIdParam(b primitive.ObjectID) (*uint32, *int32, *uint32, *int32) {
	i1 := binary.BigEndian.Uint32(b[0:4])
	i2 := int32(uint32(b[4])<<16 | uint32(b[5])<<8 | uint32(b[6]))
	i3 := uint32(b[7])<<8 | uint32(b[8])
	i4 := int32(uint32(b[9])<<16 | uint32(b[10])<<8 | uint32(b[11]))
	return &i1, &i2, &i3, &i4
}

func UnmarshalMap(bs []byte) (map[string]string, error) {
	msg := &StringMap{}
	err := proto.Unmarshal(bs, msg)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	if msg.Keys == nil || msg.Vals == nil {
		return m, nil
	}
	size := len(msg.Vals)
	for index, k := range msg.Keys {
		if index < size {
			v := msg.Vals[index]
			if v != nil && v.Val != nil {
				m[k] = *v.Val
			}
		}
	}
	return m, nil
}

func MarshalMap(m map[string]string) ([]byte, error) {
	keys := []string{}
	vs := []*StringMap_Vals{}
	for k, v := range m {
		keys = append(keys, k)
		nv := &StringMap_Vals{Val: &v}
		vs = append(vs, nv)
	}
	msg := &StringMap{Keys: keys, Vals: vs}
	res, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return res, nil
}
