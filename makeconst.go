package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"
	"regexp"
	"strings"

	"github.com/gobuffalo/packr/v2"
	"github.com/yottachain/YTCoreService/codec"
)

var ID_CLASS_MAP_CODE bytes.Buffer
var CLASS_ID_MAP_CODE bytes.Buffer

var CLASS_MAP = make(map[string]string)

func mainMake() {
	Make()
}

func Make() {
	CreateMsgCode()
	ListHandler()
}

func CreateMsgCode() {
	var content bytes.Buffer
	content.WriteString("package pkt\n\n")

	content.WriteString("import(\n")
	content.WriteString("	\"log\"\n\n")
	content.WriteString("	proto \"github.com/golang/protobuf/proto\"\n")
	content.WriteString("	\"github.com/yottachain/YTCoreService/env\"\n")
	content.WriteString(")\n\n")

	content.WriteString("type MessageInitor func() proto.Message\n\n")
	content.WriteString("var ID_CLASS_MAP = make(map[uint16]MessageInitor)\n")
	content.WriteString("var CLASS_ID_MAP = make(map[string]uint16)\n\n")

	content.WriteString("func init() {\n")
	content.WriteString("	log.SetOutput(&env.LogWrite{})\n")
	content.WriteString("	init_id_class()\n")
	content.WriteString("	init_class_id()\n")
	content.WriteString("}\n\n")

	ReadProto("msg.proto")
	ReadProto("msg.user.proto")
	ReadProto("msg.bp.proto")
	ReadProto("msg.node.proto")
	ReadProto("msg.s3.proto")
	ReadProto("msg.s3.v2.proto")
	ReadProto("msg.v2.proto")

	content.WriteString("func init_id_class() {\n")
	content.Write(ID_CLASS_MAP_CODE.Bytes())
	content.WriteString("}\n\n")

	content.WriteString("func init_class_id() {\n")
	content.Write(CLASS_ID_MAP_CODE.Bytes())
	content.WriteString("}")
	filename := "pkt/const.go"
	ioutil.WriteFile(filename, []byte(content.String()), 0777)
}

func ReadProto(protofile string) {
	box := packr.New(protofile, "res")
	txt, err := box.FindString(protofile)
	if err != nil {
		panic("Resource file '" + protofile + "' read failure")
	}
	list := strings.Split(txt, "\n")
	pkgName := strings.ReplaceAll(protofile, ".proto", "")
	for _, value := range list {
		value = strings.ReplaceAll(value, "{", "")
		value = strings.Trim(strings.Trim(value, " "), "\n")
		if strings.HasPrefix(value, "message") {
			name := value[8 : len(value)-1]
			name = strings.Trim(name, " ")
			parseType(pkgName, name)
			continue
		}
	}
}

func parseType(pkt string, n string) {
	if pkt == "" {
		return
	}
	pkt = strings.ReplaceAll(pkt, "msg", "com.ytfs.service.packet")
	name := pkt + "." + n
	crc16 := "0x" + codec.CheckSumString([]byte(name))
	if _, ok := CLASS_MAP[crc16]; ok {
		panic("Message name '" + name + "' already exist")
	}
	CLASS_MAP[crc16] = n
	ID_CLASS_MAP_CODE.WriteString(fmt.Sprintf(`	ID_CLASS_MAP[%s]=func() proto.Message { return &%s{} }`, crc16, n) + "\n")
	CLASS_ID_MAP_CODE.WriteString(fmt.Sprintf(`	CLASS_ID_MAP["%s"]=%s`, n, crc16) + "\n")
	fmt.Printf("MessageID:%s--->Name:%s\n", crc16, name)
}

var ID_HANDLER_MAP_CODE bytes.Buffer
var HANDLER_MAP = make(map[string]string)

func ListHandler() {
	ID_HANDLER_MAP_CODE.WriteString("package handle\n\n")
	ID_HANDLER_MAP_CODE.WriteString("type HandlerInitor func() MessageEvent\n\n")
	ID_HANDLER_MAP_CODE.WriteString("var ID_HANDLER_MAP = make(map[uint16]HandlerInitor)\n\n")
	ID_HANDLER_MAP_CODE.WriteString("func init() {\n")

	files, _ := ioutil.ReadDir("handle/")
	for _, f := range files {
		if !f.IsDir() {
			name := f.Name()
			ext := path.Ext(name)
			if ext == ".go" && name != "sync.go" {
				ReadHandler("handle/" + name)
				ID_HANDLER_MAP_CODE.WriteString("\n")
			}
		}
	}
	ID_HANDLER_MAP_CODE.WriteString("}")
	filename := "handle/const.go"
	ioutil.WriteFile(filename, []byte(ID_HANDLER_MAP_CODE.String()), 0777)
}

func Match(contect string) {
	pos := strings.Index(contect, "{")
	if pos < 0 {
		return
	}
	hName := contect[0 : pos-1]
	hName = strings.ReplaceAll(hName, "type", "")
	hName = strings.ReplaceAll(hName, "struct", "")
	hName = strings.ReplaceAll(hName, " ", "")
	newcontent := strings.ReplaceAll(contect, "\r", "_")
	newcontent = strings.ReplaceAll(newcontent, "\n", "_")
	for k, v := range CLASS_MAP {
		name := "*pkt." + v + "_"
		if strings.Contains(newcontent, name) {
			if oldHander, ok := HANDLER_MAP[k]; ok {
				panic("MessageID '" + k + "' had two Handler:" + oldHander + "," + hName)
			}
			ss := fmt.Sprintf(`	ID_HANDLER_MAP[%s] = func() MessageEvent { return MessageEvent(&%s{}) }`, k, hName) + "\n"
			ID_HANDLER_MAP_CODE.WriteString(ss)
			HANDLER_MAP[k] = hName
			fmt.Printf("MessageID:%s--->HandlerName:%s\n", k, hName)
			break
		}
	}
}

func ReadHandler(handlefile string) {
	txt, err := ioutil.ReadFile(handlefile)
	if err != nil {
		panic("Read file '" + handlefile + "' err:" + err.Error())
	}
	content := string(txt)
	r, _ := regexp.Compile("type([^\\{]+)struct([^\\}]+)\\}")
	var ss string
	for {
		ss = r.FindString(content)
		if ss == "" {
			break
		}
		Match(ss)
		content = strings.ReplaceAll(content, ss, "")
	}
}
