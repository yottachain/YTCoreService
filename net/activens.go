package net

import (
	"io/ioutil"
	"net/http"

	"github.com/yottachain/YTCoreService/codec"
)

func GetActiveNodes(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	json := codec.ECBDecrypt(bs, codec.FixKey)
	return string(json), nil
}
