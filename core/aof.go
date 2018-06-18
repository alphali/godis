package core

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"syscall"
)

//AppendToFile 写文件
func AppendToFile(fileName string, content string) error {
	// 以只写的模式，打开文件
	f, err := os.OpenFile(fileName, os.O_WRONLY|syscall.O_CREAT, 0644)
	if err != nil {
		log.Println("aof file open failed" + err.Error())
	} else {
		n, _ := f.Seek(0, os.SEEK_END)
		_, err = f.WriteAt([]byte(content), n)
	}
	defer f.Close()
	return err
}

func ReadAof(fileName string) []string {
	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("aof file open failed" + err.Error())
	}
	defer f.Close()
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println("aof file read failed" + err.Error())
	}
	ret := bytes.Split(content, []byte{'*'})
	var pros = make([]string, len(ret)-1)
	for k, v := range ret[1:] {
		v := append(v[:0], append([]byte{'*'}, v[0:]...)...)
		pros[k] = string(v)
	}
	return pros
}
