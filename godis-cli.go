package main

import (
	"bufio"
	"fmt"
	"godis/core/proto"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	IPPort := "127.0.0.1:9736"

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Hi Godis")
	tcpAddr, err := net.ResolveTCPAddr("tcp4", IPPort)
	checkError(err)

	//建立连接 如果第二个参数(本地地址)为nil，会自动生成一个本地地址
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)
	defer conn.Close()
	//log.Println(tcpAddr, conn.LocalAddr(), conn.RemoteAddr())

	for {
		fmt.Print(IPPort + "> ")
		text, _ := reader.ReadString('\n')
		//清除掉回车换行符
		text = strings.Replace(text, "\n", "", -1)
		send2Server(text, conn)

		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		resp, er := proto.DecodeFromBytes(buff)
		checkError(err)
		if n == 0 {
			fmt.Println(IPPort+"> ", "nil")
		} else if er == nil {
			fmt.Println(IPPort+">", string(resp.Value))
		} else {
			fmt.Println(IPPort+"> ", "err server response")
		}
	}

}
func send2Server(msg string, conn net.Conn) (n int, err error) {
	p, e := proto.EncodeCmd(msg)
	if e != nil {
		return 0, e
	}
	//fmt.Println("proto encode", p, string(p))
	n, err = conn.Write(p)
	return n, err
}
func checkError(err error) {
	if err != nil {
		log.Println("err ", err.Error())
		os.Exit(1)
	}
}
