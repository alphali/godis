package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	/*---- 命令行参数处理 ----*/
	argv := os.Args
	argc := len(os.Args)
	if argc >= 2 {
		//j := 1 /* First option to parse in Args[] */
		/* Handle special options --help and --version */
		if argv[1] == "-v" || argv[1] == "--version" {
			version()
		}
		if argv[1] == "--help" || argv[1] == "-h" {
			usage()
		}
		if argv[1] == "--test-memory" {
			if argc == 3 {
				os.Exit(0)
			} else {
				println("Please specify the amount of memory to test in megabytes.\n")
				println("Example: ./godis-server --test-memory 4096\n\n")
				os.Exit(1)
			}
		}
	}

	/*---- 监听信号 平滑退出 ----*/
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	go sigHandler(c)

	/*---- 网络处理 ----*/
	netListen, err := net.Listen("tcp", "127.0.0.1:9736")
	if err != nil {
		log.Print("listen err ")
	}
	//checkError(err)
	defer netListen.Close()

	for {
		conn, err := netListen.Accept()
		if err != nil {
			continue
		}
		go handle(conn)
	}
}

// 处理请求
func handle(conn net.Conn) {
	for {
		buff, err := readQueryFromClient(conn)
		if err != nil {
			log.Println("readQueryFromClient err")
			return
		}
		result := processInputBuffer(buff)
		writeToClient(conn, result)
	}
}

// 读取客户端请求信息
func readQueryFromClient(conn net.Conn) (buf string, err error) {
	buff := make([]byte, 512)
	n, err := conn.Read(buff)
	if err != nil {
		log.Println("conn.Read err!=nil", err, "---len---", n, conn)
		conn.Close()
		return "", err
	}
	buf = string(buff)
	return buf, nil
}

// 处理客户端请求信息
func processInputBuffer(buff string) string {
	return buff + "from Mars"
}

// 响应返回给客户端
func writeToClient(conn net.Conn, buff string) {
	conn.Write([]byte(buff))
}

func sigHandler(c chan os.Signal) {
	for s := range c {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			exitHandler()
		default:
			fmt.Println("signal ", s)
		}
	}
}

func exitHandler() {
	fmt.Println("exiting smoothly ...")
	fmt.Println("bye ")
	os.Exit(0)
}

func version() {
	println("Godis server v=0.0.1 sha=xxxxxxx:001 malloc=libc-go bits=64 ")
	os.Exit(0)
}

func usage() {
	println("Usage: ./godis-server [/path/to/redis.conf] [options]")
	println("       ./godis-server - (read config from stdin)")
	println("       ./godis-server -v or --version")
	println("       ./godis-server -h or --help")
	println("Examples:")
	println("       ./godis-server (run the server with default conf)")
	println("       ./godis-server /etc/redis/6379.conf")
	println("       ./godis-server --port 7777")
	println("       ./godis-server --port 7777 --slaveof 127.0.0.1 8888")
	println("       ./godis-server /etc/myredis.conf --loglevel verbose")
	println("Sentinel mode:")
	println("       ./godis-server /etc/sentinel.conf --sentinel")
	os.Exit(0)
}
