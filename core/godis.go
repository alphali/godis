package core

import (
	"bytes"
	"errors"
	"fmt"
	"godis/core/proto"
	"log"
	"net"
	"os"
)

//Client 与服务端连接之后即创建一个Client结构
type Client struct {
	Cmd            *GodisCommand
	Argv           []*GodisObject
	Argc           int
	Db             *GodisDb
	QueryBuf       string
	Buf            string
	FakeFlag       bool
	PubSubChannels *map[string]*List
	PubSubPatterns *List
	Flags          int //client flags
}

//flags 模式
const CLIENT_PUBSUB = (1 << 18)

//GodisCommand redis命令结构
type GodisCommand struct {
	Name string
	Proc cmdFunc
}

//命令函数指针
type cmdFunc func(c *Client, s *Server)

// Server 服务端实例结构体
type Server struct {
	Db               []*GodisDb
	DbNum            int
	Start            int64
	Port             int32
	RdbFilename      string
	AofFilename      string
	NextClientID     int32
	SystemMemorySize int32
	Clients          int32
	Pid              int
	Commands         map[string]*GodisCommand
	Dirty            int64
	AofBuf           []string
	PubSubChannels   *map[string]*List
	PubSubPatterns   *List
}

//use map[string]* as type dict
//使用Go原生数据结构map作为redis中dict结构体 暂不对dict造轮子
type dict map[string]*GodisObject

//GodisDb db结构体
type GodisDb struct {
	Dict    dict
	Expires dict
	ID      int32
}

// SetCommand cmd of set
func SetCommand(c *Client, s *Server) {
	objKey := c.Argv[1]
	objValue := c.Argv[2]
	if c.Argc != 3 {
		addReplyError(c, "(error) ERR wrong number of arguments for 'set' command")
	}
	if stringKey, ok1 := objKey.Ptr.(string); ok1 {
		if stringValue, ok2 := objValue.Ptr.(string); ok2 {
			c.Db.Dict[stringKey] = CreateObject(ObjectTypeString, stringValue)
		}
	}
	s.Dirty++
	addReplyStatus(c, "OK")
}

// GetCommand get命令实现
func GetCommand(c *Client, s *Server) {
	o := lookupKey(c.Db, c.Argv[1])
	if o != nil {
		addReplyStatus(c, o.Ptr.(string))
	} else {
		addReplyStatus(c, "nil")
	}
}

// addReply 添加回复
func addReply(c *Client, o *GodisObject) {
	c.Buf = o.Ptr.(string)
}

func addReplyStatus(c *Client, s string) {
	r := proto.NewString([]byte(s))
	addReplyString(c, r)
}
func addReplyError(c *Client, s string) {
	r := proto.NewError([]byte(s))
	addReplyString(c, r)
}
func addReplyString(c *Client, r *proto.Resp) {
	if ret, err := proto.EncodeToBytes(r); err == nil {
		c.Buf = string(ret)
	}
}

// ProcessCommand 执行命令
func (s *Server) ProcessCommand(c *Client) {
	v := c.Argv[0].Ptr
	name, ok := v.(string)
	if !ok {
		log.Println("error cmd")
		os.Exit(1)
	}
	cmd := lookupCommand(name, s)
	fmt.Println(cmd, name, s)
	if cmd != nil {
		c.Cmd = cmd
		call(c, s)
	} else {
		addReplyError(c, fmt.Sprintf("(error) ERR unknown command '%s'", name))
	}
}

// lookupCommand查找命令
func lookupCommand(name string, s *Server) *GodisCommand {
	if cmd, ok := s.Commands[name]; ok {
		return cmd
	}
	return nil
}

// call 真正调用命令
func call(c *Client, s *Server) {
	dirty := s.Dirty
	c.Cmd.Proc(c, s)
	dirty = s.Dirty - dirty
	if dirty > 0 && !c.FakeFlag {
		AppendToFile(s.AofFilename, c.QueryBuf)
	}

}
func lookupKey(db *GodisDb, key *GodisObject) (ret *GodisObject) {
	if o, ok := db.Dict[key.Ptr.(string)]; ok {
		return o
	}
	return nil
}

// CreateClient 连接建立 创建client记录当前连接
func (s *Server) CreateClient() (c *Client) {
	c = new(Client)
	c.Db = s.Db[0]
	c.QueryBuf = ""
	tmp := make(map[string]*List, 0)
	c.PubSubChannels = &tmp
	c.Flags = 0
	return c
}

// ReadQueryFromClient 读取客户端请求信息
func (c *Client) ReadQueryFromClient(conn net.Conn) (err error) {
	buff := make([]byte, 512)
	n, err := conn.Read(buff)

	if err != nil {
		log.Println("conn.Read err!=nil", err, "---len---", n, conn)
		conn.Close()
		return err
	}
	c.QueryBuf = string(buff)
	return nil
}

// ProcessInputBuffer 处理客户端请求信息
func (c *Client) ProcessInputBuffer() error {
	//r := regexp.MustCompile("[^\\s]+")
	decoder := proto.NewDecoder(bytes.NewReader([]byte(c.QueryBuf)))
	//decoder := proto.NewDecoder(bytes.NewReader([]byte("*2\r\n$3\r\nget\r\n")))
	if resp, err := decoder.DecodeMultiBulk(); err == nil {
		c.Argc = len(resp)
		c.Argv = make([]*GodisObject, c.Argc)
		for k, s := range resp {
			c.Argv[k] = CreateObject(ObjectTypeString, string(s.Value))
		}
		return nil
	}
	return errors.New("ProcessInputBuffer failed")
}
