package core

import "strconv"

func SubscribeCommand(c *Client, s *Server) {
	for j := 1; j < c.Argc; j++ {
		pubsubSubscribeChannel(c, c.Argv[j], s)
	}
	c.Flags |= CLIENT_PUBSUB

}

func pubsubSubscribeChannel(c *Client, obj *GodisObject, s *Server) {
	(*c.PubSubChannels)[obj.Ptr.(string)] = nil
	de := (*(s.PubSubChannels))[obj.Ptr.(string)]
	var clients *List
	if de == nil {
		clients = listCreate()
		(*(s.PubSubChannels))[obj.Ptr.(string)] = clients
	} else {
		clients = de
	}
	clients.listAddNodeTail(c)
}

func PublishCommand(c *Client, s *Server) {
	receivers := pubsubPublishMessage(c.Argv[1], c.Argv[2], s)
	//广播到其他集群上暂不支持
	//aof存储暂不支持
	addReplyStatus(c, strconv.Itoa(receivers))
}

func pubsubPublishMessage(channel *GodisObject, message *GodisObject, s *Server) int {
	receivers := 0
	de := (*s.PubSubChannels)[channel.Ptr.(string)]
	if de != nil {
		for list := de.head; list != nil; list = list.next {
			c := list.value.(*Client)
			addReplyStatus(c, message.Ptr.(string))
			receivers++
		}
	}
	return receivers

}
