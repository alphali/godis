package core

type listNode struct {
	prev  *listNode
	next  *listNode
	value interface{}
}

type List struct {
	head *listNode
	tail *listNode
	len  int
}

func (l List) listLength() int {
	return l.len
}

func (l List) listFirst() *listNode {
	return l.head
}

func (l List) listLast() *listNode {
	return l.tail
}

func (n listNode) listPrevNode() *listNode {
	return n.prev
}

func (n listNode) listNextNode() *listNode {
	return n.next
}

func (n listNode) listNodeValue() interface{} {
	return n.value
}

func listCreate() *List {
	list := new(List)
	list.head = nil
	list.tail = nil
	list.len = 0
	return list
}

func (l *List) listAddNodeHead(value interface{}) *List {
	node := new(listNode)

	node.value = value
	if l.len == 0 {
		l.head = node
		l.tail = node
		node.prev = nil
		node.next = nil
	} else {
		node.prev = nil
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
	l.len++
	return l
}

func (l *List) listAddNodeTail(value interface{}) *List {
	node := new(listNode)

	node.value = value
	if l.len == 0 {
		l.head = node
		l.tail = node
		node.prev = nil
		node.next = nil
	} else {
		node.prev = l.tail
		node.next = nil
		l.tail.next = node
		l.tail = node
	}
	l.len++
	return l
}

func (l *List) listInsertNode(oldNode *listNode, value interface{}, after int) *List {
	node := new(listNode)
	node.value = value
	if after > 0 {

	}
	l.len++
	return l
}
