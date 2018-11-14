package core

import (
	"math/rand"
)

/* Input flags. */
const ZADD_NONE = 0
const ZADD_INCR = (1 << 0) /* Increment the score instead of setting it. */
const ZADD_NX = (1 << 1)   /* Don't touch elements not already existing. */
const ZADD_XX = (1 << 2)

/* Output flags. */
const ZADD_NOP = (1 << 3)     /* Operation not performed because of conditionals.*/
const ZADD_NAN = (1 << 4)     /* Only touch elements already exisitng. */
const ZADD_ADDED = (1 << 5)   /* The element was new and was added. */
const ZADD_UPDATED = (1 << 6) /* The element already existed, score updated. */

const ZSKIPLIST_MAXLEVEL = 32
const ZSKIPLIST_P = 0.25 /* Skiplist P = 1/4 */

type zSet struct {
	dict *dict
	zsl  *zSkipList
}

type zSkipList struct {
	header *zSkipListNode
	tail   *zSkipListNode
	length uint //节点层的数量
	level  int  //表示目前跳跃表内，层数最大的节点的层数
}

type zSkipListNode struct { //一层的节点
	ele      string
	score    float64        //
	backward *zSkipListNode // 后退指针
	level    []zSkipListLevel
}

type zSkipListLevel struct {
	forward *zSkipListNode
	span    uint
}

// ex结尾指示开区间还是闭区间 值为 1 表示开，值为 0 表示闭
type zRangeSpec struct {
	min   float64
	max   float64
	minEx int
	maxEx int
}

func zaddCommand(c *Client) {
	zaddGenericCommand(c, ZADD_NONE)
}

func zincrbyCommand(c *Client) {
	zaddGenericCommand(c, ZADD_INCR)
}

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
func zaddGenericCommand(c *Client, flags int) {
	key := c.Argv[1]
	scoreIdx := 2
	elements := c.Argc - scoreIdx
	elements /= 2
	scores := make([]float64, elements)

	for j := 0; j < elements; j++ {
		if value, ok := c.Argv[2+j*2].Ptr.(uint64); ok {
			scores[j] = float64(value)
		}
	}

	//这里首先在client对应的db中查找该key，即有序集
	zobj := lookupKey(c.Db, key)
	if zobj == nil {
		//hash+skiplist组合方式,后续再进行判断实现ziplist
		zobj = createZsetObject()
		//添加到c.db中
		c.Db.Dict[key.Ptr.(string)] = zobj
	}

	for j := 0; j < elements; j++ {
		var newScore float64
		score := scores[j]
		retFlags := flags
		if ele, ok := c.Argv[scoreIdx+1+j*2].Ptr.(string); ok {
			zSetAdd(zobj, score, ele, &retFlags, &newScore)
		}

	}

}

// create zset
func createZsetObject() *GodisObject {
	val := new(zSet)
	val.dict = new(dict)
	dict := make(map[string]*GodisObject)
	*val.dict = dict

	val.zsl = zslCreate() //这里创建节点
	o := CreateObject(OBJ_ZSET, val)
	return o
}

/* Create a new skiplist. */
func zslCreate() *zSkipList {
	zsl := new(zSkipList)
	zsl.level = 1
	zsl.length = 0
	zsl.header = zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, "")
	for j := 0; j < ZSKIPLIST_MAXLEVEL; j++ {
		zsl.header.level[j].forward = nil
		zsl.header.level[j].span = 0
	}
	zsl.header.backward = nil
	zsl.tail = nil
	return zsl
}

// 参数依次是有序集，要添加的元素的score，要添加的元素，操作模式，新的score
func zSetAdd(zObj *GodisObject, score float64, ele string, flags *int, newScore *float64) bool {
	incr := (*flags & ZADD_INCR) != 0
	nx := (*flags & ZADD_NX) != 0
	xx := (*flags & ZADD_XX) != 0
	*flags = 0
	var curscore float64
	//暂只支持skiplist
	if zObj.ObjectType == OBJ_ZSET {
		//进行hash查找
		zs := zObj.Ptr.(*zSet) //使用*zSet好，还是zSet好

		dict := zs.dict
		de := dictFind(dict, ele)
		if de != nil {
			if nx {

			}
			if incr {

			}
			//获取存储的score
			if coreTemp, ok := de.Ptr.(float64); ok {
				curscore = coreTemp
			} else {
				//exit
			}

			//remove and in-insert when score changes
			if curscore != score {

			}

		} else if !xx {
			//insert
			zslInsert(zs.zsl, score, ele)
			//插入dict
			(*(zs.dict))[ele] = CreateObject(ObjectTypeString, score)
			*flags |= ZADD_ADDED
			return true
		}
	} else {
		//exit;
	}
	return false /* Never reached. */
}

func dictFind(d *dict, key string) *GodisObject {
	if (*d)[key] != nil {
		return (*d)[key]
	}
	return nil
}

/*
 * 创建一个成员为 obj ，分值为 score 的新节点，
 * 并将这个新节点插入到跳跃表 zsl 中。
 *
 * 函数的返回值为新节点。
 *
 * T_wrost = O(N^2), T_avg = O(N log N)
 */
/*src/t_zset.c/zslInsert*/
func zslInsert(zsl *zSkipList, score float64, ele string) *zSkipListNode {
	update := make([]*zSkipListNode, ZSKIPLIST_MAXLEVEL)
	rank := make([]uint, ZSKIPLIST_MAXLEVEL)
	x := zsl.header

	for i := zsl.level - 1; i >= 0; i-- {
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.level[i].forward != nil && (x.level[i].forward.score < score ||
			(x.level[i].forward.score == score && (x.level[i].forward.ele < ele))) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := zslRandomLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = zsl.length
		}
		zsl.level = level
	}

	x = zslCreateNode(level, score, ele)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == zsl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}
	zsl.length++
	return x
}

// 获取一个随机值作为新节点的层数
func zslRandomLevel() int {
	level := 1
	for rand.Float64()*65535 < ZSKIPLIST_P*65535 {
		level++
	}

	if level < ZSKIPLIST_MAXLEVEL {
		return level
	}
	return ZSKIPLIST_MAXLEVEL
}

// 创建新节点zSkipListNode
func zslCreateNode(level int, score float64, ele string) *zSkipListNode {
	zn := new(zSkipListNode)
	zl := make([]zSkipListLevel, level)
	zn.level = zl
	zn.score = score
	zn.ele = ele
	return zn
}

func zslFirstInRange(zsl *zSkipList, zRange *zRangeSpec) *zSkipListNode {
	if !zslIsInRange(zsl, zRange) {
		return nil
	}
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && !zslValueGteMin(x.level[i].forward.score, zRange) {
			x = x.level[i].forward
		}
	}

	x = x.level[0].forward
	if x == nil {
		return nil
	}

	if !zslValueLteMax(x.score, zRange) {
		return nil
	}
	return x
}

func zslValueGteMin(value float64, spec *zRangeSpec) bool {
	if spec.minEx != 0 {
		return value > spec.min
	}
	return value >= spec.min
}

func zslValueLteMax(value float64, spec *zRangeSpec) bool {
	if spec.maxEx != 0 {
		return value < spec.max
	}
	return value <= spec.max
}

func zslIsInRange(zsl *zSkipList, zRange *zRangeSpec) bool {
	//test invalid param
	if zRange.min > zRange.max ||
		(zRange.min == zRange.max && (zRange.minEx != 0 || zRange.maxEx != 0)) {
		return false
	}
	x := zsl.tail
	if x == nil || !zslValueGteMin(x.score, zRange) {
		return false
	}
	x = zsl.header.level[0].forward
	if x == nil || !zslValueLteMax(x.score, zRange) {
		return false
	}
	return true
}

//从skiplist删除一个节点
func zslDelete(zsl *zSkipList, score float64, ele string, node **zSkipListNode) bool {
	update := make([]*zSkipListNode, ZSKIPLIST_MAXLEVEL)
	rank := make([]uint, ZSKIPLIST_MAXLEVEL)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.level[i].forward != nil && (x.level[i].forward.score < score ||
			(x.level[i].forward.score == score && (x.level[i].forward.ele < ele))) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && score == x.score && ele == x.ele {
		zslDeleteNode(zsl, x, update)
		return true
	}
	return false
}

func zslDeleteNode(zsl *zSkipList, x *zSkipListNode, update []*zSkipListNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}

	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

func zsetScore(zobj *GodisObject, member string, score *float64) int {
	if zobj == nil || member == "" {
		return C_ERR
	}
	// only search skiplist
	if zobj.ObjectType == OBJ_ZSET {
		zs := zobj.Ptr.(*zSet)
		dict := zs.dict
		de := dictFind(dict, member)

		if de == nil {
			return C_ERR
		}
		value := de.Ptr.(float64)
		*score = value
	} else {
		panic("Unknown sorted set encoding")
	}
	return C_OK
}
