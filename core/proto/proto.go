package proto

import (
	"bytes"
	"godis/util/bufio2"
	"io"
	"log"
	"strconv"

	"errors"
)

var (
	ErrBadArrayLen        = errors.New("bad array len")
	ErrBadArrayLenTooLong = errors.New("bad array len, too long")

	ErrBadBulkBytesLen        = errors.New("bad bulk bytes len")
	ErrBadBulkBytesLenTooLong = errors.New("bad bulk bytes len, too long")

	ErrBadMultiBulkLen     = errors.New("bad multi-bulk len")
	ErrBadMultiBulkContent = errors.New("bad multi-bulk content, should be bulkbytes")
)

const (
	// MaxBulkBytesLen 最大长度
	MaxBulkBytesLen = 1024 * 1024 * 512
	// MaxArrayLen 最大长度
	MaxArrayLen = 1024 * 1024
)

type RespType byte

const (
	TypeString    = '+'
	TypeError     = '-'
	TypeInt       = ':'
	TypeBulkBytes = '$'
	TypeArray     = '*'
)

// Btoi64 byte to int64
func Btoi64(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 10 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}

	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0, errorsTrace(err)
	} else {
		return n, nil
	}
}

/*---- Encoder ----*/

type Encoder struct {
	bw *bufio2.Writer

	Err error
}

// NewEncoder
func NewEncoder(w io.Writer) *Encoder {
	return NewEncoderBuffer(bufio2.NewWriterSize(w, 8192))
}

// NewEncoderSize new encoder by size
func NewEncoderSize(w io.Writer, size int) *Encoder {
	return NewEncoderBuffer(bufio2.NewWriterSize(w, size))
}

// NewEncoderBuffer new encoder by bufWriter
func NewEncoderBuffer(bw *bufio2.Writer) *Encoder {
	return &Encoder{bw: bw}
}

// Encode 转换为协议
func (e *Encoder) Encode(r *Resp, flush bool) error {
	if e.Err != nil {
		return errorsTrace(e.Err)
	}
	if err := e.encodeResp(r); err != nil {
		e.Err = err
	} else if flush {
		e.Err = errorsTrace(e.bw.Flush())
	}
	return e.Err
}

// EncodeCmd 命令行编码协议
func EncodeCmd(cmd string) ([]byte, error) {
	return EncodeBytes([]byte(cmd))
}

// EncodeBytes Bytes编码协议
func EncodeBytes(b []byte) ([]byte, error) {
	r := bytes.Split(b, []byte(" "))
	if r == nil {
		return nil, errorsTrace(errorNew("empty split"))
	}
	resp := NewArray(nil)
	for _, v := range r {
		if len(v) > 0 {
			resp.Array = append(resp.Array, NewBulkBytes(v))
		}
	}
	return EncodeToBytes(resp)
}

// EncodeMultiBulk encode 多条批量回复
func (e *Encoder) EncodeMultiBulk(multi []*Resp, flush bool) error {
	if e.Err != nil {
		return errorsTrace(e.Err)
	}
	if err := e.encodeMultiBulk(multi); err != nil {
		e.Err = err
	} else if flush {
		e.Err = errorsTrace(e.Err)
	}
	return e.Err
}

// Flush buf to writer
func (e *Encoder) Flush() error {
	if e.Err != nil {
		return errorsTrace(errorNew("Flush error"))
	}
	if err := e.bw.Flush(); err != nil {
		e.Err = errorsTrace(errorNew("bw.Flush error"))
	}
	return e.Err
}

// Encode 转换为协议接口
func Encode(w io.Writer, r *Resp) error {
	return NewEncoder(w).Encode(r, true)
}

// EncodeToBytes Resp编码协议
func EncodeToBytes(r *Resp) ([]byte, error) {
	var b = &bytes.Buffer{}
	if err := Encode(b, r); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// encodeResp 编码
func (e *Encoder) encodeResp(r *Resp) error {
	if err := e.bw.WriteByte(byte(r.Type)); err != nil {
		return errorsTrace(err)
	}
	switch r.Type {
	case TypeString, TypeError, TypeInt:
		return e.encodeTextBytes(r.Value)
	case TypeBulkBytes:
		return e.encodeBulkBytes(r.Value)
	case TypeArray:
		return e.encodeArray(r.Array)
	default:
		return errorsTrace(e.Err)
	}
}

// encodeMultiBulk encode 多条批量回复
func (e *Encoder) encodeMultiBulk(multi []*Resp) error {
	if err := e.bw.WriteByte(byte(TypeArray)); err != nil {
		return errorsTrace(err)
	}
	return e.encodeArray(multi)
}

// encodeTextBytes encode text type
func (e *Encoder) encodeTextBytes(b []byte) error {
	if _, err := e.bw.Write(b); err != nil {
		return errorsTrace(err)
	}
	if _, err := e.bw.WriteString("\r\n"); err != nil {
		return errorsTrace(err)
	}
	return nil
}

// encode text type
func (e *Encoder) encodeTextString(s string) error {
	if _, err := e.bw.WriteString(s); err != nil {
		return errorsTrace(err)
	}
	if _, err := e.bw.WriteString("\r\n"); err != nil {
		return errorsTrace(err)
	}
	return nil
}

// encodeInt encode整数
func (e *Encoder) encodeInt(v int64) error {
	return e.encodeTextString(strconv.FormatInt(v, 10))
}

// encodeBulkBytes 批量回复
func (e *Encoder) encodeBulkBytes(b []byte) error {
	if b == nil {
		return e.encodeInt(-1)
	} else {
		if err := e.encodeInt(int64(len(b))); err != nil {
			return err
		}
		return e.encodeTextBytes(b)
	}
}

// encodeArray encode 多条批量回复
func (e *Encoder) encodeArray(array []*Resp) error {
	if array == nil {
		return e.encodeInt(-1)
	} else {
		if err := e.encodeInt(int64(len(array))); err != nil {
			return err
		}
		for _, r := range array {
			if err := e.encodeResp(r); err != nil {
				return err
			}
		}
		return nil
	}
}

/*---- Decoder ----*/
type Decoder struct {
	br *bufio2.Reader

	Err error
}

// NewDecoder
func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderBuffer(bufio2.NewReaderSize(r, 8192))
}

// NewDecoderSize by size
func NewDecoderSize(r io.Reader, size int) *Decoder {
	return NewDecoderBuffer(bufio2.NewReaderSize(r, size))
}

// NewDecoderBuffer by bufReader
func NewDecoderBuffer(br *bufio2.Reader) *Decoder {
	return &Decoder{br: br}
}

// Decode
func (d *Decoder) Decode() (*Resp, error) {
	if d.Err != nil {
		return nil, errorsTrace(errorNew("Decode err"))
	}
	r, err := d.decodeResp()
	if err != nil {
		d.Err = err
	}
	return r, d.Err
}

// DecodeMultiBulk decode批量回复
func (d *Decoder) DecodeMultiBulk() ([]*Resp, error) {
	if d.Err != nil {
		return nil, errorsTrace(errorNew("DecodeMultibulk error"))
	}
	m, err := d.decodeMultiBulk()
	if err != nil {
		d.Err = err
	}
	return m, err
}

// Decode api
func Decode(r io.Reader) (*Resp, error) {
	return NewDecoder(r).Decode()
}

// DecodeFromBytes bytes to resp
func DecodeFromBytes(p []byte) (*Resp, error) {
	return NewDecoder(bytes.NewReader(p)).Decode()
}

// DecodeMultiBulkFromBytes format multibulk
func DecodeMultiBulkFromBytes(p []byte) ([]*Resp, error) {
	return NewDecoder(bytes.NewReader(p)).DecodeMultiBulk()
}

// decodeResp 根据返回类型调用不同解析实现
func (d *Decoder) decodeResp() (*Resp, error) {
	b, err := d.br.ReadByte()
	if err != nil {
		return nil, errorsTrace(err)
	}
	r := &Resp{}
	r.Type = byte(b)
	switch r.Type {
	default:
		return nil, errorsTrace(err)
	case TypeString, TypeError, TypeInt:
		r.Value, err = d.decodeTextBytes()
	case TypeBulkBytes:
		r.Value, err = d.decodeBulkBytes()
	case TypeArray:
		r.Array, err = d.decodeArray()
	}
	return r, err
}

// decodeTextBytes decode文本
func (d *Decoder) decodeTextBytes() ([]byte, error) {
	b, err := d.br.ReadBytes('\n')
	if err != nil {
		return nil, errorsTrace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return nil, errorsTrace(err)
	} else {
		return b[:n], nil
	}
}

// decodeInt decode int
func (d *Decoder) decodeInt() (int64, error) {
	b, err := d.br.ReadSlice('\n')
	if err != nil {
		return 0, errorsTrace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return 0, errorsTrace(err)
	} else {
		return Btoi64(b[:n])
	}
}

// decodeBulkBytes decode 批量回复
func (d *Decoder) decodeBulkBytes() ([]byte, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	switch {
	case n < -1:
		return nil, errorsTrace(err)
	case n > MaxBulkBytesLen:
		return nil, errorsTrace(err)
	case n == -1:
		return nil, nil
	}
	b, err := d.br.ReadFull(int(n) + 2)
	if err != nil {
		return nil, errorsTrace(err)
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, errorsTrace(err)
	}
	return b[:n], nil
}

// decodeArray decode 多条批量回复
func (d *Decoder) decodeArray() ([]*Resp, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	switch {
	case n < -1:
		return nil, errorsTrace(err)
	case n > MaxArrayLen:
		return nil, errorsTrace(err)
	case n == -1:
		return nil, nil
	}
	array := make([]*Resp, n)
	for i := range array {
		r, err := d.decodeResp()
		if err != nil {
			return nil, err
		}
		array[i] = r
	}
	return array, nil
}

func (d *Decoder) decodeSingleLineMultiBulk() ([]*Resp, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return nil, err
	}
	multi := make([]*Resp, 0, 8)
	for l, r := 0, 0; r <= len(b); r++ {
		if r == len(b) || b[r] == ' ' {
			if l < r {
				multi = append(multi, NewBulkBytes(b[l:r]))
			}
			l = r + 1
		}
	}
	if len(multi) == 0 {
		return nil, errorsTrace(err)
	}
	return multi, nil
}

func (d *Decoder) decodeMultiBulk() ([]*Resp, error) {
	b, err := d.br.PeekByte()
	if err != nil {
		return nil, errorsTrace(err)
	}
	if RespType(b) != TypeArray {
		return d.decodeSingleLineMultiBulk()
	}
	if _, err := d.br.ReadByte(); err != nil {
		return nil, errorsTrace(err)
	}
	n, err := d.decodeInt()
	if err != nil {
		return nil, errorsTrace(err)
	}
	switch {
	case n <= 0:
		return nil, errorsTrace(ErrBadArrayLen)
	case n > MaxArrayLen:
		return nil, errorsTrace(ErrBadArrayLenTooLong)
	}
	multi := make([]*Resp, n)
	for i := range multi {
		r, err := d.decodeResp()
		if err != nil {
			return nil, err
		}
		if r.Type != TypeBulkBytes {
			return nil, errorsTrace(ErrBadMultiBulkContent)
		}
		multi[i] = r
	}
	return multi, nil
}

/*---- Response ----*/
type Resp struct {
	Type byte

	Value []byte
	Array []*Resp
}

// NewBulkBytes 批量回复类型
func NewBulkBytes(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeBulkBytes
	r.Value = value
	return r
}

// NewArray 多条批量回复类型
func NewArray(array []*Resp) *Resp {
	r := &Resp{}
	r.Type = TypeArray
	r.Array = array
	return r
}
func errorsTrace(err error) error {
	if err != nil {
		log.Println(err.Error())
	}
	return err
}

func errorNew(msg string) error {
	return errors.New("error occur, msg ")
}
