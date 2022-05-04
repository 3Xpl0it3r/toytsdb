// code is from https://github.com/dgryski/go-tsz
// TSDB压缩 Gorilla golang的实现

package toytsdb

import "io"

// bstream is a stream of bits
type bstream struct {
	// the data stream
	stream []byte

	// how many bits are valid in current byte
	count uint8
}

func newBReader(b []byte)*bstream {
	return &bstream{stream: b, count: 8}
}

func newBWriter(size int) bstream {
	return bstream{stream: make([]byte,0, size)}
}


func(b *bstream)clone()*bstream {
	d := make([]byte, len(b.stream))
	copy(d, b.stream)
	return &bstream{stream: d, count: b.count}
}

func(b *bstream)bytes()[]byte{
	return b.stream
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

// 写入一个bit，
// [0 0 0 0 0 0 0 0]
// <----------------写入方向
// writeBit write a bit into bstream, from hight address to low address
func(b *bstream)writeBit(bit bit){
	if b.count == 0{
		//当前字节里面没有剩余的bit可供写入，追加一个字节
		b.stream = append(b.stream, 0)
		b.count = 8
	}
	i := len(b.stream) -1
	if bit == true{
		b.stream[i] |= 1 << (b.count-1)
	}
	b.count --
}

// 往bstream里面写入一个字节
// writeByte write a byte into bstream
func(b *bstream)writeByte(byt byte){
	if b.count == 0{
		b.stream = append(b.stream, 0)
		b.count = 8
	}
	i := len(b.stream)-1
	// 高 8-b.count 位写入前面剩余的底b.count位
	b.stream[i] |= byt >> (8-b.count)
	// 追加一个字节
	b.stream = append(b.stream, 0)
	i ++
	// 将byt的低b.count位写入到b.stram[i]里面
	b.stream[i] = byt << b.count
}


// writeBits 将u 的低nbits位写入到bstream里面
func(b *bstream)writeBits(u uint64, nbits int){
	// 保留低nbits位(这个是需要写入到bstram里面的)
	u <<= (64-uint(nbits))
	for nbits >= 8{
		// 获取需要写入数据的前8个bit
		byt := byte(u>>56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}
	for nbits > 0{
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits --
	}
}

// readBit读取一个bit
// [0 0 0 0 0 0 0 0]
// 从高位开始读
/*
[1 2 3 4 5 6 7 8] 读取一个bit后
[2 3 4 5 6 7 8 0]
一个位只能是0/1 这里用十进制表示为了说明读取顺序
 */
func (b *bstream)readBit()(bit, error){
	if len(b.stream) == 0{
		return false, io.EOF
	}
	// b.count == 0 意味着当前字节里面的8位已经被全部读取完了
	if b.count == 0{
		b.stream = b.stream[1:]
		if len(b.stream) == 0{
			return false, io.EOF
		}
		b.count = 8
	}

	b.count --
	d := b.stream[0] & 0x80  // b'1000 0000'
	b.stream[0] <<= 1
	return d != 0, nil
}

func(b *bstream)readByte()(byte , error){
	if len(b.stream) == 0{
		return 0, io.EOF
	}
	if b.count == 0{
		b.stream = b.stream[1:]
		if len(b.stream) == 0{
			return 0, io.EOF
		}
		b.count = 8
	}
	if b.count == 8{
		b.count = 0
		return b.stream[0], nil
	}
	byt := b.stream[0]
	b.stream = b.stream[1:]
	if len(b.stream) == 0{
		return 0, io.EOF
	}
	// 将b.stream[0]里面的高8-count位写如到byt的低8-count位里面
	byt |= b.stream[0] >> b.count
	// 将读取完的位清0
	b.stream[0] <<= (8-b.count)
	return byt, nil
}


func(b *bstream)readBits(nbits int)(uint64, error){
	// 写入U从低到高写
	var u uint64
	for nbits >= 8{
		byt ,err := b.readByte()
		if err != nil{
			return 0, err
		}
		u = (u<<8)|uint64(byt)
		nbits -= 8
	}
	if nbits == 0{
		return u, nil
	}
	if nbits > int(b.count){
		u = (u << uint(b.count)) | uint64(b.stream[0] >> (8-b.count))
		nbits -= int(b.count)
		b.stream = b.stream[1:]
		if len(b.stream) == 0{
			return 0, io.EOF
		}
		b.count = 8
	}

	u = (u << uint(nbits)) | uint64(b.stream[0] >> (8-uint(nbits)))
	b.stream[0] <<= uint(nbits)
	b.count -= uint8(nbits)
	return u, nil
}

func(b *bstream)reset(){
	b.stream = b.stream[:0]
	b.count = 0
}