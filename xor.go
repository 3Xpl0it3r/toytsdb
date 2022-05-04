/*
Compressing time stamps algorithm
1. block header stores the staring time t(_1) aligned to a two hour
	the first time stamp t0 in block is stored as a delta from t-1 in 14 bits
	(14 bits is enough to span a bit more than 4 hours| 16384 seconds)
2. For subsequent time stamps tn
	a) D = (tn -t<n-1>) - (t<n-1> - t<n-2>)
	b) if D is zero, the store a single '0' bit
	c) if D is between [-63, 64], store '10' followed by the value(7bit)
	d) if D is between [-255, 256], store '110' followed by the value(9bit)
	e) if D is between [-2047, 2048], store '1110' followed by value(12 bit)
	f) otherwise store '1111' followed by D using 32 bits
*/

/*
Compressing values
1. first value is stored with no compression
2. if xor with the previous is zero(means they are same value),store a single '0' bit
3. if xor is not zero,calculate the number of leading and trailing zero in xor, store '1' bit followed by a) or b)
	a) Controller bit is '0' if the block of meaningful bits falls within the block of previous meaningful bits,
		=> there are at least as many leading zeros and as many trailing zeros as with previous value
		[ 0 0 1 1 1 1 0 0 ] < [0 1 1 1 1 1 0 ]
			current					previous
	b) Controller bit is 1, store and length of the number of leading zeros in the next 5 bits,and then store
	   the lenght of the meaningful xored value in the next 6 bits. Finally store the meanningful bit of the xored value
*/
package toytsdb

import (
	"fmt"
	"io"
	"math"
	"math/bits"
	"sync"
	"time"
)

type seriesEncoder interface {
	encodePoint(sample *Sample)error
	flush()error
}


type seriesDecoder interface {
	decodePoint(dst *Sample)error
}



type gorillaEncoder struct {
	// backend stream writer
	w io.Writer

	sync.Mutex

	T0  uint32
	t   uint32
	val float64

	bw       bstream
	leading  uint8
	trailing uint8
	finished bool

	tDelta uint32
}

func newSeriesEncoder(w io.Writer) *gorillaEncoder {
	t0 := uint32(time.Now().Unix())
	s := gorillaEncoder{
		T0:      t0,
		leading: ^uint8(0),
		w: w,
	}
	s.bw.writeBits(uint64(t0), 32)

	return &s
}

// Bytes value of the series stream
func (s *gorillaEncoder) Bytes() []byte {
	s.Lock()
	defer s.Unlock()
	return s.bw.bytes()
}

func finish(w *bstream) {
	// write an end-of-stream record
	w.writeBits(0x0f, 4)
	w.writeBits(0xffffffff, 32)
	w.writeBit(zero)
}

func (s *gorillaEncoder) Finish() {
	s.Lock()
	defer s.Unlock()
	if !s.finished {
		finish(&s.bw)
		s.finished = true
	}
}

// Put a timestamp and value to the series
func (s *gorillaEncoder) encodePoint(sample *Sample) error{
	// t uint32, v float64
	t := uint32(sample.Timestamp)
	v := float64(sample.Value)
	s.Lock()
	defer s.Unlock()
	if s.t == 0 {
		// the first time stamp t0 in block is stored as a delta from t-1 in 14 bits
		s.t = t
		s.val = v
		s.tDelta = t - s.T0
		s.bw.writeBits(uint64(s.tDelta), 14)
		s.bw.writeBits(math.Float64bits(v), 64)
		return nil
	}
	// compression time stamps
	tDelta := t - s.t               // delta
	dod := int32(tDelta - s.tDelta) // delta of delta
	switch {
	case dod == 0:
		// if D is zero, the store a single '0' bit
		s.bw.writeBit(zero)
	case -63 <= dod && dod <= 64:
		// if D is between [-63, 64], store '10' followed by the value(7bit)
		s.bw.writeBits(0x02, 2)
		s.bw.writeBits(uint64(dod), 7)
	case -255 <= dod && dod <= 256:
		// if D is between [-255, 256], store '110' followed by the value(9bit)
		s.bw.writeBits(0x06, 3)
		s.bw.writeBits(uint64(dod), 9)
	case -2047 <= dod && dod <= 2047:
		// if D is between [-2047, 2048], store '1110' followed by value(12 bit)
		s.bw.writeBits(0x0e, 4)
		s.bw.writeBits(uint64(dod), 12)
	default:
		// otherwise store '1111' followed by D using 32 bits
		s.bw.writeBits(0x0f, 4)
		s.bw.writeBits(uint64(dod), 32)
	}
	// compression values
	vDelta := math.Float64bits(v) ^ math.Float64bits(s.val)
	if vDelta == 0 {
		// if xor with the previous is zero(means they are same value),store a single '0' bit
		s.bw.writeBit(zero)
	} else {
		// if xor is not zero,calculate the number of leading and trailing zero in xor, store '1' bit
		s.bw.writeBit(one)

		leading := uint8(bits.LeadingZeros64(vDelta))
		trailing := uint8(bits.TrailingZeros64(vDelta))
		// void overflow
		if leading >= 32 {
			leading = 31
		}
		if s.leading != ^uint8(0) && leading >= s.leading && trailing >= s.trailing {
			//
			s.bw.writeBit(zero)
			s.bw.writeBits(vDelta>>s.trailing, 64-int(s.leading)-int(s.trailing))
		} else {
			// Controller bit is 1, store and length of the number of leading zeros in the next 5 bits,and then store
			//	the lenght of the meaningful xored value in the next 6 bits. Finally store the meanningful bit of the xored value
			s.leading, s.trailing = leading, trailing
			s.bw.writeBit(one)
			s.bw.writeBits(uint64(leading), 5)

			sigbits := 64 - leading - trailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(vDelta>>trailing, int(sigbits))
		}
	}
	s.tDelta = tDelta
	s.t = t
	s.val = v
	return nil
}

func(e *gorillaEncoder)flush()error{
	if _, err := e.w.Write(e.Bytes());err != nil{
		return fmt.Errorf("failed to flush bstream into files")
	}
	e.reset()
	return nil
}
func(e *gorillaEncoder)reset(){
	e.bw.reset()
	e.T0 = 0
	e.t = 0
	e.val = 0
	e.tDelta = 0
	e.leading = 0
	e.trailing = 0
}

type gorillaDecoder struct {
	T0 uint32

	t   uint32
	val float64

	br       bstream
	leading  uint8
	trailing uint8

	finished bool

	tDelta uint32

	err error
}


func bstreamIterator(br *bstream)(*gorillaDecoder, error){
	br.count = 8
	t0,err := br.readBits(32)
	if err != nil{
		return nil, err
	}
	return &gorillaDecoder{T0: uint32(t0), br: *br}, nil
}


func newSeriesDeocder(r io.Reader)(seriesDecoder, error){
	//return bstreamIterator(newBReader(b))
	b,err := io.ReadAll(r)
	if err != nil{
		return nil, fmt.Errorf("read bytes from diskpartition failed: %w", err)
	}
	return bstreamIterator(newBReader(b))
}

var _ seriesDecoder = new(gorillaDecoder)


func(it *gorillaDecoder)decodePoint(dst *Sample) error {
	if it.err != nil||it.finished{
		return io.EOF
	}

	if it.t == 0{
		// read first timestamp and value
		tDelta, err := it.br.readBits(14)
		if err != nil{
			it.err = err
			return err
		}
		it.tDelta = uint32(tDelta)
		it.t = it.T0 + it.tDelta
		v, err := it.br.readBits(64)
		if err != nil{
			it.err = err
			return  err
		}
		it.val = math.Float64frombits(v)
		dst.Timestamp = int64(it.t)
		dst.Value = it.val
		return nil
	}
	// read dod
	var d byte
	for i := 0; i<4 ;i++{
		d <<= 1
		bit,err := it.br.readBit()
		if err != nil{
			it.err = err
			return err
		}
		if bit == zero {
			break
		}
		d |= 1
	}
	var dod int32
	var sz uint
	switch d {
	case 0x00:
		dod = 0
	case 0x02:
		sz = 7
	case 0x06:
		sz = 9
	case 0x0e:
		sz = 12
	case 0x0f:
		bits, err := it.br.readBits(32)
		if err != nil{
			it.err = err
			return err
		}
		if bits == 0xffffffff{
			it.finished = true
			return err
		}
		dod = int32(bits)
	}
	if sz!= 0{
		bits,err := it.br.readBits(int(sz))
		if err != nil{
			it.err = err
			return err
		}
		if bits > (1 << (sz-1)){
			bits = bits - (1<<sz)
		}
		dod = int32(bits)
	}
	tDelta := it.tDelta + uint32(dod)
	it.tDelta = tDelta
	it.t = it.t + it.tDelta

	// read compressed value
	bit ,err := it.br.readBit()
	if err != nil{
		it.err = err
		return err
	}
	if bit == zero {
		// it.val = it.val
	}else {
		controlBit,itErr := it.br.readBit()
		if itErr != nil{
			it.err = err
			return err
		}
		if controlBit == zero {
			// case a
			// reuse leading/trailing zero bit
			// it.leading, it.trailing = it.leading, it.trailing
		}else {
			// case b
			bits,err := it.br.readBits(5)	// read the number of leading zero
			if err != nil{
				it.err = err
				return err
			}
			it.leading = uint8(bits)

			bits,err = it.br.readBits(6)	// read the number of significant
			if err != nil{
				it.err = err
				return err
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflow and we can actually need 64 ; see comment  it encoder
			if mbits == 0{
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits
		}
		mbits := int(64-it.leading - it.trailing)
		xorBits,err := it.br.readBits(mbits)
		if err != nil{
			it.err = err
			return err
		}
		vbits := math.Float64bits(it.val)
		// value0 ^ value1 = xor1  => xor1 ^ value0 = value1
		vbits ^= (xorBits << it.trailing)
		it.val = math.Float64frombits(vbits)
	}
	dst.Timestamp = int64(it.t)
	dst.Value = it.val
	return nil
}


func(it *gorillaDecoder)Value()(uint32, float64){
	return it.t, it.val
}

func(it *gorillaDecoder)Err()error{
	return it.err
}
