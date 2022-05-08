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
	   the length of the meaningful xorEd value in the next 6 bits. Finally store the meaningful bit of the xorEd value
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

//type seriesEncoder interface {
//	encodePoint(t int64, v float64) error
//	flush() error
//}
//
//type seriesDecoder interface {
//	Iterator() error
//	Value()(int64, float64)
//}

type XORChunk struct {
	// backend stream writer
	w io.Writer

	sync.Mutex

	T0  int64
	t   int64
	val float64

	bw       bStream
	leading  uint8
	trailing uint8
	finished bool

	tDelta uint32
}

func NewXorChunk(writer io.Writer) *XORChunk {
	s := XORChunk{
		T0:      time.Now().Unix(),
		leading: ^uint8(0),
		w:       writer,
	}
	s.bw.writeBits(uint64(s.T0), 64)

	return &s
}

// Bytes value of the series stream
func (e *XORChunk) Bytes() []byte {
	e.Lock()
	defer e.Unlock()
	return e.bw.bytes()
}

func finish(w *bStream) {
	// write an end-of-stream record
	w.writeBits(0x0f, 4)
	w.writeBits(0xffffffff, 32)
	w.writeBit(zero)
}

func (e *XORChunk) Finish() {
	e.Lock()
	defer e.Unlock()
	if !e.finished {
		finish(&e.bw)
		e.finished = true
	}
}

// Put a timestamp and value to the series
func (e *XORChunk) Append(t int64, v float64) error {
	// t uint32, v float64
	e.Lock()
	defer e.Unlock()
	if e.t == 0 {
		// the first time stamp t0 in block is stored as a delta from t-1 in 14 bits
		e.t = t
		e.val = v
		e.tDelta = uint32(t - e.T0)

		e.bw.writeBits(uint64(e.tDelta), 32)
		e.bw.writeBits(math.Float64bits(v), 64)
		return nil
	}
	// compression time stamps
	tDelta := uint32(t - e.t)       // delta
	dod := int32(tDelta - e.tDelta) // delta of delta
	switch {
	case dod == 0:
		// if D is zero, the store a single '0' bit
		e.bw.writeBit(zero)
	case -63 <= dod && dod <= 64:
		// if D is between [-63, 64], store '10' followed by the value(7bit)
		e.bw.writeBits(0x02, 2)
		e.bw.writeBits(uint64(dod), 7)
	case -255 <= dod && dod <= 256:
		// if D is between [-255, 256], store '110' followed by the value(9bit)
		e.bw.writeBits(0x06, 3)
		e.bw.writeBits(uint64(dod), 9)
	case -2047 <= dod && dod <= 2047:
		// if D is between [-2047, 2048], store '1110' followed by value(12 bit)
		e.bw.writeBits(0x0e, 4)
		e.bw.writeBits(uint64(dod), 12)
	default:
		// otherwise store '1111' followed by D using 32 bits
		e.bw.writeBits(0x0f, 4)
		e.bw.writeBits(uint64(dod), 32)
	}
	// compression values
	vDelta := math.Float64bits(v) ^ math.Float64bits(e.val)
	if vDelta == 0 {
		// if xor with the previous is zero(means they are same value),store a single '0' bit
		e.bw.writeBit(zero)
	} else {
		// if xor is not zero,calculate the number of leading and trailing zero in xor, store '1' bit
		e.bw.writeBit(one)

		leading := uint8(bits.LeadingZeros64(vDelta))
		trailing := uint8(bits.TrailingZeros64(vDelta))
		// void overflow
		if leading >= 32 {
			leading = 31
		}
		if e.leading != ^uint8(0) && leading >= e.leading && trailing >= e.trailing {
			//
			e.bw.writeBit(zero)
			e.bw.writeBits(vDelta>>e.trailing, 64-int(e.leading)-int(e.trailing))
		} else {
			// Controller bit is 1, store and length of the number of leading zeros in the next 5 bits,and then store
			//	the length of the meaningful xorEd value in the next 6 bits. Finally store the meaningful bit of the xorEd value
			e.leading, e.trailing = leading, trailing
			e.bw.writeBit(one)
			e.bw.writeBits(uint64(leading), 5)

			sigBits := 64 - leading - trailing
			e.bw.writeBits(uint64(sigBits), 6)
			e.bw.writeBits(vDelta>>trailing, int(sigBits))
		}
	}
	e.tDelta = tDelta
	e.t = t
	e.val = v
	return nil
}

func (e *XORChunk) flush() error {
	if _, err := e.w.Write(e.Bytes()); err != nil {
		return fmt.Errorf("failed to flush bstream into files")
	}
	e.reset()
	return nil
}
func (e *XORChunk) reset() {
	e.bw.reset()
	e.T0 = 0
	e.t = 0
	e.val = 0
	e.tDelta = 0
	e.leading = 0
	e.trailing = 0
}

type XORIterator struct {
	T0 int64

	t   int64
	val float64

	br       bStream
	leading  uint8
	trailing uint8

	finished bool

	tDelta int32

	err error
}

func newIterator(r io.Reader) (*XORIterator, error) {
	b, err := io.ReadAll(r)

	if err != nil {
		return nil, fmt.Errorf("read bytes from diskpartition failed: %w", err)
	}
	br := newBReader(b)
	t0, err := br.readBits(64)
	if err != nil {
		return nil, err
	}
	return &XORIterator{T0: int64(t0), br: *br}, nil
}

func (it *XORIterator) Next() error {
	if it.err != nil || it.finished {
		return io.EOF
	}

	if it.t == 0 {
		// read first timestamp and value
		tDelta, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return err
		}
		it.tDelta = int32(tDelta)
		it.t = it.T0 + int64(it.tDelta)
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return err
		}
		it.val = math.Float64frombits(v)
		return nil
	}
	// read dod
	var d byte
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBit()
		if err != nil {
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
		if err != nil {
			it.err = err
			return err
		}
		if bits == 0xffffffff {
			it.finished = true
			return err
		}
		dod = int32(bits)
	}
	if sz != 0 {
		bits, err := it.br.readBits(int(sz))
		if err != nil {
			it.err = err
			return err
		}
		if bits > (1 << (sz - 1)) {
			bits = bits - (1 << sz)
		}
		dod = int32(bits)
	}
	tDelta := it.tDelta + dod
	it.tDelta = tDelta
	it.t = it.t + int64(it.tDelta)

	// read compressed value
	bit, err := it.br.readBit()
	if err != nil {
		it.err = err
		return err
	}
	if bit == zero {
		// it.val = it.val
	} else {
		controlBit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return err
		}
		if controlBit == zero {
			// case a
			// reuse leading/trailing zero bit
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			// case b
			bits, err := it.br.readBits(5) // read the number of leading zero
			if err != nil {
				it.err = err
				return err
			}
			it.leading = uint8(bits)

			bits, err = it.br.readBits(6) // read the number of significant
			if err != nil {
				it.err = err
				return err
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflow and we can actually need 64 ; see comment  it encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits
		}
		mbits := int(64 - it.leading - it.trailing)
		xorBits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return err
		}
		vBits := math.Float64bits(it.val)
		// value0 ^ value1 = xor1  => xor1 ^ value0 = value1
		vBits ^= xorBits << it.trailing
		it.val = math.Float64frombits(vBits)
	}
	return nil
}

func (it *XORIterator) Value() (int64, float64) {
	return it.t, it.val
}

func (it *XORIterator) Err() error {
	return it.err
}
