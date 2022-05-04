package toytsdb

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBstreamReader(t *testing.T){
	w := bstream{}
	for _, bit := range []bit{true, false}{
		w.writeBit(bit)
	}

	for nbits := 1; nbits <= 64; nbits++{
		w.writeBits(uint64(nbits), nbits)
	}
	for v := 1 ; v < 1000; v ++{
		w.writeBits(uint64(v), 29)
	}

	// read
	r := newBReader(w.bytes())
	for _, bit := range []bit{true, false}{
		got,err := r.readBit()
		require.NoError(t, err)
		assert.Equal(t, bit, got)
	}

	for nbits := 1; nbits <= 64; nbits++{
		got,err := r.readBits(nbits)
		assert.NoError(t, err)
		assert.Equal(t, nbits, int(got), "nbits=%d", nbits)
	}

	for v := 1; v < 1000; v ++{
		got,err := r.readBits(29)
		assert.NoError(t, err)
		assert.Equal(t, uint64(v), got, "v=%d  got: %d", v, got)
	}


}
