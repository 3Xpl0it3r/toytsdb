package toytsdb

import (
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func Test_BStream_Write(t *testing.T) {
	writer := newBWriter(1024)
	writer.writeByte(8)

	reader := newBReader(writer.clone().bytes())
	gotV, gotErr := reader.readByte()
	require.Equal(t, uint8(8), gotV)
	require.Equal(t, nil, gotErr)

	writer.reset()
	writer.writeBits(12, 21)
	reader = newBReader(writer.bytes())
	gotv, goterr := reader.readBits(21)
	require.Equal(t, uint64(12), gotv)
	require.Equal(t, nil, goterr)
}

func Test_BStream_ReadBit(t *testing.T) {
	cases := []struct {
		name        string
		input       []bit
		readCount   int
		expectedOut []bit
		expectedErr error
	}{
		{
			name:        "empty read",
			input:       []bit{},
			expectedOut: nil,
			readCount:   1,
			expectedErr: io.EOF,
		},
		{
			name:        "write one bit, read one bit",
			input:       []bit{one},
			expectedOut: []bit{one},
			readCount:   1,
			expectedErr: nil,
		},
		{
			name:        "write one bit,  read two bit",
			input:       []bit{one},
			readCount:   2,
			expectedOut: []bit{one},
			expectedErr: nil,
		},
		{
			name:        "write 8 bit,read 8 bit",
			input:       []bit{one, zero, one, zero, one, zero, one, zero},
			readCount:   8,
			expectedOut: []bit{one, zero, one, zero, one, zero, one, zero},
			expectedErr: nil,
		},
		{
			name:        "write 8 bit ,read 9 bit",
			input:       []bit{one, zero, one, zero, one, zero, one, zero},
			readCount:   9,
			expectedOut: []bit{one, zero, one, zero, one, zero, one, zero},
			expectedErr: io.EOF,
		},
		{
			name:        "write 9bit, read 9bit",
			input:       []bit{one, zero, one, zero, one, zero, one, zero, one, zero, one},
			readCount:   9,
			expectedOut: []bit{one, zero, one, zero, one, zero, one, zero, one, zero, one},
			expectedErr: nil,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			write := newBWriter(4096)
			for _, v := range tt.input {
				write.writeBit(v)
			}
			read := newBReader(write.bytes())
			count := 0
			for ; count < tt.readCount; count++ {
				gotV, gotErr := read.readBit()
				if count < len(tt.input) {
					require.Equal(t, tt.expectedOut[count], gotV)
					require.NoError(t, gotErr)
				} else {
					require.Equal(t, tt.expectedErr, gotErr)
				}
			}
		})
	}

}

func Test_BStream_ReadByte(t *testing.T) {
	cases := []struct {
		name          string
		reader        bStream
		expectedValue byte
		expectedErr   error
	}{
		{
			name: "empty reader", // line 124-125
			reader: bStream{
				stream: []byte{},
				count:  0,
			},
			expectedValue: 0,
			expectedErr:   io.EOF,
		},
		{
			name: "only one byte, but has been read",
			reader: bStream{
				stream: []byte{0},
				count:  0,
			},
			expectedValue: 0,
			expectedErr:   io.EOF,
		},
		{
			name: "has read one block, next block is enough",
			reader: bStream{
				stream: []byte{0, 9},
				count:  0,
			},
			expectedValue: 9,
			expectedErr:   nil,
		},
		{
			name: "read block is not enough",
			reader: bStream{
				stream: []byte{0},
				count:  1,
			},
			expectedValue: 0,
			expectedErr:   io.EOF,
		},
		{
			name: "read block is enough",
			reader: bStream{
				stream: []byte{1, 0},
				count:  8,
			},
			expectedValue: 1,
			expectedErr:   nil,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			gotV, gotErr := tt.reader.readByte()
			require.Equal(t, tt.expectedValue, gotV)
			require.Equal(t, tt.expectedErr, gotErr)
		})
	}
}

func Test_BStream_ReadBits(t *testing.T) {
	cases := []struct {
		name        string
		reader      bStream
		readBits    int
		expectedVal uint64
		expectedErr error
	}{
		{
			name: "is not enough",
			reader: bStream{
				stream: []byte{},
				count:  0,
			},
			readBits:    64,
			expectedErr: io.EOF,
		},
		{
			name: "read 64 bit",
			reader: bStream{
				stream: []byte{0, 0, 0, 0, 0, 0, 0, 1},
				count:  8,
			},
			readBits:    64,
			expectedVal: 1,
			expectedErr: nil,
		},
		{
			name: "first read 8bit, is not enoug",
			reader: bStream{
				stream: []byte{0},
				count:  8,
			},
			readBits:    9,
			expectedVal: 0,
			expectedErr: io.EOF,
		},
		{
			name: "read 54 bit is ok",
			reader: bStream{
				stream: []byte{0, 0, 2, 0},
				count:  8,
			},
			readBits:    23,
			expectedVal: 1,
			expectedErr: nil,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			gotV, gotErr := tt.reader.readBits(tt.readBits)
			require.Equal(t, tt.expectedVal, gotV)
			require.Equal(t, tt.expectedErr, gotErr)
		})
	}
}

func Test_Bstream_Handler(t *testing.T) {
	writer := newBWriter(1024)
	for _i := 0; _i < 10; _i++ {
		writer.writeBit(_i/2 == 0)
	}
	for _i := 1; _i <= 128; _i++ {
		writer.writeByte(byte(_i))
	}
	for _i := 1; _i < 64; _i++ {
		writer.writeBits(uint64(_i), 7)
	}
	reader := newBReader(writer.clone().bytes())
	for _i := 0; _i < 10; _i++ {
		gotV, gotErr := reader.readBit()
		require.NoError(t, gotErr)
		require.Equal(t, bit(_i/2 == 0), gotV)
	}
	for _i := 1; _i <= 128; _i++ {
		gotV, gotErr := reader.readByte()
		require.NoError(t, gotErr)
		require.Equal(t, byte(_i), gotV)
	}
	for _i := 1; _i < 64; _i++ {
		gotV, gotErr := reader.readBits(7)
		require.NoError(t, gotErr)
		require.Equal(t, uint64(_i), gotV)
	}
}
