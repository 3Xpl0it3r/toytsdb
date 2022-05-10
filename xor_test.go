package toytsdb

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_XOR_Handler(t *testing.T) {
	var now int64 = 1651982439
	cases := []struct {
		name        string
		input       []Sample
		expectedErr error
	}{
		{
			name:  "reader none",
			input: []Sample{},
		},
		{
			name: "encode timestamp, dod=0",
			input: []Sample{
				{Timestamp: now, Value: 0.1},
				{Timestamp: now, Value: 0.2},
				{Timestamp: now, Value: 0.1},
			},
		},
		{
			name: "encode timestamp, dod in [-63,64] ",
			input: []Sample{
				{Timestamp: now, Value: 0.1},
				{Timestamp: now, Value: 0.1},
				{Timestamp: now + 62, Value: 0.2},
			},
		},
		{
			name: "encode timestamp dod in [-255, 256]",
			input: []Sample{
				{Timestamp: now, Value: 0.1},
				{Timestamp: now, Value: 0.1},
				{Timestamp: now + 250, Value: 0.2},
			},
		},
		{
			name: "encode timestamp dod in [-2047,2048]",
			input: []Sample{
				{Timestamp: now, Value: 0.1},
				{Timestamp: now, Value: 0.1},
				{Timestamp: now + 2040, Value: 0.3},
			},
		},
		{
			name: "encode timestamp dod > 4096",
			input: []Sample{
				{Timestamp: now, Value: 0.1},
				{Timestamp: now, Value: 0.1},
				{Timestamp: now + 4096, Value: 0.1},
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			fp, err := os.OpenFile("temp", os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
			require.NoError(t, err)
			encoder := NewXorChunk(fp)
			for _, sample := range tt.input {
				require.NoError(t, encoder.Append(sample.Timestamp, sample.Value))
			}
			encoder.Finish()
			require.NoError(t, encoder.flush())
			require.NoError(t, fp.Close())
			encoder.reset()

			fp, err = os.Open("temp")

			require.NoError(t, err)
			decoder, err := newIterator(fp)
			require.NoError(t, err)
			for i := 0; i < len(tt.input); i++ {
				err := decoder.Next()
				require.NoError(t, err)
				gotT, gotV := decoder.Value()
				require.Equal(t, tt.input[i].Timestamp, gotT)
				require.Equal(t, tt.input[i].Value, gotV)
			}

			require.NoError(t, fp.Close())
			require.NoError(t, os.RemoveAll("temp"))
		})
	}
}
