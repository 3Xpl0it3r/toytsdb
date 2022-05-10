package toytsdb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_WAL_Handler(t *testing.T) {
	wal, err := newDiskWAL("tmp", 1024)
	require.NoError(t, err)
	cases := []Row{
		{
			Labels: []Label{
				{Name: "__name__", Value: "metric"},
			},
			Sample: Sample{Timestamp: 1, Value: 0.1},
		},
		{
			Labels: []Label{
				{Name: "__name__", Value: "metric"},
			},
			Sample: Sample{Timestamp: 2, Value: 0.2},
		},
	}
	require.NoError(t, wal.append(cases))
	require.NoError(t, wal.Close())

	reader, err := newDiskWALReader("tmp")
	require.NoError(t, err)
	require.NoError(t, reader.readAll())
	for index, got := range reader.rowsToInsert {
		require.Equal(t, cases[index].Value, got.Value)
		require.Equal(t, cases[index].Timestamp, got.Timestamp)
	}

}

func Test_WAL_Encode(t *testing.T) {

}

func Test_WAL_Decode(t *testing.T) {

}

func Test_WAL_Recover(t *testing.T) {

}
