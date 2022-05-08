package toytsdb

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_storage_Select(t *testing.T){
	head := newMemoryPartition(nil, 1 * time.Hour, Seconds)
	_,err := head.insertRows([]Row{
		{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 1, Value: 10}},
		//{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 2}},
		//{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 3}},
	})
	require.NoError(t, err)
	list := newPartitionList()
	list.insert(head)
	db := TSBD{partitionList: list, workerLimitCh: make(chan struct{}), }

	got,err := db.Select([]Label{{Name: "__name__", Value: "metric1"}},0 , 4)
	require.NoError(t, err)
	require.Equal(t, int64(1), got[0].Timestamp)
	require.Equal(t, float64(10), got[0].Value)

}