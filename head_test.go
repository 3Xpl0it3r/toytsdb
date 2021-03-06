package toytsdb

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var (
	mockRow1 = Row{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 1, Value: 0.1}}
	mockRow2 = Row{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 2, Value: 0.1}}
	mockRow3 = Row{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 3, Value: 0.1}}
	mockRow4 = Row{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 4, Value: 0.1}}
)

func Test_memoryPartition_InsertRows(t *testing.T) {
	tests := []struct {
		name               string
		memoryPartition    *MemPartition
		rows               []Row
		wantErr            bool
		wantDatePoints     []*Sample
		wantOutOfOrderRows []Row
	}{
		{
			name: "inset in-order rows",
			memoryPartition: func() *MemPartition {
				m, _ := newMemoryPartition("", nil, 0, "")

				return m
			}(),
			rows: []Row{mockRow1, mockRow2, mockRow3, mockRow4},
			wantDatePoints: []*Sample{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
				{Timestamp: 4, Value: 0.1},
			},
			wantOutOfOrderRows: []Row{},
		},
		// does not support un-order samples any more
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _,row := range tt.rows{
				ref,err := tt.memoryPartition.Add(row.Labels, row.Timestamp, row.Value)
				require.Equal(t,row.Labels.Hash(), ref)
				require.NoError(t, err)
			}

			labels := Labels{
				{Name: "__name__", Value: "metric1"},
			}
			require.NoError(t, tt.memoryPartition.Commit())

			got, _ := tt.memoryPartition.selectDataPoints(labels, 0, 4)
			assert.Equal(t, tt.wantDatePoints, got)
		})
	}
}



func Test_toUnix(t *testing.T) {
	tests := []struct {
		name      string
		t         time.Time
		precision TimestampPrecision
		want      int64
	}{
		{
			name:      "to nanosecond",
			t:         time.Unix(1600000000, 0),
			precision: Nanoseconds,
			want:      1600000000000000000,
		},
		{
			name:      "to microsecond",
			t:         time.Unix(1600000000, 0),
			precision: Microseconds,
			want:      1600000000000000,
		},
		{
			name:      "to millisecond",
			t:         time.Unix(1600000000, 0),
			precision: Milliseconds,
			want:      1600000000000,
		},
		{
			name:      "to second",
			t:         time.Unix(1600000000, 0),
			precision: Seconds,
			want:      1600000000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toUnix(tt.t, tt.precision)
			assert.Equal(t, tt.want, got)
		})
	}
}
