package toytsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_memoryPartition_InsertRows(t *testing.T) {
	tests := []struct {
		name               string
		memoryPartition    *MemSeries
		rows               []Row
		wantErr            bool
		wantDatePoints     []*Sample
		wantOutOfOrderRows []Row
	}{
		{
			name: "inset in-order rows",
			memoryPartition: newMemoryPartition(nil, 0, "").(*MemSeries),
			rows: []Row{
				{Metric: "metric1", Sample: Sample{Timestamp:1, Value: 0.1}},
				{Metric: "metric1", Sample: Sample{Timestamp:2, Value: 0.1}},
				{Metric: "metric1", Sample: Sample{Timestamp:3, Value: 0.1}},
			},
			wantDatePoints: []*Sample{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
			},
			wantOutOfOrderRows: []Row{},
		},
		{
			name: "insert out-of-order rows",
			memoryPartition: func() *MemSeries {
				m := newMemoryPartition(nil, 0, "").(*MemSeries)
				m.insertRows([]Row{
					{Metric: "metric1", Sample: Sample{Timestamp: 2, Value: 0.1}},
				})
				return m
			}(),
			rows: []Row{
				{Metric: "metric1", Sample: Sample{Timestamp: 1, Value: 0.1}},
			},
			wantDatePoints: []*Sample{
				{Timestamp: 2, Value: 0.1},
			},
			wantOutOfOrderRows: []Row{
				{Metric: "metric1", Sample: Sample{Timestamp: 1, Value: 0.1}},
			},
		},
	}
	for _, tt := range tests{
		t.Run(tt.name, func(t *testing.T) {
			gotOutOfOrder, err := tt.memoryPartition.insertRows(tt.rows)
			assert.Equal(t, tt.wantErr, err!= nil)
			assert.Equal(t, tt.wantOutOfOrderRows, gotOutOfOrder)

			got, _ := tt.memoryPartition.selectDataPoints("metric1", nil, 0, 4)
			assert.Equal(t, tt.wantDatePoints, got)
		})
	}
}

func Test_memoryPartition_SelectDataPoints(t *testing.T) {
	tests := []struct{
		name string
		metric string
		labels []Label
		start int64
		end int64
		memoryPartition *MemSeries
		want []*Sample
	}{
		{
			name: "given non-exist metric name",
			metric: "unknown",
			start : 1,
			end: 2,
			memoryPartition: newMemoryPartition(nil, 0, "").(*MemSeries),
			want: []*Sample{},
		},
		{
			name: "select multiple points",
			metric: "metric1",
			start: 1,
			end: 4,
			memoryPartition: func() *MemSeries{
				m := newMemoryPartition(nil,0, "").(*MemSeries)
				m.insertRows([]Row{
					{
						Metric: "metric1",
						Sample: Sample{Timestamp: 1, Value: 0.1},
					},
					{
						Metric: "metric1",
						Sample: Sample{Timestamp: 2, Value: 0.1},
					},
					{
						Metric: "metric1",
						Sample: Sample{Timestamp: 3, Value: 0.1},
					},
				})
				return m
			}(),
			want: []*Sample{
				{
					Timestamp: 1, Value: 0.1,
				},
				{
					Timestamp: 2, Value: 0.1,
				},
				{
					Timestamp: 3, Value: 0.1,
				},
			},
		},
	}

	for _,tt := range tests{
		t.Run(tt.name, func(t *testing.T) {
			got, _ := tt.memoryPartition.selectDataPoints(tt.metric, tt.labels, tt.start, tt.end)
			assert.Equal(t, tt.want, got)
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
