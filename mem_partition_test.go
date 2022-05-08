package toytsdb

import (
	"github.com/stretchr/testify/assert"
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
			name:            "inset in-order rows",
			memoryPartition: newMemoryPartition(nil, 0, "").(*MemPartition),
			rows: []Row{mockRow1, mockRow2, mockRow3, mockRow4},
			wantDatePoints: []*Sample{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.1},
				{Timestamp: 3, Value: 0.1},
				{Timestamp: 4, Value: 0.1},
			},
			wantOutOfOrderRows: []Row{},
		},
		{
			name: "insert out-of-order rows",
			memoryPartition: func() *MemPartition {
				m := newMemoryPartition(nil, 0, "").(*MemPartition)
				m.insertRows([]Row{
					{Labels:
					[]Label{{Name: "__name__", Value: "metric1"}},
						Sample: Sample{Timestamp: 3, Value: 0.1},
					},
				})
				return m
			}(),
			rows: []Row{
				{Labels:
				[]Label{{Name: "__name__", Value: "metric1"}},
					Sample: Sample{Timestamp: 1, Value: 0.1},
				},
			},
			wantDatePoints: []*Sample{
				{Timestamp: 3, Value: 0.1},
			},
			wantOutOfOrderRows: []Row{
				{Labels:
				[]Label{{Name: "__name__", Value: "metric1"}},
					Sample: Sample{Timestamp: 1, Value: 0.1},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutOfOrder, err := tt.memoryPartition.insertRows(tt.rows)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantOutOfOrderRows, gotOutOfOrder)
			labels := Labels{
				{Name: "__name__", Value: "metric1"},
			}

			got, _ := tt.memoryPartition.selectDataPoints(labels, 0, 4)
			assert.Equal(t, tt.wantDatePoints, got)
		})
	}
}

func Test_memoryPartition_SelectDataPoints(t *testing.T) {
	tests := []struct {
		name            string
		labels          []Label
		start           int64
		end             int64
		memoryPartition *MemPartition
		want            []*Sample
	}{
		{
			name:            "given non-exist metric name",
			start:           1,
			end:             2,
			memoryPartition: newMemoryPartition(nil, 0, "").(*MemPartition),
			want:            []*Sample{},
		},
		{
			name:   "select multiple points",
			labels: []Label{{Name: "__name__", Value: "metric1"}},
			start:  0,
			end:    4,
			memoryPartition: func() *MemPartition {
				m := newMemoryPartition(nil, 0, "").(*MemPartition)
				m.insertRows([]Row{
					mockRow1,mockRow2,mockRow3,
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := tt.memoryPartition.selectDataPoints(tt.labels, tt.start, tt.end)
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
