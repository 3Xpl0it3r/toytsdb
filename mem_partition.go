package toytsdb

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var _ partition = new(MemPartition)

type MemPartition struct {
	metrics sync.Map
	numPoints int64

	minT int64
	maxT int64

	// write ahead log
	wal Wal
	// The timestamp range of partition after which they get persisted
	partitionDuration int64
	timestampPrecision TimestampPrecision
	once sync.Once
}


func newMemoryPartition(wal Wal, partitionDuration time.Duration, precision TimestampPrecision)partition{
	if wal == nil{
		wal = &nopWal{}
	}
	var d int64
	switch precision {
	case Nanoseconds:
		d = partitionDuration.Nanoseconds()
	case Microseconds:
		d = partitionDuration.Microseconds()
	case Milliseconds:
		d = partitionDuration.Milliseconds()
	case Seconds:
		d = int64(partitionDuration.Seconds())
	default:
		d = partitionDuration.Nanoseconds()
	}
	return &MemPartition{
		partitionDuration:  d,
		timestampPrecision: precision,
		wal: wal,
	}
}


func (m *MemPartition) insertRows(rows []Row) ( []Row,  error) {
	if len(rows) == 0{
		return nil, fmt.Errorf("no rows given ")
	}
	if err := m.wal.append(operationInsert, rows);err != nil{
		return nil, fmt.Errorf("failed to write to wal %w",err)
	}

	// Set min timestamp at only first
	m.once.Do(func() {
		min := rows[0].Timestamp
		for i := range rows{
			row := rows[i]
			if row.Timestamp < min{
				min = row.Timestamp
			}
		}
		atomic.SwapInt64(&m.minT, min)
	})
	outdatedRows := make([]Row, 0)
	maxTimestamp := rows[0].Timestamp
	var rowsNum int64
	for i := range rows{
		row := rows[i]
		if row.Timestamp < m.minTimestamp(){
			outdatedRows = append(outdatedRows, row)
			continue
		}

		if row.Timestamp == 0{
			row.Timestamp = toUnix(time.Now(), m.timestampPrecision)
		}
		if row.Timestamp > maxTimestamp{
			m.maxT = row.Timestamp
		}
		mt := m.getMetric(row.Labels.Hash())
		mt.insertPoint(&row.Sample)
		rowsNum ++
	}
	atomic.AddInt64(&m.numPoints, rowsNum)

	// Make max timestamp up-to-date
	if atomic.LoadInt64(&m.maxT) < maxTimestamp{
		atomic.SwapInt64(&m.maxT, maxTimestamp)
	}
	return outdatedRows, nil
}

func (m *MemPartition) selectDataPoints(labels Labels, start, end int64) ([]*Sample, error) {
	mt := m.getMetric(labels.Hash())
	return mt.selectPoints(start, end), nil
}

func (m *MemPartition) minTimestamp() int64 {
	return atomic.LoadInt64(&m.minT)
}

func (m *MemPartition) maxTimestamp() int64 {
	return atomic.LoadInt64(&m.maxT)
}

func (m *MemPartition) size() int {
	return int(atomic.LoadInt64(&m.numPoints))
}

func (m *MemPartition) active() bool {
	return m.maxTimestamp() - m.minTimestamp() + 1 < m.partitionDuration
}

func(m *MemPartition)getMetric(name uint64)*memoryMetric{
	// 使用并发map来实现， prometheues 里面采用了分片锁来提高并发行
	// todo 更换成分片锁
	value,ok := m.metrics.Load(name)
	if !ok {
		value = &memoryMetric{
			name:             name,
			points:           make([]*Sample,0, 1000),
			outOfOrderPoints: make([]*Sample,0),
		}
		m.metrics.Store(name, value)
	}
	return value.(*memoryMetric)
}

func(m *MemPartition)clean()error{
	runtime.GC()
	return nil
}

func (f *MemPartition) expired() bool {
	return false
}

type memoryMetric struct {
	name uint64
	size int64
	minTimestamp int64
	maxTimestamp int64

	points []*Sample
	outOfOrderPoints []*Sample
	mu sync.RWMutex
}

func(m *memoryMetric)insertPoint(point *Sample){
	size := atomic.LoadInt64(&m.size)
	m.mu.Lock()
	defer m.mu.Unlock()

	if size == 0{
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.minTimestamp, point.Timestamp)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}

	// Insert point in order
	if m.points[size-1].Timestamp < point.Timestamp{
		m.points = append(m.points, point)
		atomic.SwapInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}
	m.outOfOrderPoints = append(m.outOfOrderPoints, point)
}


func(m *memoryMetric)selectPoints(start, end int64)[]*Sample{
	size := atomic.LoadInt64(&m.size)
	minTimestamp := atomic.LoadInt64(&m.minTimestamp)
	maxTimestamp := atomic.LoadInt64(&m.maxTimestamp)
	var startIdx, endIdx int
	if end <= minTimestamp{
		return []*Sample{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if start <= minTimestamp{
		startIdx = 0
	}else {
		startIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp >= start
		})
	}

	if end >= maxTimestamp{
		endIdx = int(size)
	}else {
		endIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp < end
		})
	}
	return m.points[startIdx: endIdx]
}

func toUnix(t time.Time, precision TimestampPrecision)int64{
	switch precision {
	case Nanoseconds:
		return t.UnixNano()
	case Microseconds:
		return t.UnixMicro()
	case Milliseconds:
		return t.UnixMilli()
	case Seconds:
		return t.Unix()
	default:
		return t.UnixNano()
	}
}


func(m *memoryMetric)encodeAllDataPoints(encoder *XORChunk)error{
	// merge sort
	sort.Slice(m.outOfOrderPoints, func(i, j int) bool {
		return m.outOfOrderPoints[i].Timestamp < m.outOfOrderPoints[j].Timestamp
	})

	var oi, pi int
	var point *Sample
	for oi < len(m.outOfOrderPoints) && pi < len(m.points){
		if m.outOfOrderPoints[oi].Timestamp < m.points[pi].Timestamp{
			point = m.outOfOrderPoints[oi]
			oi++
		}else {
			point = m.points[pi]
			pi ++
		}
		if err := encoder.Append(point.Timestamp, point.Value);err != nil{
			return err
		}
	}
	for oi < len(m.outOfOrderPoints) {
		point = m.outOfOrderPoints[oi]
		if err := encoder.Append(point.Timestamp, point.Value);err != nil{
			return err
		}
	}
	for pi < len(m.points){
		point = m.points[pi]
		if err := encoder.Append(point.Timestamp, point.Value);err != nil{
			return err
		}
	}
	return nil
}
