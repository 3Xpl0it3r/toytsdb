package toytsdb

import (
	"path"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)


var _ Appender = new(MemPartition)

type MemPartition struct {
	metrics   sync.Map
	numPoints int64

	minT int64
	maxT int64

	// write ahead log
	wal Wal
	// The timestamp range of partition after which they get persisted
	partitionDuration  int64
	timestampPrecision TimestampPrecision
	once               sync.Once

	labelIndex labelIndex
}

func (m *MemPartition) Add(labels Labels,timestamp int64, value float64) {

	if  timestamp <= 0 ||atomic.LoadInt64(&m.minT) > timestamp{
		// illegal value, drop it
		return
	}
	m.once.Do(func() {
		if timestamp < m.minTimestamp(){
			atomic.StoreInt64(&m.minT, timestamp)
		}
	})

	if err := m.wal.append(nil);err != nil{
		return
	}
	ref := labels.Hash()
	m.labelIndex.AddLabels(labels, ref)
	mt := m.getMetric(ref)
	// if timestamp is zero
	mt.insertPoint(&Sample{Timestamp: timestamp, Value: value})
	if atomic.LoadInt64(&m.maxT) < timestamp{
		atomic.StoreInt64(&m.maxT, timestamp)
	}
}

func (m *MemPartition) Commit() error {
	panic("implement me")
}

func (m *MemPartition) Rollback() error {
	panic("implement me")
}

type labelIndex map[Label][]uint64

func(li labelIndex)AddLabels(ls Labels, ref uint64){
	for _, label := range ls{
		li.addLabel(label, ref)
	}
}

func (li labelIndex) addLabel(l Label, ref uint64) {
	refList, ok := li[l]
	if !ok {
		li[l] = []uint64{ref}
	}
	refList = append(refList, ref)
	for i := len(refList) - 1; i >= 1; i-- {
		if refList[i-1] > refList[i] {
			refList[i-1] = refList[i-1] ^ refList[i]
			refList[i] = refList[i-1] ^ refList[i]
			refList[i-1] = refList[i-1] ^ refList[i]
		}
	}
}

func newMemoryPartition(dbDir string, walBufSize int, partitionDuration time.Duration, precision TimestampPrecision) (*MemPartition, error) {
	memPartition := &MemPartition{
		timestampPrecision: precision,
		labelIndex:         map[Label][]uint64{},
	}

	if walBufSize == 0 {
		memPartition.wal = &nopWal{}
	} else {
		if wal, err := newDiskWAL(path.Join(dbDir, "wal"), walBufSize); err != nil {
			return nil, err
		} else {
			memPartition.wal = wal
		}
	}

	switch precision {
	case Nanoseconds:
		memPartition.partitionDuration = partitionDuration.Nanoseconds()
	case Microseconds:
		memPartition.partitionDuration = partitionDuration.Microseconds()
	case Milliseconds:
		memPartition.partitionDuration = partitionDuration.Milliseconds()
	case Seconds:
		memPartition.partitionDuration = int64(partitionDuration.Seconds())
	default:
		memPartition.partitionDuration = partitionDuration.Nanoseconds()
	}
	return memPartition, nil
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
	return m.maxTimestamp()-m.minTimestamp()+1 < m.partitionDuration
}

func (m *MemPartition) getMetric(ref uint64) *memoryMetric {
	// 使用并发map来实现， prometheues 里面采用了分片锁来提高并发行
	// todo 更换成分片锁
	value, ok := m.metrics.Load(ref)
	if !ok {
		value = &memoryMetric{
			name:             ref,
			points:           make([]*Sample, 0, 1000),
			outOfOrderPoints: make([]*Sample, 0),
		}
		m.metrics.Store(ref, value)
	}
	return value.(*memoryMetric)
}

func (m *MemPartition) clean() error {
	runtime.GC()
	return nil
}

func (m *MemPartition) expired() bool {
	return false
}

type memoryMetric struct {
	name         uint64
	size         int64
	minTimestamp int64
	maxTimestamp int64

	points           []*Sample
	outOfOrderPoints []*Sample
	mu               sync.RWMutex
}

func (m *memoryMetric) insertPoint(point *Sample) {
	size := atomic.LoadInt64(&m.size)
	m.mu.Lock()
	defer m.mu.Unlock()

	if size == 0 {
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.minTimestamp, point.Timestamp)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}

	// Insert point in order
	if m.points[size-1].Timestamp < point.Timestamp {
		m.points = append(m.points, point)
		atomic.SwapInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}
	m.outOfOrderPoints = append(m.outOfOrderPoints, point)
}

func (m *memoryMetric) selectPoints(start, end int64) []*Sample {
	size := atomic.LoadInt64(&m.size)
	minTimestamp := atomic.LoadInt64(&m.minTimestamp)
	maxTimestamp := atomic.LoadInt64(&m.maxTimestamp)
	var startIdx, endIdx int
	if end <= minTimestamp {
		return []*Sample{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if start <= minTimestamp {
		startIdx = 0
	} else {
		startIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp >= start
		})
	}

	if end >= maxTimestamp {
		endIdx = int(size)
	} else {
		endIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp < end
		})
	}
	return m.points[startIdx:endIdx]
}

func toUnix(t time.Time, precision TimestampPrecision) int64 {
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

func (m *memoryMetric) encodeAllDataPoints(encoder *XORChunk) error {
	// merge sort
	sort.Slice(m.outOfOrderPoints, func(i, j int) bool {
		return m.outOfOrderPoints[i].Timestamp < m.outOfOrderPoints[j].Timestamp
	})
	var oi, pi int
	var point *Sample
	for oi < len(m.outOfOrderPoints) && pi < len(m.points) {
		if m.outOfOrderPoints[oi].Timestamp < m.points[pi].Timestamp {
			point = m.outOfOrderPoints[oi]
			oi++
		} else {
			point = m.points[pi]
			pi++
		}
		if err := encoder.Append(point.Timestamp, point.Value); err != nil {
			return err
		}
	}
	for ; oi < len(m.outOfOrderPoints); oi++ {
		point = m.outOfOrderPoints[oi]
		if err := encoder.Append(point.Timestamp, point.Value); err != nil {
			return err
		}

	}
	for ; pi < len(m.points); pi++ {
		point = m.points[pi]
		if err := encoder.Append(point.Timestamp, point.Value); err != nil {
			return err
		}
	}
	return nil
}
