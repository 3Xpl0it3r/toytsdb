package toytsdb

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stripeSize = 1 << 14
	stripeMask = stripeSize - 1

	poolSize = 1 << 9
)

var _ Appender = new(MemPartition)

type MemPartition struct {
	series *stripeSeries

	samplePool []RefSample
	seriesPool []RefSeries

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

	mut sync.Mutex
}

func newMemoryPartition(dbDir string, wal Wal, partitionDuration time.Duration, precision TimestampPrecision) (*MemPartition, error) {
	memPartition := &MemPartition{
		timestampPrecision: precision,
		labelIndex:         map[Label][]uint64{},
		series:             newStripeSeries(),
		samplePool: make([]RefSample, 0, poolSize),
		seriesPool: make([]RefSeries, 0, poolSize),
	}

	if wal != nil {
		memPartition.wal = &nopWal{}
	} else {
		memPartition.wal = wal
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


// Add only add sample and series into buffer cache, for it is not committed
// if committed, then the series and samples will be recorder into chunks and wal
func (m *MemPartition) Add(labels Labels, timestamp int64, value float64) (uint64, error) {

	if timestamp <= 0 || atomic.LoadInt64(&m.minT) > timestamp {
		// illegal value, drop it
		return 0, fmt.Errorf("sample timestamp is validate")
	}
	m.once.Do(func() {
		if timestamp < m.minTimestamp() {
			atomic.StoreInt64(&m.minT, timestamp)
		}
	})
	ref := labels.Hash()
	m.labelIndex.AddLabels(labels, ref)

	mt := m.getOrCreateSeries(ref)
	m.samplePool = append(m.samplePool, RefSample{
		Ref: ref,
		T:   timestamp,
		V:   value,
		series: mt,
	})
	m.seriesPool = append(m.seriesPool, RefSeries{
		Ref:    ref,
		Labels: labels,
		series: mt,
	})
	return ref, nil
}

func (m *MemPartition) Commit() error {
	m.mut.Lock()
	defer m.mut.Unlock()
	for i,sample := range m.samplePool{
		s := Sample{
			Value:     sample.V,
			Timestamp: sample.T,
		}
		m.samplePool[i].series.insertPoint(&s)
	}
	return nil
}

func (m *MemPartition) Rollback()  {
	m.seriesPool = m.seriesPool[0:]
	m.samplePool = m.samplePool[0:]
}

type labelIndex map[Label][]uint64

func (li labelIndex) AddLabels(ls Labels, ref uint64) {
	for _, label := range ls {
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


func (m *MemPartition) selectDataPoints(labels Labels, start, end int64) ([]*Sample, error) {
	mt := m.getOrCreateSeries(labels.Hash())
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

func (m *MemPartition) getOrCreateSeries(ref uint64) *memSeries {
	// 使用并发map来实现， prometheus 里面采用了分片锁来提高并发行
	m.series.locks[ref&stripeMask].Lock()
	defer m.series.locks[ref&stripeMask].Unlock()
	ms, ok := m.series.series[ref&stripeMask][ref]
	if !ok {
		ms = newMemSeries(ref)
		m.series.series[ref&stripeMask][ref] = ms
	}
	return ms
}

func(m *MemPartition)getSeries(ref uint64)(*memSeries, error){
	m.series.locks[ref & stripeMask].Lock()
	defer m.series.locks[ref &stripeMask].Unlock()
	ms, ok := m.series.series[ref &stripeMask][ref]
	if !ok {
		return nil, fmt.Errorf("no data found")
	}
	return ms, nil
}

func (m *MemPartition) clean() error {
	runtime.GC()
	return nil
}

func (m *MemPartition) expired() bool {
	return false
}

type memSeries struct {
	sync.Mutex
	ref          uint64
	size         int64
	minTimestamp int64
	maxTimestamp int64

	points []*Sample
}

func newMemSeries(ref uint64) *memSeries {
	return &memSeries{
		ref:    ref,
		points: make([]*Sample, 0, 1000),
	}
}

// insertPoints is coroutine safety, so caller should not be lock it before call it
func (m *memSeries) insertPoint(point *Sample) {
	size := atomic.LoadInt64(&m.size)

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
}

func (m *memSeries) selectPoints(start, end int64) []*Sample {
	size := atomic.LoadInt64(&m.size)
	minTimestamp := atomic.LoadInt64(&m.minTimestamp)
	maxTimestamp := atomic.LoadInt64(&m.maxTimestamp)
	var startIdx, endIdx int
	if end <= minTimestamp {
		return []*Sample{}
	}

	m.Lock()
	defer m.Unlock()
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

func (m *memSeries) encodeAllDataPoints(encoder *XORChunk) error {
	// merge sort
	for i := 0; i < len(m.points); i++ {
		if err := encoder.Append(m.points[i].Timestamp, m.points[i].Value); err != nil {
			return err
		}
	}
	return nil
}

// stripeSeries 用来实现分片锁来降低锁的粒度
type stripeSeries struct {
	locks  [stripeSize]stripeLock
	series [stripeSize]map[uint64]*memSeries
}

type stripeLock struct {
	sync.RWMutex
	_ [40]byte
}

func newStripeSeries() *stripeSeries {
	ss := stripeSeries{
		locks:  [stripeSize]stripeLock{},
		series: [stripeSize]map[uint64]*memSeries{},
	}
	for i := 0; i< stripeSize; i++{
		ss.locks[i] = stripeLock{}
		ss.series[i] = map[uint64]*memSeries{}
	}
	return &ss
}

