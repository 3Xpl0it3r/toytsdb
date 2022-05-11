package toytsdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

var (
	partitionDirRegx = regexp.MustCompile(`^p-.+`)
)

// TimestampPrecision represents precision of timestamp
type TimestampPrecision string

const (
	Nanoseconds  TimestampPrecision = "ns"
	Microseconds TimestampPrecision = "us"
	Milliseconds TimestampPrecision = "ms"
	Seconds      TimestampPrecision = "s"

	defaultPartitionDuration      = 1 * time.Hour
	defaultPartitionRetention     = 30 * 24 * time.Hour
	defaultTimestampPrecision     = Nanoseconds
	defaultWriteablePartitionsNum = 2
	defaultWALBufferSize          = 4096
	checkExpiredInterval          = time.Hour
	defaultWALDirName             = "wal"
)

type Appender interface {
	// Add timestamp and value into appender
	Add(Labels ,int64,  float64)
	// Commit submit the collected samples
	Commit()error
	// Rollback rolls all modifications into memory partition
	Rollback()error
}

type Row struct {
	// An Optional key-value properties of futhres detailed identification
	Labels Labels
	Sample
}

// DataPoint represents a data point , the smallest unit of time seies data
type Sample struct {
	// The actual value, This field must be set
	Value float64
	// Unix timestamp
	Timestamp int64
}

// Option is an optional setting for NewStorage
type Option func(*TSDB)

type TSDB struct {
	blocks []*DiskPartition
	head   *MemPartition

	partitionDuration  time.Duration
	retention          time.Duration
	compact            chan struct{}
	timestampPrecision TimestampPrecision
	dataPath           string
	writeTimeout       time.Duration
	walBufferedSize    int

	logger        Logger
	workerLimitCh chan struct{}

	wg     sync.WaitGroup
	stopCh chan struct{}
}

func OpenTSDB(dir string, opts ...Option) (*TSDB, error) {
	if err := os.MkdirAll(dir, fs.ModePerm); err != nil && !errors.Is(os.ErrExist, err) {
		return nil, fmt.Errorf("create db directory failed: %s", err.Error())
	}
	db := &TSDB{
		blocks:             make([]*DiskPartition, 0),
		wg:                 sync.WaitGroup{},
		stopCh:             make(chan struct{}),
		partitionDuration:  defaultPartitionDuration,
		retention:          defaultPartitionRetention,
		timestampPrecision: defaultTimestampPrecision,
		walBufferedSize:    defaultWALBufferSize,
		dataPath:           dir,
		compact: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(db)
	}

	if err := os.Mkdir(db.dataPath, fs.ModePerm); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("failed to make data directory %v: %w", db.dataPath, err)
	}

	// write ahead log
	walDir := filepath.Join(db.dataPath, "wal")

	if err := db.reloadBlock();err != nil{
		return nil, err
	}

	if head, err := newMemoryPartition(db.dataPath, db.walBufferedSize, db.partitionDuration, db.timestampPrecision); err != nil {
		return nil, err
	} else {
		db.head = head
	}
	if err := db.recoverWAL(walDir); err != nil {
		return nil, fmt.Errorf("failed to recovery wal log %w", err)
	}
	go db.run()
	return db, nil
}

type dbAppender struct {
	Appender
	db *TSDB
}

func(db *TSDB)Appender()Appender{
	return &dbAppender{
		Appender: db.head,
		db:       nil,
	}
}

func (db *TSDB) Select(labels Labels, start, end int64) ([]*Sample, error) {
	if labels.Empty() {
		return nil, fmt.Errorf("labels is empty")
	}
	if start >= end {
		return nil, fmt.Errorf("the given start it greater than the end")
	}
	points := make([]*Sample, 0)
	// iterate over all partitions from the neweast one
	if samples, err := db.head.selectDataPoints(labels, start, end); err != nil {
		return nil, err
	} else {
		points = append(points, samples...)
	}
	for _, block := range db.blocks {
		if ps, err := block.selectDataPoints(labels, start, end); err != nil {
			return nil, err
		} else {
			points = append(points, ps...)
		}
	}

	if len(points) == 0 {
		return nil, fmt.Errorf("ErrNoDataPoints")
	}
	return points, nil
}

func (db *TSDB) Close() error {
	// todo
	db.wg.Wait()

	if err := db.flushPartitions(); err != nil {
		return fmt.Errorf("failed to close TSDB : %w", err)
	}
	db.removeExpiredPartitions()
	return nil
}

// flushPartitions persists all in-memory partitions ready to persisted
// For the in-memory mode, just removes it from the partition list
func (db *TSDB) flushPartitions() error {

	dir := filepath.Join(db.dataPath, fmt.Sprintf("b-%d-%d", db.head.minTimestamp(), db.head.maxTimestamp()))
	if err := db.flush(dir); err != nil {
		return fmt.Errorf("failed to compact memory parition into %s: %w", dir, err)
	}

	return nil
}

// flush compacts the data points in the give partition and flushes them to the given directory
func (db *TSDB) flush(dirPath string) error {
	if dirPath == "" {
		return fmt.Errorf("dir path is requierd ")
	}
	if err := os.MkdirAll(dirPath, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to make directory %q: %w", dirPath, err)
	}
	f, err := os.Create(filepath.Join(dirPath, dataFileName))
	if err != nil {
		return fmt.Errorf("failed to create faile %q: %w", dirPath, err)
	}
	defer f.Close()

	encoder := NewXorChunk(f)
	metrics := map[uint64]diskMetric{}
	db.head.metrics.Range(func(key, value interface{}) bool {
		mt, ok := value.(*memoryMetric)
		if !ok {
			fmt.Printf("unknown value found\n")
			return false
		}
		offset, err := f.Seek(io.SeekStart, 1)
		if err != nil {
			fmt.Printf("failed to set file offset of metric %q: %v\n", mt.name, err)
			return false
		}

		// Compress data points for each metric
		if err := mt.encodeAllDataPoints(encoder); err != nil {
			return false
		}

		metrics[mt.name] = diskMetric{
			Name:          mt.name,
			Offset:        offset,
			MinTimestamp:  mt.minTimestamp,
			MaxTimestamp:  mt.maxTimestamp,
			NumDataPoints: mt.size + int64(len(mt.outOfOrderPoints)),
		}
		return true
	})

	if err := encoder.flush(); err != nil {
		return err
	}

	b, err := json.Marshal(&BlockMeta{
		MinTimestamp:  db.head.minTimestamp(),
		MaxTimestamp:  db.head.maxTimestamp(),
		NumDataPoints: db.head.size(),
		Metrics:       metrics,
		CreateAt:      time.Now(),
	})

	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	metaPath := filepath.Join(dirPath, metaFileName)
	if err := os.WriteFile(metaPath, b, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to write metadata to %s: %w", metaPath, err)
	}
	if block, err := openDiskPartition(dirPath, db.retention); err != nil {
		fmt.Printf("open DiskPartition failed ;%v", err)
	} else {
		db.blocks = append(db.blocks, block)
	}
	return nil
}

func (db *TSDB) removeExpiredPartitions() {
	for index, block := range db.blocks {
		if block.expired() {
			db.blocks = db.blocks[index:]
		}
	}
}

func (db *TSDB) recoverWAL(walDir string) error {
	reader, err := newDiskWALReader(walDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := reader.readAll(); err != nil {
		return fmt.Errorf("failed to read wal: %w", err)
	}
	if len(reader.rowsToInsert) == 0 {
		return nil
	}
	for _, row := range reader.rowsToInsert{
		db.Appender().Add(row.Labels, row.Timestamp, row.Value)
	}

	return nil
}

func (db *TSDB) run() {
	defer db.Close()
	for true {
		select {
		case <-db.stopCh:
			return
		case <-db.compact:
			db.Compact()
		}
	}
}

func (db *TSDB)Compact(){
	// todo compact
}

//
func(db *TSDB)reloadBlock()error{
	dirs, err := os.ReadDir(db.dataPath)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}

	var needAdded []fs.DirEntry = []fs.DirEntry{}
	var newBlocks []*DiskPartition = []*DiskPartition{}
	var isOpened bool
	for _, e := range dirs {
		isOpened = false
		if !e.IsDir() || !partitionDirRegx.MatchString(e.Name()) {
			continue
		}
		for _, b := range db.blocks{
			if !b.expired(){
				newBlocks = append(newBlocks, b)
			}
			if e.Name() == b.f.Name(){
				isOpened = true
				break
			}
		}
		if isOpened{
			continue
		}
		needAdded = append(needAdded, e)
	}

	for _,d := range needAdded{
		block,err := openDiskPartition(d.Name(), db.retention)
		if err != nil{
			return fmt.Errorf("open diskpartition failed")
		}
		newBlocks = append(newBlocks, block)
	}
	db.blocks = append(db.blocks[0:], newBlocks...)
	sort.Slice(db.blocks, func(i, j int) bool {
		return db.blocks[i].minTimestamp() < db.blocks[j].minTimestamp()
	})
	return nil
}

