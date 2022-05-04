package toytsdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

type walOperation byte

const (
	operationInsert walOperation = iota
)

type RefSeries struct {
	Ref uint64
	Labels []Label
}

type RefSample struct {
	Ref uint64
	T int64
	V float64
}

// WAL is a write ahead log that is used to log series and log samples	
type Wal interface {
	LogSeries([]RefSeries)error
	LogSamples([]RefSample)error
	Truncate(mint int64, keep func(uint64)bool)error
	Close()error
	append(op walOperation, rows []Row) error
	flush() error
	punctuate() error
	removeOldest() error
	removeAll() error
	refresh() error
}

type diskWal struct {
	dir string
	w   *bufio.Writer

	fd           *os.File
	bufferedSize int
	mu           sync.Mutex

	index uint32
}

func (d *diskWal) LogSeries(series []RefSeries) error {
	// todo
	return nil
}

func (d *diskWal) LogSamples(samples []RefSample) error {
	// todo
	return nil
}

func (d *diskWal) Truncate(mint int64, keep func(uint64) bool) error {
	// todo
	return nil
}

func (d *diskWal) Close() error {
	// todo
	return nil
}

// append appends the given entry to the end of file via the file description it has
func (d *diskWal) append(op walOperation, rows []Row) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch op {
	case operationInsert:
		for _, row := range rows {
			if err := d.w.WriteByte(byte(op)); err != nil {
				return fmt.Errorf("failed to write operation %w\n", err)
			}
			name := marshalMetricName(row.Metric, row.Labels)
			lBuf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(lBuf, uint64(len(name)))
			if _, err := d.w.Write(lBuf[:n]); err != nil {
				return fmt.Errorf("failed to write the length metrics name :%w", err)
			}
			//write metrics name
			if _, err := d.w.WriteString(name); err != nil {
				return fmt.Errorf("failed to write metrics string %w", err)
			}

			tsBuf := make([]byte, 0, binary.MaxVarintLen64)
			n = binary.PutVarint(tsBuf, row.Sample.Timestamp)
			if _, err := d.w.Write(tsBuf[:n]); err != nil {
				return fmt.Errorf("failed to write timestamp")
			}
			vBuf := make([]byte, 0, binary.MaxVarintLen64)
			n = binary.PutUvarint(vBuf, math.Float64bits(row.Sample.Value))
			if _, err := d.w.Write(vBuf[:n]); err != nil {
				return fmt.Errorf("failed to write the value")
			}
		}
	default:
		return fmt.Errorf("unknown operation %v", op)
	}
	if d.bufferedSize == 0 {
		return d.flush()
	}
	return nil
}

// flush flushed all buffered entries to the underline file
func (d *diskWal) flush() error {
	if err := d.w.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer")
	}
	return nil
}

// punctuate set boundary and create a new segment
func (d *diskWal) punctuate() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.flush(); err != nil {
		return err
	}
	if err := d.fd.Close(); err != nil {
		return err
	}
	f, err := d.createSegmentFile(d.dir)
	if err != nil {
		return err
	}
	d.fd = f
	d.w = bufio.NewWriterSize(f, d.bufferedSize)
	return nil
}

// truncatedOldest removes only the oldest segment
func (d *diskWal) removeOldest() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	files, err := os.ReadDir(d.dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL direcrory %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no segment found")
	}
	return os.RemoveAll(filepath.Join(d.dir, files[0].Name()))
}

func (d *diskWal) removeAll() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.fd.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(d.dir); err != nil {
		return fmt.Errorf("failed to remove files unser %q: %w", d.dir, err)
	}
	return os.MkdirAll(d.dir, fs.ModePerm)
}

func (d *diskWal) refresh() error {
	if err := d.removeAll(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := d.createSegmentFile(d.dir)
	if err != nil {
		return err
	}
	d.fd = f
	d.w = bufio.NewWriterSize(f, d.bufferedSize)
	return nil
}

func newDiskWAL(dir string, bufferedSize int) (Wal, error) {
	if err := os.MkdirAll(dir, fs.ModePerm); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("failed to make wal dir: %w", err)
	}
	w := &diskWal{dir: dir, bufferedSize: bufferedSize}
	f, err := w.createSegmentFile(dir)
	if err != nil {
		return nil, err
	}
	w.fd = f
	w.w = bufio.NewWriterSize(f, bufferedSize)
	return w, nil
}

func (d *diskWal) createSegmentFile(dir string) (*os.File, error) {
	name := strconv.Itoa(int(atomic.LoadUint32(&d.index)))
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file failed: %w", err)
	}
	atomic.AddUint32(&d.index, 1)
	return f, nil

}

type walRecord struct {
	op  walOperation
	row Row
}

type diskWALReader struct {
	dir          string
	files        []os.DirEntry
	rowsToInsert []Row
}

func newDiskWALReader(dir string) (*diskWALReader, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read the wal dir %w", err)
	}

	return &diskWALReader{
		dir:          dir,
		files:        files,
		rowsToInsert: make([]Row, 0),
	}, nil
}

func (f *diskWALReader) readAll() error {
	for _, file := range f.files {
		if file.IsDir() {
			return fmt.Errorf("unexpected directory found under the wal directory %w", file.Name())
		}
		fd, err := os.Open(filepath.Join(f.dir, file.Name()))
		if err != nil {
			return fmt.Errorf("failed to open wal segment files %w", err)
		}
		segment := &segment{
			file: fd,
			r:    bufio.NewReader(fd),
		}

		for segment.next() {
			rec := segment.record()
			switch rec.op {
			case operationInsert:
				f.rowsToInsert = append(f.rowsToInsert, rec.row)
			}
		}
		if err := segment.close(); err != nil {
			return err
		}
		if segment.error() != nil {
			return fmt.Errorf("encounter an error while reading wal files %q : %w", file.Name(), segment.error())
		}
	}
	return nil
}

type segment struct {
	file    *os.File
	r       *bufio.Reader
	current walRecord
	err     error
}

func (f *segment) next() bool {
	op, err := f.r.ReadByte()
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		f.err = err
		return false
	}
	switch walOperation(op) {
	case operationInsert:
		metricLen, err := binary.ReadUvarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read the length of metric name %w", err)
			return false
		}
		metrics := make([]byte, int(metricLen))
		if _, err := io.ReadFull(f.r, metrics); err != nil {
			f.err = fmt.Errorf("failed to read the metrics %w", err)
			return false
		}
		// read the timestamp
		ts, err := binary.ReadVarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read timestamp %w", err)
			return false
		}
		val, err := binary.ReadUvarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read values %w", err)
			return false
		}
		f.current = walRecord{
			op: walOperation(op),
			row: Row{
				Metric: string(metrics),
				Sample: Sample{
					Value:     math.Float64frombits(val),
					Timestamp: ts,
				},
			},
		}
	default:
		f.err = fmt.Errorf("unknown operation %v found", op)
		return false
	}
	return true
}

func (f *segment) error() error {
	return f.err
}
func (f *segment) record() *walRecord {
	return &f.current
}
func (f *segment) close() error {
	return f.file.Close()
}

type nopWal struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func (f *nopWal) LogSeries(series []RefSeries) error {
	panic("implement me")
}

func (f *nopWal) LogSamples(samples []RefSample) error {
	panic("implement me")
}

func (f *nopWal) Truncate(mint int64, keep func(uint64) bool) error {
	panic("implement me")
}

func (f *nopWal) Close() error {
	panic("implement me")
}

func (f *nopWal) flush() error {
	return nil
}

func (f *nopWal) punctuate() error {
	return nil
}

func (f *nopWal) removeOldest() error {
	return nil
}

func (f *nopWal) removeAll() error {
	return nil
}

func (f *nopWal) refresh() error {
	return nil
}

var _ Wal = new(nopWal)

func (f *nopWal) append(_ walOperation, _ []Row) error {
	return nil
}
