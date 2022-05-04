//
package toytsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	dataFileName = "data"
	metaFileName = "BlockMeta.json"
)

type DiskPartition struct {
	dirPath string
	meta BlockMeta
	f *os.File
	mappedFile []byte
	
	retention time.Duration
	
}

func (d *DiskPartition) expired() bool {
	diff := time.Since(d.meta.CreateAt)
	if diff > d.retention{
		return true
	}
	return false
}

type BlockMeta struct {
	MinTimestamp int64 `json:"minTimestamp"`
	MaxTimestamp int64 `json:"maxTimestamp"`
	NumDataPoints int `json:"numDataPoints"`
	Metrics map[string]diskMetric `json:"metrics"`
	
	CreateAt time.Time `json:"createAt"`
}


type diskMetric struct {
	Name string `json:"name"`
	Offset int64 `json:"offset"`
	MinTimestamp int64 `json:"min_timestamp"`
	MaxTimestamp int64 `json:"max_timestamp"`
	NumDataPoints int64 `json:"num_data_points"`
}

func openDiskPartition(dirPath string, retention time.Duration)(partition, error){
	if dirPath == ""{
		return nil, fmt.Errorf("dir path is required")
	}
	f, err := os.Open(filepath.Join(dirPath, dataFileName))
	if err != nil{
		return nil, fmt.Errorf("failed to read data: %w",err)
	}
	defer f.Close()
	info,err := f.Stat()
	if err != nil{
		return nil, fmt.Errorf("failed to fetch file info: %w",err)
	}
	if info.Size() == 0{
		return nil, fmt.Errorf("no data points")
	}
	mapped, err := Mmap(int(f.Fd()), int(info.Size()))
	if err != nil{
		return nil, fmt.Errorf("failed to perform mmap: %w", err)
	}

	m := BlockMeta{}
	mf,err := os.Open(filepath.Join(dirPath, metaFileName))
	if err != nil{
		return nil, fmt.Errorf("failed to read metadata :%w", err)
	}
	defer mf.Close()
	decoder := json.NewDecoder(mf)
	if err := decoder.Decode(&m);err != nil{
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return &DiskPartition{
		dirPath:             dirPath,
		meta:                m,
		f:                   f,
		mappedFile:          mapped,
		retention: retention,
	}, nil
}



func (d DiskPartition) insertRows(rows []Row) (outdatedRows []Row, err error) {
	return nil, fmt.Errorf("cannot insert rows to disk")
}

func (d DiskPartition) selectDataPoints(metrics string, labels []Label, start, end int64) ([]*Sample, error) {
	if d.expired(){
		return nil, fmt.Errorf("this disk partition has expired")
	}
	name := marshalMetricName(metrics, labels)
	mt,ok := d.meta.Metrics[name]
	if !ok {
		return nil, fmt.Errorf("NoDataPoint")
	}
	r := bytes.NewReader(d.mappedFile)

	if _, err := r.Seek(mt.Offset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	decoder, err := newSeriesDeocder(r)
	if err !=nil{
		return nil, fmt.Errorf("failed to generate decoder")
	}



	if err != nil{
		return nil, err
	}
	points := make([]*Sample, 0, mt.NumDataPoints)
	for i := 0 ;i< int(mt.NumDataPoints);i++{
		sample := &Sample{}
		if err := decoder.decodePoint(sample);err !=nil{
			return nil, fmt.Errorf("failed to decode point %w", err)
		}
		if sample.Timestamp < start{
			continue
		}
		if sample.Timestamp>= end{
			break
		}
		points = append(points, sample)
	}

	return points, nil
}

func (d DiskPartition) minTimestamp() int64 {
	return d.meta.MinTimestamp
}

func (d DiskPartition) maxTimestamp() int64 {
	return d.meta.MaxTimestamp
}

func (d DiskPartition) size() int {
	return d.meta.NumDataPoints
}

func (d DiskPartition) active() bool {
	return false
}


func(d *DiskPartition)clean()error{
	if err := os.RemoveAll(d.dirPath);err != nil{
		return fmt.Errorf("clean all diskpartition failed %w", err)
	}
	return nil
}