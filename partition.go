package toytsdb

// partition is a chunk of time-series data with the timestamp range
// A partition acts as a fully independent database contains all data
// points for its time range

// The partition's lifecycle is: Writable -> ReadOnly
// *WritAble*
// 	it can be written. Only one partition can be writable within a partition list
// *Readonly"
// 	it can not be written, Partitions will be Readonly if it exceed the partition  range

type partition interface {
	insertRows(rows []Row)(outdatedRows []Row,err error)
	// Read operations
	// selectDataPoints gives back certain metrics data points within the give range
	selectDataPoints(labels Labels, start, end int64)([]*Sample, error)
	// minTimestamp return the minimum Unix timestamp in milliseconds
	minTimestamp()int64
	// maxTimestamp return the maximum Unix timestamp in milliseconds
	maxTimestamp()int64
	// size returns the number of data points
	size()int
	// active meas not only writeable but having the qualities to be head partition
	active()bool
	// gc
	clean()error
	// expired means this partition should be removed
	expired()bool

}
