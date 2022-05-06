package toytsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakePartition struct {
	minT int64
	maxT int64
}

var _ partition = new(fakePartition)

func Test_partitionList_Remove(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl
		target            partition
		wantErr           bool
		wantPartitionList partitionListImpl
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl{},
			wantErr:       true,
		},
		{
			name: "remove the head node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition{
				minT: 1,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 1,
				head: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
			},
		},
		{
			name: "remove the tail node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition{
				minT: 2,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 1,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
				},
			},
		},
		{
			name: "remove the middle node",
			partitionList: func() partitionListImpl {
				third := &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				}
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
					next: third,
				}
				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			target: &fakePartition{
				minT: 2,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 3,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition{
				minT: 3,
			},
			wantPartitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.partitionList.remove(tt.target)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantPartitionList, tt.partitionList)
		})
	}
}

func Test_partitionList_Swap(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl
		old               partition
		new               partition
		wantErr           bool
		wantPartitionList partitionListImpl
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl{},
			wantErr:       true,
		},
		{
			name: "swap the head node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition{
				minT: 1,
			},
			new: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 100,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 2,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
			},
		},
		{
			name: "swap the tail node",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition{
				minT: 2,
			},
			new: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 100,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 100,
					},
				},
			},
		},
		{
			name: "swap the middle node",
			partitionList: func() partitionListImpl {
				third := &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				}
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
					next: third,
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			old: &fakePartition{
				minT: 2,
			},
			new: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 3,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 100,
						},
						next: &partitionNode{
							val: &fakePartition{
								minT: 3,
							},
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl {
				second := &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				}

				first := &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition{
				minT: 100,
			},
			wantPartitionList: partitionListImpl{
				numPartitions: 2,
				head: &partitionNode{
					val: &fakePartition{
						minT: 1,
					},
					next: &partitionNode{
						val: &fakePartition{
							minT: 2,
						},
					},
				},
				tail: &partitionNode{
					val: &fakePartition{
						minT: 2,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.partitionList.swap(tt.old, tt.new)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantPartitionList, tt.partitionList)
		})
	}
}




func (f fakePartition) insertRows(rows []Row) (outdatedRows []Row, err error) {
	return nil, nil
}

func (f fakePartition) selectDataPoints(labels Labels, start, end int64) ([]*Sample, error) {
	return nil, nil
}

func (f fakePartition) minTimestamp() int64 {
	return f.minT
}

func (f fakePartition) maxTimestamp() int64 {
	return f.maxT
}

func (f fakePartition) size() int {
	return 0
}

func (f fakePartition) active() bool {
	return true
}

func (f fakePartition) clean() error {
	return nil
}

func (f fakePartition) expired() bool {
	return true
}
