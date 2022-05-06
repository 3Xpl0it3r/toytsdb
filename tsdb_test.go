package toytsdb

//
//func Test_storage_Select(t *testing.T){
//	tests := []struct{
//		name string
//		storage TSBD
//		metric string
//		labels []Label
//		start int64
//		end int64
//		want []*Sample
//		wantErr bool
//	}{
//		{
//			name: "select from single partition",
//			metric: "metric1",
//			start: 1,
//			end: 5,
//			storage: func() TSBD{
//				part1 := newMemoryPartition(nil, 1 * time.Hour, Seconds)
//				_,err := part1.insertRows([]Row{
//					{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 1}},
//					//{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 2}},
//					//{Labels: []Label{{Name: "__name__", Value: "metric1"}}, Sample: Sample{Timestamp: 3}},
//				})
//				if err != nil{
//					panic(err)
//				}
//				list := newPartitionList()
//				list.insert(part1)
//
//				return TSBD{partitionList: list, workerLimitCh: make(chan struct{}), }
//			}(),
//			want: []*Sample{
//				{Timestamp: 1},
//				//{Timestamp: 1}, {Timestamp: 2}, {Timestamp: 3},
//			},
//		},
//	}
//
//	for _,tt := range tests{
//		t.Run(tt.name, func(t *testing.T) {
//			got,err := tt.storage.Select(tt.labels, tt.start, tt.end)
//			fmt.Printf("debug %v\n", got)
//			assert.Equal(t, tt.wantErr, err != nil)
//			assert.Equal(t, tt.want, got)
//		})
//	}
//}