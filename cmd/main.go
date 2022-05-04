package main

import (
	"fmt"
	"github.com/3Xpl0it3r/toytsdb"
)

func main(){
	s,err := toytsdb.OpenTSDB("pdata")
	if err != nil{
		panic(err)
	}
	_ = s.InsertRows([]toytsdb.Row{
		{
			Metric: "metric1",
			Sample: toytsdb.Sample{Timestamp: 1600000000, Value: 0.1},
		},
	})
	points, _ := s.Select("metric1", nil, 1600000000, 1600000001)
	for _, p := range points {
		fmt.Printf("timestamp: %v, value: %v\n", p.Timestamp, p.Value)
		// => timestamp: 1600000000, value: 0.1
	}
	s.Close()
}