package toytsdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOpenDiskPartition(t *testing.T){
	tests := []struct{
		name string
		dirPath string
		retention time.Duration
		want partition
		wantErr bool
	}{
		{
			name: "empty dir name given",
			dirPath: "",
			retention: 24 * time.Hour,
			wantErr: true,
		},
		{
			name: "non-existent dir given",
			dirPath: "./non-existent-dir",
			retention: 24 * time.Hour,
			wantErr: true,
		},
	}
	for _, tt := range tests{
		t.Run(tt.name, func(t *testing.T) {
			got, err := openDiskPartition(tt.dirPath, tt.retention)
			fmt.Printf("Debug %v| Err: %v\n", got, err)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want,got)
		})
	}
}