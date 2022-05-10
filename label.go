package toytsdb

import (
	"bytes"
	"github.com/cespare/xxhash"
	"strconv"
)

const (
	sep = '\xff'
)

type Labels []Label

type Label struct {
	Name  string
	Value string
}

func (ls Labels) String() string {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, l := range ls {
		if i >= 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
	}
	b.WriteByte('}')
	return b.String()
}

func (ls Labels) Hash() uint64 {
	b := make([]byte, 0, 1024)
	for _, v := range ls {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

func (ls Labels) Empty() bool {
	if len(ls) == 0 {
		return true
	}
	return false
}

func (ls Labels) Equals(o Labels) bool {
	if len(ls) != len(o) {
		return false
	}
	for i, l := range ls {
		if o[i] != l {
			return false
		}
	}
	return true
}
