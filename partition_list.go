package toytsdb

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// partitionList represents a linked for partitions
// Each partition is arranged in order order of newest to oldlist

// Head and its next partitions must be writable to accept out-of-order data points
// even if it's inactive
type partitionList interface {
	insert(partition partition)

	remove(partition partition)error

	// swap replaces the old partition with the new one
	swap(old, new partition)error

	getHead()*partitionNode
	// size returns the number of partitions of its list
	size()int

	String()string
}


type partitionListImpl struct {
	numPartitions int64
	head *partitionNode
	tail *partitionNode
	mu sync.RWMutex
}

func newPartitionList()partitionList{
	return &partitionListImpl{}
}




func(p *partitionListImpl)getHead()*partitionNode{
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.head
}
func(p *partitionListImpl)setHead(node *partitionNode){
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = node
}

func(p *partitionListImpl)setTail(node *partitionNode){
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tail = node
}

func(p *partitionListImpl)insert(partition partition){
	p.mu.Lock()
	p.head = &partitionNode{
		val:  partition,
		next: p.head,
		mu:   sync.Mutex{},
	}
	p.mu.Unlock()
	atomic.AddInt64(&p.numPartitions, 1)
}

func(p *partitionListImpl)remove(target partition)error{
	if p.size() <= 0{
		// if link list is empty ,then return error
		return fmt.Errorf("empty parition")
	}

	var prev,cur *partitionNode

	for cur = p.head; cur!= nil && !samePartitions(cur.value(), target) ; cur = cur.next{
		prev = cur
	}

	if cur == nil{
		return fmt.Errorf("target is not in partition list")
	}

	switch {
	case prev == nil:
		p.setHead(cur.getNext())
	case cur.getNext() == nil:
		prev.setNext(nil)
		p.setTail(prev)
	default:
		prev.setNext(cur.getNext())
	}
	atomic.AddInt64(&p.numPartitions, -1)
	if err := cur.value().clean();err != nil{
		return fmt.Errorf("clean partition failed %w", err)
	}
	return nil
}

func(p *partitionListImpl)swap(old,new partition)error{
	if p.size() <= 0{
		return fmt.Errorf("empty partition")
	}
	var cur *partitionNode

	for cur = p.head;cur!= nil && !samePartitions(cur.value(),old); cur = cur.next{}
	if cur == nil{
		return fmt.Errorf("target is not int the partition list")
	}
	cur.setValue(new)
	return nil
}


func (p *partitionListImpl)String()string{
	b := &strings.Builder{}
	var cur *partitionNode
	for cur = p.head; cur!= nil ; cur = cur.getNext(){
		switch cur.value().(type) {
		case *MemPartition:
			b.WriteString("[Memory Partition]")
		case *DiskPartition:
			b.WriteString("[Disk partition]")
		default:
			b.WriteString("[Unknown partition]")
		}
		b.WriteString("->")
	}
	return strings.TrimSuffix(b.String(), "->")
}

func(p *partitionListImpl)size()int{
	return int(atomic.LoadInt64(&p.numPartitions))
}

func samePartitions(x,y partition)bool{
	result :=  x.minTimestamp() == y.minTimestamp()
	return result
}

// partitionNode is a wrapper a partition to hold the datapoint
type partitionNode struct {
	val partition
	next *partitionNode
	mu sync.Mutex
}

func(p *partitionNode)value()partition{
	return p.val
}
func(p *partitionNode)setValue(value partition){
	p.mu.Lock()
	defer p.mu.Unlock()
	p.val = value
}
func(p *partitionNode)setNext(node *partitionNode){
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = node
}
func(p *partitionNode)getNext()*partitionNode{
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.next
}