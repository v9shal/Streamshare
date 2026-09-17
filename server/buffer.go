package main

import (
	"sync"
)

type RingBuffer struct {
	lines [][]byte
	size  int
	head  int
	mu    sync.RWMutex
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		lines: make([][]byte, 0, size),
		size:  size,
		head:  0,
	}
}

func (rb *RingBuffer) Write(line []byte) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.lines) < rb.size {
		rb.lines = append(rb.lines, line)

	} else {
		rb.lines[rb.head] = line
		rb.head = (rb.head + 1) % rb.size
	}
}

func (rb *RingBuffer) GetAll() [][]byte {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([][]byte, len(rb.lines))
	if len(rb.lines) < rb.size {
		copy(result, rb.lines)
	} else {
		n := copy(result, rb.lines[rb.head:])
		copy(result[n:], rb.lines[:rb.head])
	}
	return result
}
