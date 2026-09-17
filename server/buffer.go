package main

import "sync"

type RingBuffer struct {
	mu    sync.RWMutex
	lines [][]byte
	size  int
	head  int
}

func NewRingBuffer(size int) *RingBuffer {
	if size <= 0 {
		size = 1
	}
	return &RingBuffer{
		lines: make([][]byte, 0, size),
		size:  size,
	}
}

func (rb *RingBuffer) Write(line []byte) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if len(rb.lines) < rb.size {
		rb.lines = append(rb.lines, line)
		return
	}
	rb.lines[rb.head] = line
	rb.head = (rb.head + 1) % rb.size
}

func (rb *RingBuffer) GetAll() [][]byte {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([][]byte, len(rb.lines))
	if len(rb.lines) < rb.size {
		copy(result, rb.lines)
		return result
	}
	n := copy(result, rb.lines[rb.head:])
	copy(result[n:], rb.lines[:rb.head])
	return result
}

func (rb *RingBuffer) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return len(rb.lines)
}
