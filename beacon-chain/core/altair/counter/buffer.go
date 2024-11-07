package buffer

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Buffer struct {
	mu           sync.Mutex
	ErrBuffer    []error
	SuccessCount int64
	ErrorCount   int64
}

func New() *Buffer {
	return &Buffer{
		ErrBuffer: make([]error, 0),
	}
}

func (b *Buffer) AddError(item error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ErrBuffer = append(b.ErrBuffer, item)
}

// Flush clears the error buffer and returns its contents
func (b *Buffer) FlushErrorBuffer() []error {
	b.mu.Lock()
	defer b.mu.Unlock()

	contents := make([]error, len(b.ErrBuffer))
	copy(contents, b.ErrBuffer)

	b.ErrBuffer = []error{}
	return contents
}

func (b *Buffer) IncrementSuccess() {
	atomic.AddInt64(&b.SuccessCount, 1)
}

func (b *Buffer) IncrementError() {
	atomic.AddInt64(&b.ErrorCount, 1)
}

// ResetCounts counters
func (b *Buffer) resetCounts() {
	atomic.StoreInt64(&b.SuccessCount, 0)
	atomic.StoreInt64(&b.ErrorCount, 0)
}

func (b *Buffer) GetSuccessCount() int64 {
	return atomic.LoadInt64(&b.SuccessCount)
}

func (b *Buffer) GetErrorCount() int64 {
	return atomic.LoadInt64(&b.ErrorCount)
}

type Aggregation struct {
	Errors       []error
	SuccessCount int64
	ErrorCount   int64
}

func (b *Buffer) GetAggregation() string {
	aggr := Aggregation{
		SuccessCount: b.GetSuccessCount(),
		ErrorCount:   b.GetErrorCount(),
		Errors:       b.FlushErrorBuffer(),
	}
	b.resetCounts()
	return aggr.FormatOutput()
}

func (a *Aggregation) FormatOutput() string {
	result := "Aggregation Summary:\n"
	result += fmt.Sprintf("  Successes: %d\n", a.SuccessCount)
	result += fmt.Sprintf("  Errors: %d\n", a.ErrorCount)

	if len(a.Errors) > 0 {
		result += "  Error Details:\n"
		for i, err := range a.Errors {
			result += fmt.Sprintf("    %d: %v\n", i+1, err)
		}
	}

	return result
}
