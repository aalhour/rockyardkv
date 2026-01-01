// rate_limiter.go implements a Rate Limiter for I/O operations.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/rate_limiter.h
//   - util/rate_limiter.cc
package rockyardkv

import (
	"sync"
	"time"
)

// RateLimiterMode specifies when rate limiting should be applied.
type RateLimiterMode int

const (
	// RateLimiterModeReadsOnly applies rate limiting only to reads.
	RateLimiterModeReadsOnly RateLimiterMode = iota
	// RateLimiterModeWritesOnly applies rate limiting only to writes (compaction, flush).
	RateLimiterModeWritesOnly
	// RateLimiterModeAllIO applies rate limiting to all I/O.
	RateLimiterModeAllIO
)

// IOPriority specifies the priority of I/O operations.
type IOPriority int

const (
	// IOPriorityLow is for background operations like compaction.
	IOPriorityLow IOPriority = iota
	// IOPriorityHigh is for user reads and writes.
	IOPriorityHigh
	// IOPriorityTotal is the count of priorities.
	IOPriorityTotal
)

// RateLimiter controls the rate of I/O operations.
type RateLimiter interface {
	// Request requests bytes to be written/read.
	// It blocks until enough quota is available.
	Request(bytes int64, priority IOPriority)

	// SetBytesPerSecond dynamically sets the rate limit.
	SetBytesPerSecond(bytesPerSecond int64)

	// GetBytesPerSecond returns the current rate limit.
	GetBytesPerSecond() int64

	// GetTotalBytesThrough returns total bytes passed through the limiter.
	GetTotalBytesThrough(priority IOPriority) int64

	// GetTotalRequests returns total request count.
	GetTotalRequests(priority IOPriority) int64

	// IsRateLimited returns true if the priority is rate limited.
	IsRateLimited(priority IOPriority) bool
}

// GenericRateLimiter is a token-bucket based rate limiter.
type GenericRateLimiter struct {
	mu sync.Mutex

	// Configuration
	bytesPerSecond int64
	refillPeriod   time.Duration
	fairness       int64 // Lower fairness = more aggressive rate limiting

	// Mode
	mode RateLimiterMode

	// Token bucket state
	availableBytes int64
	lastRefillTime time.Time

	// Statistics
	totalBytesThrough [IOPriorityTotal]int64
	totalRequests     [IOPriorityTotal]int64

	// For waiting
	cv *sync.Cond
}

// RateLimiterOptions contains options for creating a rate limiter.
type RateLimiterOptions struct {
	// BytesPerSecond is the maximum rate in bytes per second.
	BytesPerSecond int64

	// RefillPeriod is how often to refill the token bucket.
	// Default: 100ms
	RefillPeriod time.Duration

	// Fairness controls how aggressively high-priority requests preempt low-priority.
	// Range: 1-100, lower = more aggressive. Default: 10
	Fairness int64

	// Mode specifies what I/O to rate limit.
	Mode RateLimiterMode
}

// DefaultRateLimiterOptions returns default options.
func DefaultRateLimiterOptions() *RateLimiterOptions {
	return &RateLimiterOptions{
		BytesPerSecond: 100 * 1024 * 1024, // 100 MB/s
		RefillPeriod:   100 * time.Millisecond,
		Fairness:       10,
		Mode:           RateLimiterModeWritesOnly,
	}
}

// NewGenericRateLimiter creates a new rate limiter.
func NewGenericRateLimiter(opts *RateLimiterOptions) *GenericRateLimiter {
	if opts == nil {
		opts = DefaultRateLimiterOptions()
	}
	if opts.RefillPeriod == 0 {
		opts.RefillPeriod = 100 * time.Millisecond
	}
	if opts.Fairness == 0 {
		opts.Fairness = 10
	}

	rl := &GenericRateLimiter{
		bytesPerSecond: opts.BytesPerSecond,
		refillPeriod:   opts.RefillPeriod,
		fairness:       opts.Fairness,
		mode:           opts.Mode,
		lastRefillTime: time.Now(),
	}
	rl.cv = sync.NewCond(&rl.mu)

	// Initial allocation
	rl.availableBytes = opts.BytesPerSecond / 10 // Start with 100ms worth

	return rl
}

// Request requests bytes to be written/read.
func (rl *GenericRateLimiter) Request(bytes int64, priority IOPriority) {
	if bytes <= 0 {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Update statistics
	rl.totalRequests[priority]++
	rl.totalBytesThrough[priority] += bytes

	// Refill tokens if needed
	rl.refill()

	// Wait for tokens
	for rl.availableBytes < bytes {
		// Calculate how long to wait
		needed := bytes - rl.availableBytes
		waitTime := min(time.Duration(needed*int64(time.Second))/time.Duration(rl.bytesPerSecond), rl.refillPeriod)

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			time.Sleep(waitTime)
			close(done)
		}()

		rl.mu.Unlock()
		<-done
		rl.mu.Lock()

		rl.refill()
	}

	rl.availableBytes -= bytes
}

// refill adds tokens based on elapsed time.
// Must be called with rl.mu held.
func (rl *GenericRateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(rl.lastRefillTime)
	if elapsed < time.Millisecond {
		return // Too soon
	}

	// Calculate tokens to add
	tokensToAdd := int64(float64(rl.bytesPerSecond) * elapsed.Seconds())
	rl.availableBytes += tokensToAdd
	rl.lastRefillTime = now

	// Cap at max burst (1 second worth)
	maxBurst := rl.bytesPerSecond
	if rl.availableBytes > maxBurst {
		rl.availableBytes = maxBurst
	}
}

// SetBytesPerSecond dynamically sets the rate limit.
func (rl *GenericRateLimiter) SetBytesPerSecond(bytesPerSecond int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.bytesPerSecond = bytesPerSecond
}

// GetBytesPerSecond returns the current rate limit.
func (rl *GenericRateLimiter) GetBytesPerSecond() int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.bytesPerSecond
}

// GetTotalBytesThrough returns total bytes passed through the limiter.
func (rl *GenericRateLimiter) GetTotalBytesThrough(priority IOPriority) int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.totalBytesThrough[priority]
}

// GetTotalRequests returns total request count.
func (rl *GenericRateLimiter) GetTotalRequests(priority IOPriority) int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.totalRequests[priority]
}

// IsRateLimited returns true if the priority is rate limited.
func (rl *GenericRateLimiter) IsRateLimited(priority IOPriority) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	switch rl.mode {
	case RateLimiterModeReadsOnly:
		return priority == IOPriorityHigh // High priority = reads
	case RateLimiterModeWritesOnly:
		return priority == IOPriorityLow // Low priority = compaction/flush
	case RateLimiterModeAllIO:
		return true
	default:
		return false
	}
}

// NewRateLimiter creates a rate limiter with the specified bytes per second.
// This is a convenience function.
func NewRateLimiter(bytesPerSecond int64) RateLimiter {
	return NewGenericRateLimiter(&RateLimiterOptions{
		BytesPerSecond: bytesPerSecond,
	})
}
