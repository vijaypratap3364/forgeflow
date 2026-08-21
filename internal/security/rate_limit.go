package security

import (
	"fmt"
	"sync"
	"time"
)

// RateLimitDecision describes one limiter decision and response metadata.
type RateLimitDecision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
	ResetAt    time.Time
}

// RateLimiter limits authenticated operations by a caller-selected stable key.
type RateLimiter interface {
	Allow(string) RateLimitDecision
}

type rateWindow struct {
	startedAt time.Time
	count     int
}

// FixedWindowRateLimiter is a concurrency-safe, process-local limiter suited
// to the current single API process. A shared limiter is required when the API
// is horizontally scaled.
type FixedWindowRateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	now     func() time.Time
	windows map[string]rateWindow
	calls   uint64
}

// NewFixedWindowRateLimiter creates a limiter with deterministic clock injection.
func NewFixedWindowRateLimiter(limit int, window time.Duration, now func() time.Time) (*FixedWindowRateLimiter, error) {
	if limit < 1 {
		return nil, fmt.Errorf("rate limit must be at least one")
	}
	if window <= 0 {
		return nil, fmt.Errorf("rate-limit window must be positive")
	}
	if now == nil {
		now = time.Now
	}
	return &FixedWindowRateLimiter{
		limit:   limit,
		window:  window,
		now:     now,
		windows: make(map[string]rateWindow),
	}, nil
}

// Allow consumes one request from key's current fixed window.
func (limiter *FixedWindowRateLimiter) Allow(key string) RateLimitDecision {
	now := limiter.now().UTC()
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	limiter.calls++
	if limiter.calls%1024 == 0 {
		for candidate, window := range limiter.windows {
			if !now.Before(window.startedAt.Add(limiter.window)) {
				delete(limiter.windows, candidate)
			}
		}
	}
	window, exists := limiter.windows[key]
	if !exists || !now.Before(window.startedAt.Add(limiter.window)) {
		window = rateWindow{startedAt: now}
	}
	if window.count >= limiter.limit {
		return RateLimitDecision{
			Allowed:    false,
			Limit:      limiter.limit,
			Remaining:  0,
			RetryAfter: maxDuration(0, window.startedAt.Add(limiter.window).Sub(now)),
			ResetAt:    window.startedAt.Add(limiter.window),
		}
	}
	window.count++
	limiter.windows[key] = window
	return RateLimitDecision{
		Allowed:   true,
		Limit:     limiter.limit,
		Remaining: limiter.limit - window.count,
		ResetAt:   window.startedAt.Add(limiter.window),
	}
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

var _ RateLimiter = (*FixedWindowRateLimiter)(nil)
