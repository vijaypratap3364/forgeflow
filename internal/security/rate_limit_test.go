package security

import (
	"testing"
	"time"
)

func TestFixedWindowRateLimiter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	limiter, err := NewFixedWindowRateLimiter(2, time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewFixedWindowRateLimiter() error = %v", err)
	}

	first := limiter.Allow("user-1:POST")
	second := limiter.Allow("user-1:POST")
	denied := limiter.Allow("user-1:POST")
	other := limiter.Allow("user-2:POST")
	if !first.Allowed || first.Remaining != 1 || !second.Allowed || second.Remaining != 0 {
		t.Fatalf("allowed decisions = %#v %#v", first, second)
	}
	if denied.Allowed || denied.RetryAfter != time.Minute {
		t.Fatalf("denied decision = %#v", denied)
	}
	if !other.Allowed {
		t.Fatalf("other key decision = %#v", other)
	}

	now = now.Add(time.Minute)
	reset := limiter.Allow("user-1:POST")
	if !reset.Allowed || reset.Remaining != 1 {
		t.Fatalf("reset decision = %#v", reset)
	}
}
