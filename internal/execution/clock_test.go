package execution

import (
	"context"
	"sync"
	"time"
)

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  map[*fakeTimer]time.Time
	changed chan struct{}
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{
		now:     now.UTC(),
		timers:  make(map[*fakeTimer]time.Time),
		changed: make(chan struct{}),
	}
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) NewTimer(duration time.Duration) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	timer := &fakeTimer{
		clock: clock,
		ch:    make(chan time.Time, 1),
	}
	if duration <= 0 {
		timer.ch <- clock.now
		timer.fired = true
		return timer
	}
	clock.timers[timer] = clock.now.Add(duration)
	clock.notifyLocked()
	return timer
}

func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	clock.now = clock.now.Add(duration)
	for timer, deadline := range clock.timers {
		if deadline.After(clock.now) {
			continue
		}
		delete(clock.timers, timer)
		timer.fired = true
		timer.ch <- clock.now
	}
	clock.notifyLocked()
}

func (clock *fakeClock) WaitForTimers(ctx context.Context, count int) bool {
	for {
		clock.mu.Lock()
		if len(clock.timers) >= count {
			clock.mu.Unlock()
			return true
		}
		changed := clock.changed
		clock.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

func (clock *fakeClock) notifyLocked() {
	close(clock.changed)
	clock.changed = make(chan struct{})
}

type fakeTimer struct {
	clock   *fakeClock
	ch      chan time.Time
	fired   bool
	stopped bool
}

func (timer *fakeTimer) C() <-chan time.Time {
	return timer.ch
}

func (timer *fakeTimer) Stop() {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	if timer.fired || timer.stopped {
		return
	}
	timer.stopped = true
	delete(timer.clock.timers, timer)
	timer.clock.notifyLocked()
}
