package execution

import "time"

// Clock supplies scheduler time and cancelable timers. Tests can provide a
// controlled implementation so retries and lease expiry need no real waiting.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Timer is the minimal timer contract needed by the scheduler and workers.
type Timer interface {
	C() <-chan time.Time
	Stop()
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func (systemClock) NewTimer(duration time.Duration) Timer {
	return &systemTimer{timer: time.NewTimer(duration)}
}

type systemTimer struct {
	timer *time.Timer
}

func (timer *systemTimer) C() <-chan time.Time {
	return timer.timer.C
}

func (timer *systemTimer) Stop() {
	timer.timer.Stop()
}
