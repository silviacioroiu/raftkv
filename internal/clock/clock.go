// Package clock abstracts time so that Raft's timers can later be driven by
// a fake clock in tests. Raft code should take a Clock and never call the
// time package's Now, After or NewTimer directly.
package clock

import "time"

type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTimer(d time.Duration) Timer
}

// Timer mirrors time.Timer, with the channel behind a method so that it can
// be an interface.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Real is the wall clock.
type Real struct{}

var _ Clock = Real{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (Real) NewTimer(d time.Duration) Timer         { return realTimer{time.NewTimer(d)} }

type realTimer struct {
	t *time.Timer
}

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
