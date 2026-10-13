// Package hrtime is a monotonic clock fine enough to time sub-millisecond
// operations on every platform.
//
// It exists because of Windows: there, Go's time.Now advances only on the
// system timer tick, roughly every 0.5 to 1 ms, so two calls a few hundred
// microseconds apart often return the same instant. That is too coarse for
// measuring request latency and for ordering events in a recorded history.
package hrtime

import "time"

// Now returns the time elapsed since an arbitrary fixed point. Only
// differences between two results are meaningful.
func Now() time.Duration { return now() }
