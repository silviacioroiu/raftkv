//go:build !windows

package hrtime

import "time"

var start = time.Now()

// time.Since reads the monotonic clock, which has nanosecond resolution on
// Linux and macOS.
func now() time.Duration { return time.Since(start) }
