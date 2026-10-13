package hrtime

import (
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32  = syscall.NewLazyDLL("kernel32.dll")
	counter   = kernel32.NewProc("QueryPerformanceCounter")
	frequency = kernel32.NewProc("QueryPerformanceFrequency")
	ticksPerS = queryFrequency()
)

func queryFrequency() int64 {
	var f int64
	frequency.Call(uintptr(unsafe.Pointer(&f)))
	if f <= 0 {
		panic("hrtime: QueryPerformanceFrequency failed")
	}
	return f
}

// now reads the performance counter, which ticks at least once per
// microsecond on any hardware Windows supports.
func now() time.Duration {
	var c int64
	counter.Call(uintptr(unsafe.Pointer(&c)))
	// Whole seconds and the remainder are converted separately so that the
	// multiplication cannot overflow.
	whole, rest := c/ticksPerS, c%ticksPerS
	return time.Duration(whole)*time.Second + time.Duration(rest*int64(time.Second)/ticksPerS)
}
