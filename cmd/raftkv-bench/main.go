// Command raftkv-bench measures the cluster and writes the numbers, tables
// and charts used in the README.
//
// It starts a full cluster inside this process: every node has its own TCP
// listener on 127.0.0.1 and its own directory of fsynced files. So the
// numbers include real sockets, real serialisation and real disk syncs, but
// no network latency, and all nodes share one machine's CPU and disk.
//
//	go run ./cmd/raftkv-bench -out bench/results
//	go run ./cmd/raftkv-bench -render bench/results/results.json -out bench/results
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/silviacioroiu/raftkv/internal/cluster"
	"github.com/silviacioroiu/raftkv/internal/hrtime"
	"github.com/silviacioroiu/raftkv/raft"
)

type Settings struct {
	Sizes             []int   `json:"cluster_sizes"`
	Clients           []int   `json:"client_counts"`
	DurationSec       float64 `json:"measure_seconds"`
	WarmupSec         float64 `json:"warmup_seconds"`
	Keys              int     `json:"key_space"`
	ValueBytes        int     `json:"value_bytes"`
	Workload          string  `json:"workload"`
	Fsync             bool    `json:"fsync"`
	SnapshotThreshold uint64  `json:"snapshot_threshold"`
	ElectionTimeoutMs [2]int  `json:"election_timeout_ms"`
	HeartbeatMs       int     `json:"heartbeat_ms"`
	FailoverTrials    int     `json:"failover_trials"`
}

type Env struct {
	Date      string `json:"date"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"logical_cpus"`
	Machine   string `json:"machine,omitempty"`
	Transport string `json:"transport"`
}

type ThroughputPoint struct {
	Nodes     int     `json:"nodes"`
	Clients   int     `json:"clients"`
	Ops       int     `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Ms     float64 `json:"p50_ms"`
	P99Ms     float64 `json:"p99_ms"`
	MeanMs    float64 `json:"mean_ms"`
}

type FailoverResult struct {
	Nodes       int       `json:"nodes"`
	ElectionMs  []float64 `json:"election_ms"`
	ClientMs    []float64 `json:"client_ms"`
	ElectionP50 float64   `json:"election_p50_ms"`
	ElectionP95 float64   `json:"election_p95_ms"`
	ElectionMax float64   `json:"election_max_ms"`
	ClientP50   float64   `json:"client_p50_ms"`
	ClientP95   float64   `json:"client_p95_ms"`
	ClientMax   float64   `json:"client_max_ms"`
}

type Results struct {
	Env        Env               `json:"environment"`
	Settings   Settings          `json:"settings"`
	Throughput []ThroughputPoint `json:"throughput"`
	Failover   []FailoverResult  `json:"failover"`
}

func main() {
	var (
		sizes     = flag.String("sizes", "3,5,7", "cluster sizes")
		clients   = flag.String("clients", "1,4,16,64", "concurrent client counts")
		duration  = flag.Duration("duration", 8*time.Second, "measured time per data point")
		warmup    = flag.Duration("warmup", 2*time.Second, "unmeasured time before each data point")
		trials    = flag.Int("failover-trials", 15, "leader crashes per cluster size; 0 skips the failover test")
		fsync     = flag.Bool("fsync", true, "force every write to disk, as Raft requires")
		threshold = flag.Uint64("snapshot-threshold", 10000, "log entries between snapshots")
		machine   = flag.String("machine", "", "free-text description of the hardware, recorded in the results")
		out       = flag.String("out", "bench/results", "output directory")
		render    = flag.String("render", "", "skip measuring; rebuild tables and charts from this results.json")
	)
	flag.Parse()

	var res Results
	if *render != "" {
		raw, err := os.ReadFile(*render)
		if err != nil {
			fatal(err)
		}
		if err := json.Unmarshal(raw, &res); err != nil {
			fatal(err)
		}
	} else {
		res = Results{
			Env: Env{
				Date:      time.Now().Format("2006-01-02"),
				GoVersion: runtime.Version(),
				OS:        runtime.GOOS,
				Arch:      runtime.GOARCH,
				CPUs:      runtime.NumCPU(),
				Machine:   *machine,
				Transport: "net/rpc over TCP on 127.0.0.1, all nodes in one process",
			},
			Settings: Settings{
				Sizes:             mustInts(*sizes),
				Clients:           mustInts(*clients),
				DurationSec:       duration.Seconds(),
				WarmupSec:         warmup.Seconds(),
				Keys:              1000,
				ValueBytes:        64,
				Workload:          "50% Put, 50% Get, uniform keys, closed loop",
				Fsync:             *fsync,
				SnapshotThreshold: *threshold,
				ElectionTimeoutMs: [2]int{150, 300},
				HeartbeatMs:       50,
				FailoverTrials:    *trials,
			},
		}
		if len(res.Settings.Sizes) > seriesColorCount {
			fatal(fmt.Errorf("at most %d cluster sizes can be charted together", seriesColorCount))
		}
		for _, n := range res.Settings.Sizes {
			points, err := measureThroughput(n, res.Settings)
			if err != nil {
				fatal(err)
			}
			res.Throughput = append(res.Throughput, points...)
		}
		if *trials > 0 {
			for _, n := range res.Settings.Sizes {
				fr, err := measureFailover(n, res.Settings)
				if err != nil {
					fatal(err)
				}
				res.Failover = append(res.Failover, fr)
			}
		}
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal(err)
	}
	if *render == "" {
		raw, _ := json.MarshalIndent(res, "", "  ")
		write(*out, "results.json", string(raw)+"\n")
	}
	write(*out, "results.md", markdown(res))
	for name, svg := range charts(res) {
		write(*out, name, svg)
	}
}

func newCluster(nodes int, s Settings) (*cluster.Cluster, func(), error) {
	dir, err := os.MkdirTemp("", "raftkv-bench-")
	if err != nil {
		return nil, nil, err
	}
	c, err := cluster.New(cluster.Options{
		Size:    nodes,
		TCP:     true,
		DataDir: dir,
		NoFsync: !s.Fsync,
		Raft:    func(cfg *raft.Config) { cfg.SnapshotThreshold = s.SnapshotThreshold },
	})
	if err != nil {
		os.RemoveAll(dir)
		return nil, nil, err
	}
	cleanup := func() {
		c.Close()
		os.RemoveAll(dir)
	}
	if _, err := c.WaitLeader(15 * time.Second); err != nil {
		cleanup()
		return nil, nil, err
	}
	return c, cleanup, nil
}

type sample struct {
	start, end time.Duration // since the run began
}

// measureThroughput runs a closed-loop workload at each client count: every
// client issues its next request as soon as the previous one is answered.
func measureThroughput(nodes int, s Settings) ([]ThroughputPoint, error) {
	c, cleanup, err := newCluster(nodes, s)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	value := strings.Repeat("v", s.ValueBytes)
	warmup := time.Duration(s.WarmupSec * float64(time.Second))
	measure := time.Duration(s.DurationSec * float64(time.Second))

	var points []ThroughputPoint
	for _, n := range s.Clients {
		samples := make([][]sample, n)
		errs := make(chan error, n)
		stop := make(chan struct{})
		begin := hrtime.Now()

		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			cl, err := c.NewClient()
			if err != nil {
				return nil, err
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(i) + 1))
				for {
					select {
					case <-stop:
						return
					default:
					}
					key := "key-" + strconv.Itoa(rng.Intn(s.Keys))
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					start := (hrtime.Now() - begin)
					var err error
					if rng.Intn(2) == 0 {
						err = cl.Put(ctx, key, value)
					} else {
						_, err = cl.Get(ctx, key)
					}
					end := (hrtime.Now() - begin)
					cancel()
					if err != nil {
						errs <- fmt.Errorf("%d nodes, %d clients: %w", nodes, n, err)
						return
					}
					samples[i] = append(samples[i], sample{start, end})
				}
			}(i)
		}
		time.Sleep(warmup + measure)
		close(stop)
		wg.Wait()
		select {
		case err := <-errs:
			return nil, err
		default:
		}

		// Count only requests that ran entirely inside the measured window.
		var lat []float64
		for _, per := range samples {
			for _, sm := range per {
				if sm.start >= warmup && sm.end <= warmup+measure {
					lat = append(lat, float64(sm.end-sm.start)/float64(time.Millisecond))
				}
			}
		}
		sort.Float64s(lat)
		p := ThroughputPoint{
			Nodes:     nodes,
			Clients:   n,
			Ops:       len(lat),
			OpsPerSec: float64(len(lat)) / measure.Seconds(),
			P50Ms:     percentile(lat, 0.50),
			P99Ms:     percentile(lat, 0.99),
			MeanMs:    mean(lat),
		}
		points = append(points, p)
		fmt.Printf("throughput  nodes=%d clients=%-3d %8.0f ops/s  p50 %.2f ms  p99 %.2f ms\n",
			p.Nodes, p.Clients, p.OpsPerSec, p.P50Ms, p.P99Ms)
	}
	return points, nil
}

// measureFailover crashes the leader repeatedly while one client writes
// continuously, and records two things per crash:
//
//   - election time: from the crash until another node is leader;
//   - client-observed outage: the longest the client waited for a write to
//     succeed, counted from the crash.
func measureFailover(nodes int, s Settings) (FailoverResult, error) {
	fr := FailoverResult{Nodes: nodes}
	c, cleanup, err := newCluster(nodes, s)
	if err != nil {
		return fr, err
	}
	defer cleanup()

	cl, err := c.NewClient()
	if err != nil {
		return fr, err
	}
	cl.AttemptTimeout = 250 * time.Millisecond

	begin := hrtime.Now()
	var mu sync.Mutex
	var samples []sample
	stop := make(chan struct{})
	writerErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			start := (hrtime.Now() - begin)
			err := cl.Put(ctx, "failover", strconv.Itoa(i))
			end := (hrtime.Now() - begin)
			cancel()
			if err != nil {
				writerErr <- err
				return
			}
			mu.Lock()
			samples = append(samples, sample{start, end})
			mu.Unlock()
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for trial := 0; trial < s.FailoverTrials; trial++ {
		time.Sleep(time.Second) // let the cluster settle and replicate
		old, ok := c.Leader()
		if !ok {
			return fr, fmt.Errorf("no leader before trial %d", trial)
		}

		crash := (hrtime.Now() - begin)
		c.Crash(old)
		for {
			if id, ok := c.Leader(); ok && id != old {
				break
			}
			if (hrtime.Now()-begin)-crash > 15*time.Second {
				return fr, fmt.Errorf("%d nodes: no new leader 15s after a crash", nodes)
			}
			time.Sleep(500 * time.Microsecond)
		}
		elected := (hrtime.Now() - begin) - crash

		time.Sleep(time.Second) // give the client time to find the new leader
		select {
		case err := <-writerErr:
			return fr, err
		default:
		}
		var outage time.Duration
		mu.Lock()
		for _, sm := range samples {
			if sm.end < crash {
				continue
			}
			from := sm.start
			if from < crash {
				from = crash
			}
			if d := sm.end - from; d > outage {
				outage = d
			}
		}
		samples = samples[:0]
		mu.Unlock()

		fr.ElectionMs = append(fr.ElectionMs, ms(elected))
		fr.ClientMs = append(fr.ClientMs, ms(outage))
		fmt.Printf("failover    nodes=%d trial=%-2d elected in %6.1f ms  client outage %6.1f ms\n",
			nodes, trial+1, ms(elected), ms(outage))

		if err := c.Start(old); err != nil {
			return fr, err
		}
	}

	e := append([]float64(nil), fr.ElectionMs...)
	o := append([]float64(nil), fr.ClientMs...)
	sort.Float64s(e)
	sort.Float64s(o)
	fr.ElectionP50, fr.ElectionP95, fr.ElectionMax = percentile(e, 0.5), percentile(e, 0.95), percentile(e, 1)
	fr.ClientP50, fr.ClientP95, fr.ClientMax = percentile(o, 0.5), percentile(o, 0.95), percentile(o, 1)
	return fr, nil
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// percentile uses the nearest-rank method on an ascending slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range v {
		sum += x
	}
	return sum / float64(len(v))
}

func mustInts(s string) []int {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			fatal(fmt.Errorf("bad number %q", part))
		}
		out = append(out, n)
	}
	return out
}

func write(dir, name, content string) {
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		fatal(err)
	}
	fmt.Println("wrote", path)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "raftkv-bench:", err)
	os.Exit(1)
}
