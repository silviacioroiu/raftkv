// Command raftkv-server runs one node of the key-value cluster.
//
// A three-node cluster on one machine:
//
//	raftkv-server -id n1 -cluster n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003 -data data/n1
//	raftkv-server -id n2 -cluster n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003 -data data/n2
//	raftkv-server -id n3 -cluster n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003 -data data/n3
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/silviacioroiu/raftkv/kv"
	"github.com/silviacioroiu/raftkv/raft"
	"github.com/silviacioroiu/raftkv/storage"
	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/rpcnet"
)

func main() {
	var (
		id        = flag.String("id", "", "this node's ID; must appear in -cluster")
		members   = flag.String("cluster", "", "all members as id=host:port, comma separated")
		dataDir   = flag.String("data", "", "directory for this node's durable state")
		threshold = flag.Uint64("snapshot-threshold", 10000, "log entries to accumulate before compacting; 0 disables")
		verbose   = flag.Bool("v", false, "log role changes and compactions")
	)
	flag.Parse()

	addrs, err := ParseCluster(*members)
	if err != nil {
		fatal(err)
	}
	self := transport.NodeID(*id)
	if _, ok := addrs[self]; !ok || *dataDir == "" {
		fmt.Fprintln(os.Stderr, "usage: raftkv-server -id ID -cluster id=host:port,... -data DIR")
		os.Exit(2)
	}

	level := slog.LevelWarn
	if *verbose {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	store, err := storage.OpenFile(*dataDir, true)
	if err != nil {
		fatal(err)
	}
	tr, err := rpcnet.Listen(self, addrs[self])
	if err != nil {
		fatal(err)
	}
	var peers []transport.NodeID
	for peer, addr := range addrs {
		peers = append(peers, peer)
		if peer != self {
			tr.SetPeer(peer, addr)
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })

	srv, err := kv.Start(kv.Options{
		Raft:      raft.Config{ID: self, Peers: peers, SnapshotThreshold: *threshold},
		Transport: tr,
		Store:     store,
		Logger:    logger,
	})
	if err != nil {
		fatal(err)
	}
	fmt.Printf("%s listening on %s, %d-node cluster\n", self, tr.Addr(), len(peers))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			tr.Close()
			srv.Close()
			store.Close()
			return
		case <-ticker.C:
			if *verbose {
				st := srv.Raft().Status()
				logger.Info("status", "role", st.Role.String(), "term", st.Term, "leader", string(st.LeaderID),
					"commit", st.CommitIndex, "applied", st.LastApplied, "snapshot", st.SnapshotIndex)
			}
		}
	}
}

// ParseCluster parses "id=host:port,id=host:port".
func ParseCluster(s string) (map[transport.NodeID]string, error) {
	out := make(map[transport.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad cluster member %q, want id=host:port", part)
		}
		out[transport.NodeID(id)] = addr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-cluster is empty")
	}
	return out, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "raftkv-server:", err)
	os.Exit(1)
}
