// Command raftkv-cli is a client for a running cluster.
//
//	raftkv-cli -cluster n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003 put greeting hello
//	raftkv-cli -cluster ... append greeting ", world"
//	raftkv-cli -cluster ... get greeting
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/silviacioroiu/raftkv/kv"
	"github.com/silviacioroiu/raftkv/transport"
	"github.com/silviacioroiu/raftkv/transport/rpcnet"
)

func main() {
	members := flag.String("cluster", "", "all members as id=host:port, comma separated")
	timeout := flag.Duration("timeout", 10*time.Second, "give up after this long")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if *members == "" || len(args) < 2 {
		usage()
		os.Exit(2)
	}

	tr, err := rpcnet.Listen("cli", "127.0.0.1:0")
	if err != nil {
		fatal(err)
	}
	defer tr.Close()

	var servers []transport.NodeID
	for _, part := range strings.Split(*members, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			fatal(fmt.Errorf("bad cluster member %q, want id=host:port", part))
		}
		servers = append(servers, transport.NodeID(id))
		tr.SetPeer(transport.NodeID(id), addr)
	}
	sort.Slice(servers, func(i, j int) bool { return servers[i] < servers[j] })

	client := kv.NewClient(tr, servers)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd, key := args[0], args[1]; {
	case cmd == "get" && len(args) == 2:
		value, err := client.Get(ctx, key)
		if err != nil {
			fatal(err)
		}
		fmt.Println(value)
	case cmd == "put" && len(args) == 3:
		if err := client.Put(ctx, key, args[2]); err != nil {
			fatal(err)
		}
	case cmd == "append" && len(args) == 3:
		if err := client.Append(ctx, key, args[2]); err != nil {
			fatal(err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: raftkv-cli -cluster id=host:port,... COMMAND
  get KEY
  put KEY VALUE
  append KEY VALUE`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "raftkv-cli:", err)
	os.Exit(1)
}
