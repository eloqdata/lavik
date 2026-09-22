// Copyright (C) 2026 EloqData Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// cluster-client-go is the go-redis half of the fixed-version Cluster client
// compatibility matrix. It wraps a standard, unpatched
// github.com/redis/go-redis/v9 ClusterClient in a JSON-lines stdin/stdout
// protocol so tests/meta_integration/gate_cluster_client.py can orchestrate
// failover while the connection pool stays alive inside this process — closing
// the client mid-scenario would defeat the connection-pool acceptance.
//
// The driver never retries or reclassifies on its own: every redirection and
// retry decision comes from the stock client. Errors are only bucketed by
// substring so the gate can tell tolerated failover-window rejections from
// genuine incompatibilities.
//
// Usage: cluster-client-go --addrs host:port[,host:port...] --protocol 2|3
//
//	[--password pw] [--tls-ca ca.crt]
//
// Then write one JSON object per line on stdin; each produces exactly one
// JSON line on stdout: {"ok":true,...} or {"ok":false,"error":"..."}.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type request struct {
	Op       string   `json:"op"`
	Key      string   `json:"key"`
	Keys     []string `json:"keys"`
	Args     []string `json:"args"`
	Value    string   `json:"value"`
	Prefixes []string `json:"prefixes"`
	Addr     string   `json:"addr"`
}

// classify buckets an error for the failover-window assertions. Tolerated
// buckets are the documented transient rejections of a Meta-managed owner
// change plus raw connection loss to a killed node; anything else is a
// compatibility failure the gate must see.
func classify(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "MOVED"):
		return "MOVED"
	case strings.Contains(s, "TRYAGAIN"):
		return "TRYAGAIN"
	case strings.Contains(s, "CLUSTERDOWN"):
		return "CLUSTERDOWN"
	case strings.Contains(s, "LOADING"):
		return "LOADING"
	case strings.Contains(s, "READONLY"):
		return "READONLY"
	case strings.Contains(s, "MASTERDOWN"):
		return "MASTERDOWN"
	case errors.Is(err, io.EOF),
		strings.Contains(s, "connection reset"),
		strings.Contains(s, "broken pipe"),
		strings.Contains(s, "connection refused"),
		strings.Contains(s, "i/o timeout"),
		strings.Contains(s, "server closed"):
		return "conn"
	default:
		return "fatal"
	}
}

type loadStats struct {
	mu     sync.Mutex
	setOK  int
	getOK  int
	errors map[string]int
	fatal  string
}

func (s *loadStats) recordOK(get bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if get {
		s.getOK++
	} else {
		s.setOK++
	}
}

// recordErr reports whether the loop must stop (fatal bucket latched).
func (s *loadStats) recordErr(bucket string, err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors[bucket]++
	if bucket == "fatal" && s.fatal == "" {
		s.fatal = err.Error()
	}
	return bucket == "fatal"
}

func (s *loadStats) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	errs := map[string]int{}
	for k, v := range s.errors {
		errs[k] = v
	}
	return map[string]any{
		"set_ok": s.setOK, "get_ok": s.getOK,
		"errors": errs, "fatal": s.fatal,
	}
}

type driver struct {
	client   *redis.ClusterClient
	loadStop chan struct{}
	loadDone chan struct{}
	load     *loadStats
}

func (d *driver) stopLoad() {
	if d.loadStop != nil {
		close(d.loadStop)
		d.loadStop = nil
	}
}

// runLoad hammers every prefix with alternating SET/GET until stopped. A
// fatal-bucket error latches and halts the loop. It closes done on exit so
// load_stop reads only final counters.
func (d *driver) runLoad(prefixes []string, stop <-chan struct{}, done chan<- struct{}, stats *loadStats) {
	defer close(done)
	ctx := context.Background()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}
		for _, prefix := range prefixes {
			key := fmt.Sprintf("{%s}-%d", prefix, i%64)
			if err := d.client.Set(ctx, key, fmt.Sprintf("v%d", i), 0).Err(); err == nil {
				stats.recordOK(false)
			} else if stats.recordErr(classify(err), err) {
				return
			}
			if err := d.client.Get(ctx, key).Err(); err == nil || errors.Is(err, redis.Nil) {
				stats.recordOK(true)
			} else if stats.recordErr(classify(err), err) {
				return
			}
		}
	}
}

func splitAddr(addr string) (string, int, error) {
	colon := strings.LastIndex(addr, ":")
	if colon < 0 {
		return "", 0, fmt.Errorf("unexpected node address %q", addr)
	}
	host := strings.TrimPrefix(strings.TrimSuffix(addr[:colon], "]"), "[")
	port, err := strconv.Atoi(addr[colon+1:])
	if err != nil {
		return "", 0, fmt.Errorf("unexpected node port in %q", addr)
	}
	return host, port, nil
}

func slotsNormalized(slots []redis.ClusterSlot) ([]map[string]any, error) {
	out := []map[string]any{}
	for _, slot := range slots {
		entry := map[string]any{
			"first": slot.Start, "last": slot.End,
			"primary": nil, "replicas": []map[string]any{},
		}
		for i, node := range slot.Nodes {
			host, port, err := splitAddr(node.Addr)
			if err != nil {
				return nil, err
			}
			desc := map[string]any{"id": node.ID, "host": host, "port": port}
			if i == 0 {
				entry["primary"] = desc
			} else {
				entry["replicas"] = append(entry["replicas"].([]map[string]any), desc)
			}
		}
		out = append(out, entry)
	}
	return out, nil
}

func (d *driver) handle(req request) map[string]any {
	ctx := context.Background()
	switch req.Op {
	case "ping":
		if err := d.client.Ping(ctx).Err(); err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true, "value": "PONG"}
	case "set":
		if err := d.client.Set(ctx, req.Key, req.Value, 0).Err(); err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true}
	case "get":
		value, err := d.client.Get(ctx, req.Key).Result()
		if errors.Is(err, redis.Nil) {
			return map[string]any{"ok": true, "value": nil}
		}
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true, "value": value}
	case "mget":
		// Typed MGet routes the whole command by its first key, so a
		// cross-slot call surfaces the server's CROSSSLOT; the gate uses
		// same-hashtag keys here as the positive control.
		values, err := d.client.MGet(ctx, req.Keys...).Result()
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true, "value": values}
	case "do":
		args := make([]any, len(req.Args))
		for i, a := range req.Args {
			args[i] = a
		}
		value, err := d.client.Do(ctx, args...).Result()
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true, "value": value}
	case "probe_at":
		// Run one command against a specific cluster member through the
		// ClusterClient's own per-node connection (the pooled path), so a
		// foreign-slot key must surface that node's MOVED reply.
		// ForEachMaster runs one callback goroutine per master, so the
		// first-match latch is mutex-guarded; result/probeErr are written
		// only by the matching callback and read after ForEachMaster waits.
		var probeMu sync.Mutex
		var result any
		var probeErr error
		found := false
		err := d.client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			if node.Options().Addr != req.Addr {
				return nil
			}
			args := make([]any, len(req.Args))
			for i, a := range req.Args {
				args[i] = a
			}
			value, callErr := node.Do(ctx, args...).Result()
			probeMu.Lock()
			defer probeMu.Unlock()
			if found {
				return nil
			}
			found = true
			result, probeErr = value, callErr
			return nil
		})
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		if !found {
			return map[string]any{"ok": false, "error": "node not in cluster map: " + req.Addr}
		}
		if probeErr != nil {
			return map[string]any{"ok": false, "error": probeErr.Error()}
		}
		return map[string]any{"ok": true, "value": result}
	case "cluster_slots":
		slots, err := d.client.ClusterSlots(ctx).Result()
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		normalized, err := slotsNormalized(slots)
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true, "value": normalized}
	case "cluster_shards":
		_, err := d.client.ClusterShards(ctx).Result()
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true}
	case "pool_stats":
		stats := d.client.PoolStats()
		return map[string]any{"ok": true, "value": map[string]any{
			"hits": stats.Hits, "misses": stats.Misses,
			"timeouts": stats.Timeouts, "total_conns": stats.TotalConns,
			"idle_conns": stats.IdleConns, "stale_conns": stats.StaleConns,
		}}
	case "version":
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return map[string]any{"ok": false, "error": "no build info"}
		}
		for _, dep := range info.Deps {
			if dep.Path == "github.com/redis/go-redis/v9" {
				return map[string]any{"ok": true, "value": dep.Version}
			}
		}
		return map[string]any{"ok": false, "error": "go-redis module not found"}
	case "load_start":
		if len(req.Prefixes) == 0 {
			return map[string]any{"ok": false, "error": "load_start needs prefixes"}
		}
		d.stopLoad()
		d.load = &loadStats{errors: map[string]int{}}
		d.loadStop = make(chan struct{})
		d.loadDone = make(chan struct{})
		go d.runLoad(req.Prefixes, d.loadStop, d.loadDone, d.load)
		return map[string]any{"ok": true}
	case "load_stop":
		stop := d.loadStop
		done := d.loadDone
		d.stopLoad()
		if d.load == nil {
			return map[string]any{"ok": false, "error": "no load running"}
		}
		// A single in-flight op can still be blocked on its read timeout when
		// the stop lands; wait for the goroutine so the counters are final.
		if stop != nil && done != nil {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
			}
		}
		return map[string]any{"ok": true, "value": d.load.snapshot()}
	case "close":
		d.stopLoad()
		if err := d.client.Close(); err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		return map[string]any{"ok": true, "quit": true}
	default:
		return map[string]any{"ok": false, "error": "unknown op " + req.Op}
	}
}

func main() {
	addrsFlag := flag.String("addrs", "", "comma-separated seed host:port list")
	protocol := flag.Int("protocol", 3, "RESP version, 2 or 3")
	password := flag.String("password", "", "requirepass password")
	tlsCA := flag.String("tls-ca", "", "PEM CA bundle enabling TLS")
	flag.Parse()
	if *addrsFlag == "" || (*protocol != 2 && *protocol != 3) {
		fmt.Fprintln(os.Stderr, "usage: --addrs h:p[,h:p] --protocol 2|3 [--password pw] [--tls-ca ca]")
		os.Exit(2)
	}
	options := &redis.ClusterOptions{
		Addrs:        strings.Split(*addrsFlag, ","),
		Protocol:     *protocol,
		Password:     *password,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     16,
	}
	if *tlsCA != "" {
		pem, err := os.ReadFile(*tlsCA)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			fmt.Fprintln(os.Stderr, "no CA certificates in "+*tlsCA)
			os.Exit(2)
		}
		options.TLSConfig = &tls.Config{
			RootCAs:    roots,
			ServerName: "127.0.0.1",
			MinVersion: tls.VersionTLS12,
		}
	}
	d := &driver{client: redis.NewClusterClient(options)}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			encoder.Encode(map[string]any{"ok": false, "error": "bad request: " + err.Error()})
			continue
		}
		reply := d.handle(req)
		if err := encoder.Encode(reply); err != nil {
			fmt.Fprintln(os.Stderr, "stdout write failed:", err)
			os.Exit(1)
		}
		if quit, _ := reply["quit"].(bool); quit {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "stdin read failed:", err)
		os.Exit(1)
	}
}
