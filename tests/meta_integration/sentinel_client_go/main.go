// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

// A persistent, unmodified FailoverClient controlled by JSON lines. The driver
// makes no discovery calls or retries; the Python gate issues ordinary business
// commands through the same pool before and after a fault. Dial/close counters
// expose active event-driven pool retirement without issuing another command.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type counters struct {
	mu             sync.Mutex
	dialed, closed map[string]int
}
type observedConn struct {
	net.Conn
	once  sync.Once
	stats *counters
	addr  string
}

func (c *observedConn) Close() error {
	c.once.Do(func() { c.stats.mu.Lock(); c.stats.closed[c.addr]++; c.stats.mu.Unlock() })
	return c.Conn.Close()
}
func main() {
	seeds := flag.String("addrs", "", "Sentinel seeds")
	group := flag.String("group", "single-discovery", "service name")
	password := flag.String("password", "sentinel-secret", "Sentinel password")
	protocol := flag.Int("protocol", 3, "Data RESP version; Sentinel retains library default")
	flag.Parse()
	stats := &counters{dialed: map[string]int{}, closed: map[string]int{}}
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: *group, SentinelAddrs: strings.Split(*seeds, ","), SentinelPassword: *password,
		Protocol: *protocol, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
		ContextTimeoutEnabled: true, MaxRetries: -1, PoolSize: 4,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			stats.mu.Lock()
			stats.dialed[addr]++
			stats.mu.Unlock()
			return &observedConn{Conn: c, stats: stats, addr: addr}, nil
		},
	})
	defer client.Close()
	var subscription *redis.PubSub
	defer func() {
		if subscription != nil {
			subscription.Close()
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var req struct{ Op, Key, Value string }
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			panic(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var value any
		var err error
		switch req.Op {
		case "set":
			value, err = client.Set(ctx, req.Key, req.Value, 0).Result()
		case "get":
			value, err = client.Get(ctx, req.Key).Result()
		case "subscribe":
			if subscription != nil {
				err = fmt.Errorf("subscription already exists")
				break
			}
			subscription = client.Subscribe(ctx, req.Key)
			_, err = subscription.Receive(ctx)
		case "receive":
			if subscription == nil {
				err = fmt.Errorf("no subscription")
				break
			}
			var message *redis.Message
			message, err = subscription.ReceiveMessage(ctx)
			if err == nil {
				value = message.Payload
			}
		case "publish":
			value, err = client.Publish(ctx, req.Key, req.Value).Result()
		case "stats":
			stats.mu.Lock()
			value = map[string]any{"dialed": clone(stats.dialed), "closed": clone(stats.closed)}
			stats.mu.Unlock()
		case "close":
			cancel()
			return
		default:
			err = fmt.Errorf("unknown operation %s", req.Op)
		}
		cancel()
		reply := map[string]any{"ok": err == nil, "value": value}
		if err != nil {
			reply["error"] = err.Error()
		}
		if err = encoder.Encode(reply); err != nil {
			panic(err)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
func clone(src map[string]int) map[string]int {
	dst := map[string]int{}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
