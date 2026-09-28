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
	"crypto/tls"
	"crypto/x509"
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
	replicaOnly := flag.Bool("replica", false, "Read from discovered replicas")
	dataPassword := flag.String("data-password", "", "Data password")
	ca := flag.String("tls-ca", "", "TLS CA for both Sentinel and Data")
	cert := flag.String("tls-cert", "", "Application client certificate")
	key := flag.String("tls-key", "", "Application private key")
	flag.Parse()
	var tlsConfig *tls.Config
	if *ca != "" {
		pem, err := os.ReadFile(*ca)
		if err != nil {
			panic(err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			panic("invalid CA")
		}
		tlsConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		if *cert != "" {
			pair, err := tls.LoadX509KeyPair(*cert, *key)
			if err != nil {
				panic(err)
			}
			tlsConfig.Certificates = []tls.Certificate{pair}
		}
	}
	// Leave ServerName empty: the standard dialer verifies the actual target.
	dial := redis.NewDialer(&redis.Options{TLSConfig: tlsConfig, DialTimeout: time.Second})
	sentinel := redis.NewSentinelClient(&redis.Options{Addr: strings.Split(*seeds, ",")[0], Password: *password, Protocol: *protocol, TLSConfig: tlsConfig, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer sentinel.Close()
	stats := &counters{dialed: map[string]int{}, closed: map[string]int{}}
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: *group, SentinelAddrs: strings.Split(*seeds, ","), SentinelPassword: *password,
		ReplicaOnly: *replicaOnly, Password: *dataPassword, TLSConfig: tlsConfig, Protocol: *protocol, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
		ContextTimeoutEnabled: true, MaxRetries: -1, PoolSize: 4,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dial(ctx, network, addr)
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
		case "sentinel-address":
			value, err = sentinel.GetMasterAddrByName(ctx, *group).Result()
		case "sentinel-ping":
			value, err = sentinel.Ping(ctx).Result()
		case "set":
			value, err = client.Set(ctx, req.Key, req.Value, 0).Result()
		case "role":
			value, err = client.Do(ctx, "ROLE").Result()
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
