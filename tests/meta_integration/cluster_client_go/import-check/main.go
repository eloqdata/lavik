// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

// Verify cutover through the module's pinned, unmodified go-redis client.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	address := flag.String("address", "", "target seed")
	password := flag.String("password", "", "Data password")
	cluster := flag.Bool("cluster", false, "use Cluster routing")
	protocol := flag.Int("protocol", 2, "RESP version")
	key := flag.String("key", "", "migrated key")
	value := flag.String("value", "", "expected migrated value")
	flag.Parse()
	if redis.Version() != "9.22.0" {
		panic("migration acceptance requires go-redis 9.22.0")
	}
	var client redis.UniversalClient
	if *cluster {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{*address}, Password: *password, Protocol: *protocol,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr: *address, Password: *password, Protocol: *protocol,
		})
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := client.Get(ctx, *key).Result()
	if err != nil || got != *value {
		panic(fmt.Sprintf("migrated GET: value=%q error=%v", got, err))
	}
	if err := client.Set(ctx, *key, "after-cutover", 0).Err(); err != nil {
		panic(err)
	}
	if got, err = client.Get(ctx, *key).Result(); err != nil || got != "after-cutover" {
		panic(fmt.Sprintf("cutover GET: value=%q error=%v", got, err))
	}
	fmt.Println("go-redis cutover passed")
}
