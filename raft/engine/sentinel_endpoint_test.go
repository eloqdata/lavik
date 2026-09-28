// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
package engine

import (
	"encoding/json"
	"testing"
)

func TestMemberSentinelTransportAndHostnames(t *testing.T) {
	m := Member{ID: 1, Principal: "lavik://meta/1", Raft: "127.0.0.1:7000", Data: "127.0.0.1:7001", Admin: "127.0.0.1:7002"}
	for _, address := range []string{"127.0.0.1:26379", "tcp://127.0.0.1:26379", "tls://meta.example:26379", "tls://[::1]:26379"} {
		m.Sentinel = address
		if err := m.validate(); err != nil {
			t.Errorf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"tls://0.0.0.0:26379", "tls://[::]:26379", "tls://meta.example:0", "udp://meta.example:26379", "tls://bad host:26379"} {
		m.Sentinel = address
		if err := m.validate(); err == nil {
			t.Errorf("accepted %s", address)
		}
	}
	m.Sentinel = "tls://meta.example:26379"
	m.Admin = "admin.example:7002"
	if err := m.validate(); err == nil {
		t.Fatal("opened control-plane DNS")
	}
}

func TestSentinelRoutesSurviveSnapshotAndRestart(t *testing.T) {
	for _, address := range []string{"127.0.0.1:26379", "tls://sentinel.example:26379", "tcp://[::1]:26379"} {
		t.Run(address, func(t *testing.T) {
			cfg := storageConfig(t)
			cfg.Local.Sentinel = "tls://127.0.0.1:26380"
			cfg.SentinelTransports = 3
			cfg.Initial[0].Sentinel = address
			disk, _, err := openDisk(cfg)
			if err != nil {
				t.Fatal(err)
			}
			data, err := encodeSnapshot(cfg.Initial, []byte("app"))
			if err != nil {
				t.Fatal(err)
			}
			image := testImage(t, 3, 1, nil)
			image.Data = data
			writeEntries(t, disk, 1, 3, 1)
			if err := disk.publishSnapshot(image); err != nil {
				t.Fatal(err)
			}
			disk.wal.Close()
			cfg.Initial = nil
			disk, restored, err := openDisk(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.wal.Close()
			if string(restored.snapshot.Data) != string(data) {
				t.Fatal("snapshot changed advertised route")
			}
			if disk.genesis.Initial[0].Sentinel != address {
				t.Fatal("genesis changed advertised route")
			}
		})
	}
}

func TestSentinelTransportRequiredOnRestartAndInvitation(t *testing.T) {
	cfg := storageConfig(t)
	cfg.Local.Sentinel = "tls://127.0.0.1:26380"
	cfg.Initial[0].Sentinel = "tls://sentinel.example:26379"
	cfg.SentinelTransports = 2
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disk.wal.Close()
	seed := joinSeed{Index: 1, Members: cfg.Initial}
	cfg.Initial = nil
	cfg.Local.Sentinel = "127.0.0.1:26380"
	cfg.SentinelTransports = 1
	if disk, _, err := openDisk(cfg); err == nil {
		disk.wal.Close()
		t.Fatal("TLS registration restarted plaintext-only")
	}
	if err := seed.validate(cfg.Local, 1); err == nil {
		t.Fatal("TLS invitation admitted plaintext-only listener")
	}
	cfg.SentinelTransports = 3
	disk, _, err = openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disk.wal.Close()
	if err := seed.validate(cfg.Local, 3); err != nil {
		t.Fatal("dual listener rejected", err)
	}
}

func TestTaggedSentinelPendingInvitationRecovery(t *testing.T) {
	cfg := storageConfig(t)
	cfg.Initial = nil
	cfg.Local.Sentinel = "tls://127.0.0.1:26380"
	cfg.SentinelTransports = 2
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	members := testMembers()
	members[0].Sentinel = "tls://sentinel.example:26379"
	seed := joinSeed{Index: 1, Members: members}
	data, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.saveJoin(data); err != nil {
		t.Fatal(err)
	}
	disk.wal.Close()
	disk, _, err = openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disk.wal.Close()
	cfg.SentinelTransports = 1
	if disk, _, err := openDisk(cfg); err == nil {
		disk.wal.Close()
		t.Fatal("pending TLS invitation allowed plaintext-only restart")
	}
}
