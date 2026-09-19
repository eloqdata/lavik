// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/storage/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type configurationApplyBarrier struct {
	testApp
	entered chan struct{}
	release chan struct{}
}

func (a *configurationApplyBarrier) Advance(index uint64) {
	if index == 4 {
		close(a.entered)
		<-a.release
	}
}

func TestSnapshotRestoreSupersedesPendingConfigurationApplication(t *testing.T) {
	for _, entryType := range []pb.EntryType{pb.EntryConfChange, pb.EntryConfChangeV2} {
		for _, kind := range []pb.ConfChangeType{pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode, pb.ConfChangeRemoveNode} {
			t.Run(entryType.String()+"/"+kind.String(), func(t *testing.T) {
				members := testMembers()
				app := &configurationApplyBarrier{entered: make(chan struct{}), release: make(chan struct{})}
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(app.release) }) }
				cfg := Config{Local: members[1], Initial: members, Dir: t.TempDir(), Heartbeat: time.Hour, ElectionTicks: 3}
				r, err := Open(cfg, app, &generationLink{}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { unblock(); r.Close() })
				eventually(t, "bootstrap applied", func() bool { return r.Status().Applied == 3 })

				member := Member{ID: 4, Raft: "127.0.0.1:7104", Data: "127.0.0.1:7304", Admin: "127.0.0.1:7204", Principal: "lavik://meta/4"}
				if kind == pb.ConfChangeRemoveNode {
					member = members[2]
				}
				descriptor, err := json.Marshal(member)
				if err != nil {
					t.Fatal(err)
				}
				var change proto.Message = &pb.ConfChange{Type: kind.Enum(), NodeId: new(member.ID), Context: descriptor}
				if entryType == pb.EntryConfChangeV2 {
					change = &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: kind.Enum(), NodeId: new(member.ID)}}, Context: descriptor}
				}
				data, err := proto.Marshal(change)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Step(&pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)), Index: new(uint64(3)), LogTerm: new(uint64(1)), Commit: new(uint64(4)), Entries: []*pb.Entry{{Type: entryType.Enum(), Index: new(uint64(4)), Term: new(uint64(2)), Data: data}}}); err != nil {
					t.Fatal(err)
				}
				select {
				case <-app.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("configuration application did not reach barrier")
				}
				image := testImage(t, 10, 3, []byte("newer application image"))
				if err := r.Step(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: image}); err != nil {
					t.Fatal(err)
				}
				eventually(t, "RawNode accepted snapshot while apply is blocked", func() bool { return r.Status().Commit == 10 })
				if s := r.Status(); s.Applied != 3 || s.Snapshot != 0 {
					t.Fatalf("uninstalled snapshot escaped application barrier: %+v", s)
				}
				unblock()
				eventually(t, "snapshot installed", func() bool { return r.Status().Snapshot == 10 && r.Status().Applied == 10 })
				r.Close() // Join the sole protocol owner before inspecting RawNode.
				installed, err := r.core.memory.Snapshot()
				if err != nil || installed.Metadata.GetIndex() != 10 || installed.Metadata.GetTerm() != 3 || !bytes.Equal(installed.Data, image.Data) || installed.Metadata.ConfState.Equivalent(image.Metadata.ConfState) != nil || r.core.conf.Equivalent(image.Metadata.ConfState) != nil {
					t.Fatalf("installed snapshot/configuration mismatch: %v", err)
				}
				actual := r.core.raw.Status().Config
				voters := slices.Sorted(maps.Keys(actual.Voters.IDs()))
				if !slices.Equal(voters, []uint64{1, 2, 3}) || len(actual.Learners) != 0 || len(actual.LearnersNext) != 0 {
					t.Fatalf("stale apply changed restored RawNode configuration: %+v (published: %v)", actual, r.core.conf)
				}
			})
		}
	}
}

func pendingConfiguration(t *testing.T, c *core) *pb.Entry {
	t.Helper()
	member := Member{ID: 4, Raft: "127.0.0.1:7104", Data: "127.0.0.1:7304", Admin: "127.0.0.1:7204", Principal: "lavik://meta/4"}
	descriptor, err := json.Marshal(member)
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(&pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(4))}}, Context: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	entry := &pb.Entry{Type: pb.EntryConfChangeV2.Enum(), Index: new(uint64(2)), Term: new(uint64(2)), Data: data}
	stepCore(t, c, &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(2)), Entries: []*pb.Entry{entry, {Index: new(uint64(3)), Term: new(uint64(2))}}})
	finishAppend(t, c, appendTask(t, c))
	return entry
}

func TestRestoringSnapshotDefersCaptureAndAllowsLaterConfiguration(t *testing.T) {
	c := testCore(t, 2)
	entry := pendingConfiguration(t, c)
	stepCore(t, c, &pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: testImage(t, 5, 3, nil)})
	install := appendTask(t, c)
	if err := c.appliedEntry(entry); err != nil {
		t.Fatal(err)
	}
	waiter := make(chan Result, 1)
	r := &Runtime{core: c, snapshotWaiter: waiter}
	r.maybeCapture()
	if len(r.applyQueue) != 0 || r.snapshotInProgress {
		t.Fatal("capture crossed pending snapshot's application/configuration barrier")
	}
	select {
	case result := <-waiter:
		t.Fatalf("snapshot request completed before installation: %+v", result)
	default:
	}
	finishAppend(t, c, install)
	r.maybeCapture()
	select {
	case result := <-waiter:
		if result.Index != 5 || result.Err != nil {
			t.Fatalf("wrong snapshot request completion: %+v", result)
		}
	default:
		t.Fatal("installed snapshot stranded the waiting capture request")
	}
	// The fence belongs to the covered prefix, not a term or a permanent
	// suppression of configuration application after installing an image.
	later := proto.Clone(entry).(*pb.Entry)
	later.Index, later.Term = new(uint64(6)), new(uint64(3))
	stepCore(t, c, &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(3)), Commit: new(uint64(6)), Entries: []*pb.Entry{later}})
	finishAppend(t, c, appendTask(t, c))
	if err := c.appliedEntry(later); err != nil {
		t.Fatal(err)
	}
	want := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	if c.conf.Equivalent(want) != nil || len(c.members) != 4 {
		t.Fatalf("configuration beyond installed snapshot was suppressed: %v", c.conf)
	}
	r.maybeCapture()
	if len(r.applyQueue) != 1 || r.applyQueue[0].capture.metadata.GetIndex() != 6 || r.applyQueue[0].capture.metadata.ConfState.Equivalent(want) != nil {
		t.Fatal("later capture did not bind the new application/configuration cut")
	}
}

func TestIgnoredSnapshotPreservesPendingConfigurationApplication(t *testing.T) {
	for _, reason := range []string{"excluded-local", "matching-log"} {
		t.Run(reason, func(t *testing.T) {
			c := testCore(t, 2)
			entry := pendingConfiguration(t, c)
			image := testImage(t, 5, 3, nil)
			image.Metadata.ConfState.Voters = []uint64{1, 3}
			members := []Member{testMembers()[0], testMembers()[2]}
			if reason == "matching-log" {
				image.Metadata.Index, image.Metadata.Term = new(uint64(3)), new(uint64(2))
				image.Metadata.ConfState.Voters = []uint64{1, 2, 3, 4}
				var change pb.ConfChangeV2
				if err := proto.Unmarshal(entry.Data, &change); err != nil {
					t.Fatal(err)
				}
				var added Member
				if err := json.Unmarshal(change.Context, &added); err != nil {
					t.Fatal(err)
				}
				members = append(testMembers(), added)
			}
			var err error
			image.Data, err = encodeSnapshot(members, nil)
			if err != nil {
				t.Fatal(err)
			}
			stepCore(t, c, &pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: image})
			finishAppend(t, c, appendTask(t, c))
			if err := c.appliedEntry(entry); err != nil {
				t.Fatal(err)
			}
			if c.conf.Equivalent(&pb.ConfState{Voters: []uint64{1, 2, 3, 4}}) != nil || len(c.members) != 4 {
				t.Fatalf("ignored snapshot suppressed a valid configuration: %v", c.conf)
			}
		})
	}
}

func TestReceivedSnapshotCrashRecovery(t *testing.T) {
	for _, base := range []string{"log-only", "published-snapshot"} {
		for _, suffix := range []string{"none", "entries"} {
			t.Run(base+"/"+suffix, func(t *testing.T) {
				stages := []string{"snapshot-file", "snapshot-rename", "snapshot-directory", "snapshot-publication", "wal-write", "wal-sync", "snapshot-release", "completed"}
				if suffix == "entries" {
					stages = append(stages, "wal-write-suffix", "wal-sync-suffix", "partial-suffix")
				}
				for _, stage := range stages {
					t.Run(stage, func(t *testing.T) {
						cfg := storageConfig(t)
						cfg.Heartbeat = time.Hour
						r, err := Open(cfg, &testApp{}, &generationLink{}, nil, nil)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(r.Close)
						eventually(t, "durable bootstrap", func() bool { return r.Status().Applied == 3 })
						r.Close()
						cfg.Initial = nil
						if base == "published-snapshot" {
							disk, _, err := openDisk(cfg)
							if err != nil {
								t.Fatal(err)
							}
							err = disk.publishSnapshot(testImage(t, 3, 1, []byte("previous image")))
							closeErr := disk.wal.Close()
							if err != nil || closeErr != nil {
								t.Fatal(errors.Join(err, closeErr))
							}
						}
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReceivedSnapshotCrashHelper$")
						child.Env = append(os.Environ(), "LAVIK_TEST_SNAPSHOT_CRASH_DIR="+cfg.Dir, "LAVIK_TEST_SNAPSHOT_CRASH_STAGE="+stage, "LAVIK_TEST_SNAPSHOT_CRASH_SUFFIX="+suffix)
						out, err := child.CombinedOutput()
						var exited *exec.ExitError
						if !errors.As(err, &exited) || exited.ExitCode() != 86 {
							t.Fatalf("crash cut was not reached: %v\n%s", err, out)
						}
						r, err = Open(cfg, &testApp{}, &generationLink{}, nil, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer r.Close()
						wantCommit := uint64(3)
						switch stage {
						case "wal-sync", "wal-write-suffix", "partial-suffix":
							wantCommit = 10
						case "wal-sync-suffix", "snapshot-release", "completed":
							wantCommit = 10
							if suffix == "entries" {
								wantCommit = 12
							}
						}
						eventually(t, "recovery application published", func() bool { return r.Status().Applied == wantCommit })
						s := r.Status()
						if s.Commit != wantCommit || s.Applied != s.Commit || (s.Commit >= 10 && s.Snapshot != 10) {
							t.Fatalf("recovered commit has no complete application root: %+v", s)
						}
						if stage == "partial-suffix" && s.Durable != 11 {
							t.Fatalf("partial uncommitted suffix was not exercised: %+v", s)
						}
					})
				}
			})
		}
	}
}

// The child exits inside the real store operation without WAL.Close or deferred
// cleanup, so a graceful flush cannot conceal an incomplete durable handoff.
func TestReceivedSnapshotCrashHelper(t *testing.T) {
	dir := os.Getenv("LAVIK_TEST_SNAPSHOT_CRASH_DIR")
	if dir == "" {
		return
	}
	stage := os.Getenv("LAVIK_TEST_SNAPSHOT_CRASH_STAGE")
	// Force rotation while publishing the new image and its HardState.
	wal.SegmentSizeBytes = 256
	cfg := Config{Local: testMembers()[0], Dir: dir}
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	oldCut := disk.gcCut.Load()
	message := &pb.Message{Term: new(uint64(2)), Commit: new(uint64(10)), Snapshot: testImage(t, 10, 2, []byte("received image"))}
	if os.Getenv("LAVIK_TEST_SNAPSHOT_CRASH_SUFFIX") == "entries" {
		message.Commit = new(uint64(12))
		message.Entries = []*pb.Entry{{Index: new(uint64(11)), Term: new(uint64(2))}, {Index: new(uint64(12)), Term: new(uint64(2))}}
	}
	visits := map[string]int{}
	disk.beforeIO = func(point string) error {
		visits[point]++
		atCut := point == stage
		if strings.HasSuffix(stage, "-suffix") {
			atCut = point == strings.TrimSuffix(stage, "-suffix") && visits[point] == 2
		}
		if stage == "partial-suffix" && point == "wal-write" && visits[point] == 2 {
			// Model a complete first suffix record reaching disk before the
			// rest of WAL.Save, including its final HardState, can be written.
			if err := disk.wal.Save(&pb.HardState{}, message.Entries[:1]); err != nil {
				t.Fatal(err)
			}
			atCut = true
		}
		if atCut {
			if disk.gcCut.Load() != oldCut {
				t.Fatalf("GC advanced before snapshot transaction completed: %d", disk.gcCut.Load())
			}
			if err := disk.reclaim(oldCut); err != nil {
				t.Fatal(err)
			}
			os.Exit(86)
		}
		return nil
	}
	if err := disk.save(message); err != nil {
		t.Fatal(err)
	}
	if stage != "completed" || disk.gcCut.Load() != 10 {
		t.Fatalf("missing crash cut or unpublished GC frontier: stage=%s cut=%d", stage, disk.gcCut.Load())
	}
	if err := disk.reclaim(disk.gcCut.Load()); err != nil {
		t.Fatal(err)
	}
	os.Exit(86)
}
