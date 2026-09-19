// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/etcd/server/v3/storage/wal"
	pb "go.etcd.io/raft/v3/raftpb"
)

func storageConfig(t *testing.T) Config {
	t.Helper()
	members := testMembers()
	return Config{Local: members[0], Initial: members, Dir: t.TempDir(), ElectionTicks: 3, MaxPendingBytes: 4 << 20}
}

func testImage(t *testing.T, index, term uint64, app []byte) *pb.Snapshot {
	t.Helper()
	data, err := encodeSnapshot(testMembers(), app)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: new(index), Term: new(term), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
}

func writeEntries(t *testing.T, disk *diskStore, first, last, term uint64) {
	t.Helper()
	for index := first; index <= last; index++ {
		entry := &pb.Entry{Index: new(index), Term: new(term), Data: bytes.Repeat([]byte{byte(index)}, 256)}
		if err := disk.save(&pb.Message{Term: new(term), Commit: new(index), Entries: []*pb.Entry{entry}}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWALRotationSnapshotReclamationAndTailRecovery(t *testing.T) {
	// No test in this package runs in parallel. Restore the upstream test seam
	// only after WAL.Close has joined its preallocation pipeline.
	old := wal.SegmentSizeBytes
	wal.SegmentSizeBytes = 2048
	defer func() { wal.SegmentSizeBytes = old }()
	cfg := storageConfig(t)
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, disk, 1, 120, 1)
	files, _ := filepath.Glob(filepath.Join(cfg.Dir, "wal", "*.wal"))
	if len(files) < 3 {
		t.Fatal("fixture did not rotate WAL")
	}
	for _, cut := range []uint64{80, 90, 100, 110} {
		if err := disk.publishSnapshot(testImage(t, cut, 1, []byte("image"))); err != nil {
			t.Fatal(err)
		}
	}
	if err := disk.reclaim(110); err != nil {
		t.Fatal(err)
	}
	after, _ := filepath.Glob(filepath.Join(cfg.Dir, "wal", "*.wal"))
	if len(after) >= len(files) {
		t.Fatal("no covered segment reclaimed")
	}
	images, _ := filepath.Glob(filepath.Join(cfg.Dir, "snap", "*.snap"))
	if len(images) != 2 {
		t.Fatalf("want current and previous images, got %d", len(images))
	}
	if err := disk.wal.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Initial = nil
	disk, rec, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	if rec.snapshot.Metadata.GetIndex() != 110 || rec.hard.GetCommit() != 120 || len(rec.entries) != 10 {
		t.Fatalf("wrong recovery cut: snap=%d hard=%v entries=%d", rec.snapshot.Metadata.GetIndex(), rec.hard, len(rec.entries))
	}
	for i, entry := range rec.entries {
		if entry.GetIndex() != 111+uint64(i) || !bytes.Equal(entry.Data, bytes.Repeat([]byte{byte(111 + i)}, 256)) {
			t.Fatal("suffix changed during compaction")
		}
	}
}

func TestPreparedSnapshotDoesNotAdvanceRecoveryRoot(t *testing.T) {
	cfg := storageConfig(t)
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, disk, 1, 10, 1)
	if err = disk.publishSnapshot(testImage(t, 5, 1, []byte("published"))); err != nil {
		t.Fatal(err)
	}
	if err = disk.prepareSnapshot(testImage(t, 10, 1, []byte("unpublished"))); err != nil {
		t.Fatal(err)
	}
	if err = disk.reclaim(disk.gcCut.Load()); err != nil {
		t.Fatal(err)
	}
	disk.wal.Close()
	cfg.Initial = nil
	disk, rec, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	if rec.snapshot.Metadata.GetIndex() != 5 || len(rec.entries) != 5 {
		t.Fatal("unpublished image became a recovery root")
	}
}

func TestRecoveryRemovesAbandonedSnapshotStages(t *testing.T) {
	cfg := storageConfig(t)
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, disk, 1, 10, 1)
	if err = disk.publishSnapshot(testImage(t, 5, 1, []byte("published"))); err != nil {
		t.Fatal(err)
	}
	if err = disk.prepareSnapshot(testImage(t, 10, 1, []byte("unpublished"))); err != nil {
		t.Fatal(err)
	}
	if err = disk.wal.Close(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(disk.dir, "snap")
	name := fmt.Sprintf("%016x-%016x.snap", 1, 10)
	prepared, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	// Crashes before the image rename and after it can leave populated or
	// empty staging directories. Neither is a published recovery root.
	stages := []string{filepath.Join(dir, "prepare-before-rename"), filepath.Join(dir, "prepare-after-rename")}
	for _, stage := range stages {
		if err := os.Mkdir(stage, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(stages[0], name), prepared, 0600); err != nil {
		t.Fatal(err)
	}
	note := filepath.Join(dir, "prepare-operator-note")
	if err := os.WriteFile(note, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Initial = nil
	disk, rec, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	for _, stage := range stages {
		if _, err := os.Stat(stage); !os.IsNotExist(err) {
			t.Fatalf("abandoned snapshot stage survived recovery: %s: %v", stage, err)
		}
	}
	if rec.snapshot.Metadata.GetIndex() != 5 || rec.hard.GetCommit() != 10 || len(rec.entries) != 5 {
		t.Fatal("staging cleanup changed the published recovery root or WAL suffix")
	}
	if data, err := os.ReadFile(filepath.Join(dir, name)); err != nil || !bytes.Equal(data, prepared) {
		t.Fatalf("staging cleanup changed the renamed, unpublished image: %v", err)
	}
	if data, err := os.ReadFile(note); err != nil || string(data) != "keep" {
		t.Fatalf("staging cleanup changed an unrelated file: %v", err)
	}
}

func TestRecoveryFailsClosedOnMissingOrCorruptEvidence(t *testing.T) {
	for _, kind := range []string{"marker", "started", "started-content", "wal", "snapshot-missing", "snapshot-checksum", "wal-checksum", "genesis-mismatch"} {
		t.Run(kind, func(t *testing.T) {
			cfg := storageConfig(t)
			disk, _, err := openDisk(cfg)
			if err != nil {
				t.Fatal(err)
			}
			writeEntries(t, disk, 1, 10, 1)
			if err = disk.publishSnapshot(testImage(t, 5, 1, []byte("old"))); err != nil {
				t.Fatal(err)
			}
			image := testImage(t, 10, 1, []byte("new"))
			if err = disk.publishSnapshot(image); err != nil {
				t.Fatal(err)
			}
			disk.wal.Close()
			cfg.Initial = nil
			path := filepath.Join(disk.dir, "snap", fmt.Sprintf("%016x-%016x.snap", 1, 10))
			switch kind {
			case "marker":
				err = os.Remove(filepath.Join(cfg.Dir, "RAFT"))
			case "started":
				err = os.Remove(filepath.Join(disk.dir, "STARTED"))
			case "started-content":
				err = os.WriteFile(filepath.Join(disk.dir, "STARTED"), []byte("bad"), 0600)
			case "wal":
				err = os.RemoveAll(filepath.Join(disk.dir, "wal"))
			case "snapshot-missing":
				err = os.Remove(path)
			case "snapshot-checksum":
				err = os.WriteFile(path, []byte("bad snapshot"), 0600)
			case "genesis-mismatch":
				path = filepath.Join(cfg.Dir, "RAFT")
				data, _ := os.ReadFile(path)
				data = bytes.Replace(data, []byte("7100"), []byte("9100"), 1)
				err = os.WriteFile(path, data, 0600)
			case "wal-checksum":
				files, _ := filepath.Glob(filepath.Join(disk.dir, "wal", "*.wal"))
				f, openErr := os.OpenFile(files[0], os.O_RDWR, 0600)
				if openErr != nil {
					t.Fatal(openErr)
				}
				_, err = f.WriteAt([]byte{0xff, 0xfe, 0xfd}, 64)
				f.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			bad, _, err := openDisk(cfg)
			if err == nil {
				bad.wal.Close()
				t.Fatal("damaged directory accepted or silently fell back")
			}
		})
	}
}

func TestPristineWaitingAndRestartClassification(t *testing.T) {
	cfg := storageConfig(t)
	cfg.Initial = nil
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disk.wal.Close()
	disk, rec, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if disk.started || len(rec.entries) > 0 || len(disk.genesis.Initial) > 0 {
		t.Fatal("waiting joiner became election eligible")
	}
	disk.wal.Close()
	cfg.Initial = testMembers()
	if d, _, err := openDisk(cfg); err == nil {
		d.wal.Close()
		t.Fatal("restart accepted a new genesis")
	}
	// Losing RAFT must not let partial durable state bootstrap a fresh voter.
	for _, existing := range []string{"STARTED", "JOIN", "wal", "snap", "raft_log.dat", "cluster_config.dat", "snapshot_1.dat", "unrelated"} {
		t.Run(existing, func(t *testing.T) {
			cfg := storageConfig(t)
			path := filepath.Join(cfg.Dir, existing)
			var err error
			if existing == "wal" || existing == "snap" {
				err = os.Mkdir(path, 0700)
			} else {
				err = os.WriteFile(path, nil, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if d, _, err := openDisk(cfg); err == nil {
				d.wal.Close()
				t.Fatal("non-pristine directory accepted as fresh genesis")
			}
		})
	}
}

func TestWALSuffixReplacementSurvivesRestart(t *testing.T) {
	cfg := storageConfig(t)
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeEntries(t, disk, 1, 3, 1)
	// The abandoned suffix is uncommitted. Replacing it must neither retain
	// its tail after restart nor overwrite the committed prefix.
	for i := uint64(4); i <= 8; i++ {
		if err := disk.save(&pb.Message{Term: new(uint64(1)), Commit: new(uint64(3)), Entries: []*pb.Entry{{Index: new(i), Term: new(uint64(1)), Data: []byte("old")}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := disk.save(&pb.Message{Term: new(uint64(2)), Commit: new(uint64(4)), Entries: []*pb.Entry{{Index: new(uint64(4)), Term: new(uint64(2)), Data: []byte("replacement")}}}); err != nil {
		t.Fatal(err)
	}
	disk.wal.Close()
	cfg.Initial = nil
	disk, rec, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	if len(rec.entries) != 4 || string(rec.entries[3].Data) != "replacement" || rec.hard.GetTerm() != 2 {
		t.Fatal("abandoned suffix resurrected")
	}
}

func TestSnapshotCannotOvertakeDurableCommit(t *testing.T) {
	cfg := storageConfig(t)
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	writeEntries(t, disk, 1, 3, 1)
	if err := disk.publishSnapshot(testImage(t, 4, 1, nil)); err == nil {
		t.Fatal("published an uncommitted snapshot")
	}
	if disk.gcCut.Load() != 0 {
		t.Fatal("invalid publication enabled GC")
	}
}
