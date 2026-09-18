// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const storageFormat = "lavik-etcd-raft-v1"

type genesis struct {
	Format  string   `json:"format"`
	Local   Member   `json:"local"`
	Initial []Member `json:"initial"`
}

// diskStore belongs exclusively to the append executor after startup. Neither
// RawNode nor a network/Bycorf worker takes its WAL mutex or performs file I/O.
type diskStore struct {
	wal     *wal.WAL
	dir     string
	genesis genesis
	join    *joinSeed
	started bool
	hard    *pb.HardState
	gcCut   atomic.Uint64
	// Fault hooks run on the owner executor. Tests use entered/release barriers,
	// not scheduler-dependent sleeps, to hold real durability work pending.
	beforeSave func() error
	beforeIO   func(string) error
}

type recovered struct {
	hard     *pb.HardState
	entries  []*pb.Entry
	snapshot *pb.Snapshot
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	return errors.Join(err, f.Close())
}

func writeExclusive(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

func readBounded(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("oversized or non-regular recovery file: %s", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(data)) > limit {
		return nil, errors.New("recovery file grew beyond budget")
	}
	return data, err
}

func readSnapshot(path string) (*pb.Snapshot, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxSnapshotMessage {
		return nil, errors.New("oversized snapshot file")
	}
	return snap.Read(zap.NewNop(), path)
}

func openDisk(cfg Config) (*diskStore, recovered, error) {
	var rec recovered
	if err := cfg.Local.validate(); err != nil {
		return nil, rec, err
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, rec, err
	}
	dir := filepath.Join(cfg.Dir, "raft-v1")
	marker := filepath.Join(cfg.Dir, "RAFT")
	meta, err := readBounded(marker, 1<<20)
	fresh := errors.Is(err, os.ErrNotExist)
	if err != nil && !fresh {
		return nil, rec, err
	}
	if !fresh && len(cfg.Initial) != 0 {
		return nil, rec, errors.New("initial manifest is only valid on a pristine Meta directory")
	}
	if fresh {
		files, err := os.ReadDir(cfg.Dir)
		if err != nil {
			return nil, rec, err
		}
		// A missing marker must not turn a damaged or legacy directory into a
		// fresh election-eligible node. Only a stale administrative socket is
		// unrelated to durable state and may predate initialization.
		for _, f := range files {
			if f.Type()&os.ModeSocket == 0 {
				return nil, rec, fmt.Errorf("refusing non-pristine Meta directory: %s", f.Name())
			}
		}
		seen := map[uint64]bool{}
		for _, m := range cfg.Initial {
			if err := m.validate(); err != nil {
				return nil, rec, err
			}
			if seen[m.ID] {
				return nil, rec, errors.New("duplicate genesis member")
			}
			seen[m.ID] = true
		}
		if len(cfg.Initial) != 0 && (!seen[cfg.Local.ID] || len(cfg.Initial)%2 == 0 || len(cfg.Initial) > 5) {
			return nil, rec, errors.New("genesis requires 1, 3, or 5 voters including local member")
		}
		g := genesis{Format: storageFormat, Local: cfg.Local, Initial: cfg.Initial}
		meta, err = json.Marshal(g)
		if err != nil {
			return nil, rec, err
		}
		// Publish intent first. A crash during creation deliberately requires
		// operator recovery; it must never reopen genesis or joiner grace.
		if err = writeExclusive(marker, meta); err != nil {
			return nil, rec, err
		}
		if err = syncDirectory(cfg.Dir); err != nil {
			return nil, rec, err
		}
		if err = os.MkdirAll(filepath.Join(dir, "snap"), 0700); err != nil {
			return nil, rec, err
		}
		if err = syncDirectory(dir); err != nil {
			return nil, rec, err
		}
		if err = syncDirectory(cfg.Dir); err != nil {
			return nil, rec, err
		}
	}
	s := &diskStore{dir: dir, beforeSave: cfg.beforeSave, beforeIO: cfg.beforeIO}
	if err := json.Unmarshal(meta, &s.genesis); err != nil {
		return nil, rec, err
	}
	if s.genesis.Format != storageFormat || s.genesis.Local.ID != cfg.Local.ID || s.genesis.Local.Principal != cfg.Local.Principal {
		return nil, rec, errors.New("Meta storage format or local identity mismatch")
	}
	waldir := filepath.Join(dir, "wal")
	if !fresh {
		if err = s.readJoin(); err != nil {
			return nil, rec, err
		}
		var evidence []byte
		evidence, err = readBounded(filepath.Join(dir, "STARTED"), 64)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, rec, err
		}
		s.started = err == nil
		if s.started && string(evidence) != storageFormat {
			return nil, rec, errors.New("corrupt Raft startup evidence")
		}
	}
	lg := zap.NewNop() // Synchronous log sinks must not become a control dependency.
	if fresh {
		s.wal, err = wal.Create(lg, waldir, meta)
	} else {
		var markers []*walpb.Snapshot
		markers, err = wal.ValidSnapshotEntries(lg, waldir)
		if err == nil {
			// Require the newest published snapshot. Falling back after its
			// corruption can silently lose state once covered WAL is reclaimed.
			sort.Slice(markers, func(i, j int) bool { return markers[i].GetIndex() > markers[j].GetIndex() })
			cut := &walpb.Snapshot{}
			if len(markers) > 0 {
				cut = markers[0]
			}
			if cut.GetIndex() > 0 {
				name := fmt.Sprintf("%016x-%016x.snap", cut.GetTerm(), cut.GetIndex())
				rec.snapshot, err = readSnapshot(filepath.Join(dir, "snap", name))
				if err == nil && (rec.snapshot.Metadata.GetIndex() != cut.GetIndex() || rec.snapshot.Metadata.GetTerm() != cut.GetTerm() || !proto.Equal(rec.snapshot.Metadata.GetConfState(), cut.GetConfState())) {
					err = errors.New("snapshot does not match WAL publication")
				}
			}
			if err == nil {
				s.wal, err = wal.Open(lg, waldir, cut)
			}
		}
		if err == nil {
			var saved []byte
			saved, rec.hard, rec.entries, err = s.wal.ReadAll()
			if err == nil && !bytes.Equal(saved, meta) {
				err = errors.New("WAL genesis differs from RAFT marker")
			}
			if err == nil && (s.started == (raft.IsEmptyHardState(rec.hard) && len(rec.entries) == 0 && raft.IsEmptySnap(rec.snapshot))) {
				err = errors.New("missing or contradictory Raft startup evidence")
			}
		}
	}
	if err != nil {
		if s.wal != nil {
			_ = s.wal.Close()
		}
		return nil, rec, fmt.Errorf("Meta WAL recovery: %w", err)
	}
	s.hard = rec.hard
	if !raft.IsEmptySnap(rec.snapshot) {
		if err := s.wal.ReleaseLockTo(rec.snapshot.Metadata.GetIndex()); err != nil {
			_ = s.wal.Close()
			return nil, rec, err
		}
		s.gcCut.Store(rec.snapshot.Metadata.GetIndex())
	}
	return s, rec, nil
}

// save completes only once all prior append tasks and this task are durable.
// Response-only tasks are barriers too: no fast path may bypass an older sync.
func (s *diskStore) save(m *pb.Message) error { return s.savePrepared(m, false) }

func (s *diskStore) savePrepared(m *pb.Message, prepared bool) error {
	if s.beforeSave != nil {
		if err := s.beforeSave(); err != nil {
			return err
		}
	}
	if !s.started {
		if err := writeExclusive(filepath.Join(s.dir, "STARTED"), []byte(storageFormat)); err != nil {
			return err
		}
		if err := syncDirectory(s.dir); err != nil {
			return err
		}
		s.started = true
	}
	if !prepared && !raft.IsEmptySnap(m.Snapshot) {
		if err := s.prepareSnapshot(m.Snapshot); err != nil {
			return err
		}
	}
	hs := &pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
	if err := s.fault("wal-write"); err != nil {
		return err
	}
	if err := s.wal.Save(hs, m.Entries); err != nil {
		return err
	}
	if !raft.IsEmptyHardState(hs) {
		s.hard = hs
	}
	// WAL.Save may skip fsync for a commit-only update. We promise a durable
	// prefix for every completion, including the persisted commit watermark.
	if err := s.fault("wal-sync"); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	if !raft.IsEmptySnap(m.Snapshot) {
		return s.publishSnapshotMarker(m.Snapshot)
	}
	return nil
}

func (s *diskStore) publishSnapshot(image *pb.Snapshot) error {
	if err := s.prepareSnapshot(image); err != nil {
		return err
	}
	return s.publishSnapshotMarker(image)
}

// prepareSnapshot runs on the bulk executor for a local capture. Publishing
// the WAL marker belongs to the ordered append executor and is a separate step.
func (s *diskStore) prepareSnapshot(image *pb.Snapshot) error {
	dir := filepath.Join(s.dir, "snap")
	// etcd's snapshot encoder provides the checksum. Stage in a private
	// directory so SaveSnap cannot truncate an already published image.
	stage, err := os.MkdirTemp(dir, "prepare-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err = s.fault("snapshot-file"); err != nil {
		return err
	}
	if err = snap.New(zap.NewNop(), stage).SaveSnap(image); err != nil {
		return err
	}
	name := fmt.Sprintf("%016x-%016x.snap", image.Metadata.GetTerm(), image.Metadata.GetIndex())
	if _, err = os.Stat(filepath.Join(dir, name)); err == nil {
		existing, readErr := readSnapshot(filepath.Join(dir, name))
		if readErr != nil {
			return readErr
		}
		if !proto.Equal(existing, image) {
			return errors.New("contradictory snapshot at same term/index")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err = s.fault("snapshot-rename"); err != nil {
			return err
		}
		if err = os.Rename(filepath.Join(stage, name), filepath.Join(dir, name)); err != nil {
			return err
		}
	} else {
		return err
	}
	if err = s.fault("snapshot-directory"); err != nil {
		return err
	}
	if err = syncDirectory(dir); err != nil {
		return err
	}
	crashPoint("meta-snapshot-after-file")
	return nil
}

func (s *diskStore) publishSnapshotMarker(image *pb.Snapshot) error {
	index := image.Metadata.GetIndex()
	if index <= s.gcCut.Load() {
		return nil
	}
	if s.hard.GetCommit() < index {
		return errors.New("snapshot publication overtook durable commit")
	}
	if err := s.fault("snapshot-publication"); err != nil {
		return err
	}
	if err := s.wal.SaveSnapshot(&walpb.Snapshot{Index: image.Metadata.Index, Term: image.Metadata.Term, ConfState: image.Metadata.ConfState}); err != nil {
		return err
	}
	crashPoint("meta-snapshot-after-marker")
	// Only the ordered append owner touches WAL locks. Bulk reclamation uses
	// its own file handles and cannot hold the WAL mutex during unlink/fsync.
	if err := s.wal.ReleaseLockTo(index); err != nil {
		return err
	}
	s.gcCut.Store(index)
	return nil
}

func (s *diskStore) fault(stage string) error {
	if s.beforeIO != nil {
		return s.beforeIO(stage)
	}
	return nil
}
