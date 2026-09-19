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
	"strings"
	"sync/atomic"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// STARTED distinguishes an untouched waiting joiner from a node whose Raft
// state was lost. Its fixed contents also detect interrupted marker writes.
const startedEvidence = "started\n"

type genesis struct {
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
	// Meta owns one current layout directly in its data directory. RAFT is
	// durable identity/bootstrap evidence, not a storage-version selector.
	dir := cfg.Dir
	marker := filepath.Join(dir, "RAFT")
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
		// A missing marker must not turn an existing or damaged directory into a
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
		g := genesis{Local: cfg.Local, Initial: cfg.Initial}
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
	}
	s := &diskStore{dir: dir, beforeSave: cfg.beforeSave, beforeIO: cfg.beforeIO}
	if err := json.Unmarshal(meta, &s.genesis); err != nil {
		return nil, rec, err
	}
	if s.genesis.Local.ID != cfg.Local.ID || s.genesis.Local.Principal != cfg.Local.Principal {
		return nil, rec, errors.New("Meta local identity mismatch")
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
		if s.started && string(evidence) != startedEvidence {
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
	if err := s.removeSnapshotStages(); err != nil {
		_ = s.wal.Close()
		return nil, rec, fmt.Errorf("Meta snapshot staging cleanup: %w", err)
	}
	return s, rec, nil
}

// Cleanup runs only after WAL ownership and recovery validation, before any
// executors start. A crash can bypass prepareSnapshot's deferred cleanup; these
// private directories never contain a published root. Leave final image names
// and unrelated files alone, including images renamed before WAL publication.
func (s *diskStore) removeSnapshotStages() error {
	dir := filepath.Join(s.dir, "snap")
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	removed := false
	for _, file := range files {
		if !file.IsDir() || !strings.HasPrefix(file.Name(), "prepare-") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, file.Name())); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return syncDirectory(dir)
	}
	return nil
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
		if err := writeExclusive(filepath.Join(s.dir, "STARTED"), []byte(startedEvidence)); err != nil {
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
	if !raft.IsEmptySnap(m.Snapshot) {
		index := m.Snapshot.Metadata.GetIndex()
		if hs.GetCommit() < index {
			return errors.New("received snapshot has no covering HardState commit")
		}
		// Recovery accepts a marker only when the WAL commit covers it. Make
		// the prepared image and marker durable first, so a crash never leaves
		// a commit beyond the recoverable log/snapshot. Keep the old GC cut
		// until the entire append transaction has completed.
		if err := s.writeSnapshotMarker(m.Snapshot); err != nil {
			return err
		}
		if len(m.Entries) > 0 {
			// A torn suffix write must not leave entries beyond a log gap while
			// recovery still ignores the new marker. First anchor the snapshot
			// itself, then persist the suffix and its final commit normally.
			base := proto.Clone(hs).(*pb.HardState)
			base.Commit = new(index)
			if err := s.saveWAL(base, nil); err != nil {
				return err
			}
		}
	}
	if err := s.saveWAL(hs, m.Entries); err != nil {
		return err
	}
	if !raft.IsEmptySnap(m.Snapshot) {
		return s.releaseSnapshot(m.Snapshot.Metadata.GetIndex())
	}
	return nil
}

func (s *diskStore) saveWAL(hs *pb.HardState, entries []*pb.Entry) error {
	if err := s.fault("wal-write"); err != nil {
		return err
	}
	if err := s.wal.Save(hs, entries); err != nil {
		return err
	}
	// WAL.Save may skip fsync for a commit-only update. We promise a durable
	// prefix for every completion, including the persisted commit watermark.
	if err := s.fault("wal-sync"); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	if !raft.IsEmptyHardState(hs) {
		s.hard = hs
	}
	return nil
}

func (s *diskStore) publishSnapshot(image *pb.Snapshot) error {
	if err := s.prepareSnapshot(image); err != nil {
		return err
	}
	return s.publishSnapshotMarker(image)
}

// prepareSnapshot runs on the bulk executor for captured and received images.
// The WAL marker and covering HardState belong to the ordered append executor.
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
	if err := s.writeSnapshotMarker(image); err != nil {
		return err
	}
	return s.releaseSnapshot(index)
}

// A received marker may precede its covering HardState. Neither WAL locks nor
// the GC frontier can advance until both are durable.
func (s *diskStore) writeSnapshotMarker(image *pb.Snapshot) error {
	if err := s.fault("snapshot-publication"); err != nil {
		return err
	}
	if err := s.wal.SaveSnapshot(&walpb.Snapshot{Index: image.Metadata.Index, Term: image.Metadata.Term, ConfState: image.Metadata.ConfState}); err != nil {
		return err
	}
	crashPoint("meta-snapshot-after-marker")
	return nil
}

func (s *diskStore) releaseSnapshot(index uint64) error {
	if index <= s.gcCut.Load() {
		return nil
	}
	if s.hard.GetCommit() < index {
		return errors.New("snapshot reclamation overtook durable commit")
	}
	if err := s.fault("snapshot-release"); err != nil {
		return err
	}
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
