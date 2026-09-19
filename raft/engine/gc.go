// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

// reclaim deletes only whole, unlocked WAL segments strictly before the
// snapshot's boundary segment. Keeping the predecessor matches etcd's recovery
// convention even when a segment starts exactly at the snapshot index.
func (s *diskStore) reclaim(cut uint64) error {
	if cut == 0 {
		return nil
	}
	if err := s.fault("wal-gc"); err != nil {
		return err
	}
	waldir := filepath.Join(s.dir, "wal")
	files, err := os.ReadDir(waldir)
	if err != nil {
		return err
	}
	var covered []string
	for _, f := range files {
		var sequence, index uint64
		if !strings.HasSuffix(f.Name(), ".wal") {
			continue
		}
		if _, err := fmt.Sscanf(f.Name(), "%016x-%016x.wal", &sequence, &index); err != nil {
			return err
		}
		if index < cut {
			covered = append(covered, f.Name())
		}
	}
	sort.Strings(covered)
	for i := 0; i+1 < len(covered); i++ {
		path := filepath.Join(waldir, covered[i])
		locked, err := fileutil.TryLockFile(path, os.O_WRONLY, 0600)
		if errors.Is(err, fileutil.ErrLocked) {
			continue
		}
		if err != nil {
			return err
		}
		err = os.Remove(path)
		closeErr := locked.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	if len(covered) > 1 {
		if err := syncDirectory(waldir); err != nil {
			return err
		}
	}
	// Keep the current image and the newest older image. Orphans newer than
	// the published cut are harmless and must not be mistaken for a root.
	dir := filepath.Join(s.dir, "snap")
	if err := s.fault("snapshot-gc"); err != nil {
		return err
	}
	files, err = os.ReadDir(dir)
	if err != nil {
		return err
	}
	type oldSnapshot struct {
		index uint64
		name  string
	}
	var older []oldSnapshot
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".snap") {
			continue
		}
		var term, index uint64
		if _, err := fmt.Sscanf(f.Name(), "%016x-%016x.snap", &term, &index); err != nil {
			return err
		}
		if index < cut {
			older = append(older, oldSnapshot{index, f.Name()})
		}
	}
	sort.Slice(older, func(i, j int) bool { return older[i].index < older[j].index })
	for i := 0; i+1 < len(older); i++ {
		if err := os.Remove(filepath.Join(dir, older[i].name)); err != nil {
			return err
		}
	}
	if len(older) > 1 {
		return syncDirectory(dir)
	}
	return nil
}

func (r *Runtime) gcLoop() {
	defer r.workers.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			// Reclamation failure preserves the durable root and is retryable.
			// Expose it instead of hiding growing physical disk consumption.
			if err := r.disk.reclaim(r.disk.gcCut.Load()); err != nil {
				r.gcFailures.Add(1)
			} else {
				r.gcFailures.Store(0)
			}
		}
	}
}
