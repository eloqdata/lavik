// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	pb "go.etcd.io/raft/v3/raftpb"
)

func metricFamilies(t *testing.T, r *Runtime) map[string]*dto.MetricFamily {
	t.Helper()
	data, err := CollectMetrics(r)
	if err != nil {
		t.Fatal(err)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return families
}

func sample(t *testing.T, families map[string]*dto.MetricFamily, name string, labels ...string) *dto.Metric {
	t.Helper()
	for _, metric := range families[name].GetMetric() {
		found := true
		for i := 0; i < len(labels); i += 2 {
			matched := false
			for _, label := range metric.Label {
				if label.GetName() == labels[i] && label.GetValue() == labels[i+1] {
					matched = true
				}
			}
			found = found && matched
		}
		if found {
			return metric
		}
	}
	t.Fatalf("missing %s %v", name, labels)
	return nil
}

func TestUpstreamMetricContractAndRealStoragePaths(t *testing.T) {
	before := metricFamilies(t, nil)
	disk, _, err := openDisk(storageConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	writeEntries(t, disk, 1, 4, 1)
	if err = disk.publishSnapshot(testImage(t, 4, 1, []byte("snapshot"))); err != nil {
		t.Fatal(err)
	}
	after := metricFamilies(t, nil)
	if len(after) != 6 {
		t.Fatalf("exposed unapproved process families: %v", after)
	}
	for name := range upstreamMetrics {
		family := after[name]
		if name == "etcd_disk_wal_write_bytes_total" {
			if family.GetType() != dto.MetricType_GAUGE || family.Metric[0].GetGauge().GetValue() <= before[name].Metric[0].GetGauge().GetValue() {
				t.Fatal("WAL byte gauge contract or trigger changed")
			}
			continue
		}
		if family.GetType() != dto.MetricType_HISTOGRAM {
			t.Fatal(name, "type changed")
		}
		h := family.Metric[0].Histogram
		if h.GetSampleCount() <= before[name].Metric[0].Histogram.GetSampleCount() {
			t.Fatal(name, "not exercised by Lavik storage")
		}
		// The text parser includes the implicit infinity bucket.
		if len(h.Bucket) != 15 {
			t.Fatalf("%s bucket count %d", name, len(h.Bucket))
		}
		for i := 0; i < 14; i++ {
			if h.Bucket[i].GetUpperBound() != .001*math.Pow(2, float64(i)) {
				t.Fatal(name, "changed bucket", i)
			}
		}
	}
}

func TestCollectorUsesPublishedStatusAndOmitsUnknownApplied(t *testing.T) {
	r := &Runtime{cfg: Config{Local: testMembers()[0], Heartbeat: time.Second, ElectionTicks: 3}, disk: &diskStore{metrics: newRuntimeMetrics()}}
	r.disk.metrics.registry.MustRegister(runtimeCollector{r})
	status := &Status{Published: time.Now(), ProtocolRole: "StateLeader", Role: Role{Term: 7}, Members: testMembers(), Learners: map[uint64]bool{3: true}, PeerApplied: map[uint64]uint64{2: 99}, PeerAgeMicros: map[uint64]uint64{2: 1, 3: 9_000_000}, Progress: map[uint64]PeerProgress{2: {Match: 101, Next: 102, State: "StateReplicate"}}}
	r.status.Store(status)
	families := metricFamilies(t, r)
	if sample(t, families, "lavik_meta_raft_peer_applied_index", "peer_id", "2").GetGauge().GetValue() != 99 {
		t.Fatal("lost application evidence")
	}
	if len(families["lavik_meta_raft_peer_applied_index"].Metric) != 1 {
		t.Fatal("unknown peer application filled with zero")
	}
	if sample(t, families, "lavik_meta_raft_peer_fresh", "peer_id", "3").GetGauge().GetValue() != 0 {
		t.Fatal("expired peer fresh")
	}
	if sample(t, families, "lavik_meta_raft_peer_heartbeat_age_seconds", "peer_id", "3").GetGauge().GetValue() < 9 {
		t.Fatal("lost expired evidence age")
	}
	// Never mutate a published object. Age can invalidate it without asking its owner.
	expired := *status
	expired.Published = time.Now().Add(-4 * time.Second)
	r.status.Store(&expired)
	if metricFamilies(t, r)["lavik_meta_raft_peer_applied_index"] != nil {
		t.Fatal("stale applied evidence exported")
	}
	follower := *status
	follower.ProtocolRole = "StateFollower"
	r.status.Store(&follower)
	if metricFamilies(t, r)["lavik_meta_raft_peer_match_index"] != nil {
		t.Fatal("follower exported leader-only progress")
	}
	removed := *status
	removed.Members = removed.Members[:1]
	r.status.Store(&removed)
	families = metricFamilies(t, r)
	if len(families["lavik_meta_raft_member"].Metric) != 1 || families["lavik_meta_raft_peer_fresh"] != nil {
		t.Fatal("removed member series survived")
	}
}

func TestSnapshotAndGCCumulativeFailuresSurviveSuccess(t *testing.T) {
	var failSnapshot, failGC atomic.Bool
	nodes := openTestCluster(t, func(i int, cfg *Config) Application {
		cfg.beforeIO = func(stage string) error {
			if (stage == "snapshot-file" && failSnapshot.Load()) || (stage == "wal-gc" && failGC.Load()) {
				return errors.New("injected")
			}
			return nil
		}
		return &testApp{}
	}, nil)
	leader := clusterLeader(t, nodes)
	appliedProposal(t, leader, "snapshot input")
	failSnapshot.Store(true)
	result, err := leader.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := <-result; got.Err == nil {
		t.Fatal("expected failed prepare")
	}
	eventually(t, "snapshot failure publication", func() bool { return leader.Status().SnapshotFailures > 0 })
	before := sample(t, metricFamilies(t, leader), "lavik_meta_raft_snapshot_errors_total").GetCounter().GetValue()
	failSnapshot.Store(false)
	failGC.Store(true)
	result, err = leader.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := <-result; got.Err != nil {
		t.Fatal(got.Err)
	}
	eventually(t, "successful snapshot clears streak", func() bool { return leader.Status().SnapshotFailures == 0 })
	eventually(t, "GC failure publication", func() bool { return leader.Status().GCFailures > 0 })
	gcBefore := sample(t, metricFamilies(t, leader), "lavik_meta_raft_gc_errors_total").GetCounter().GetValue()
	failGC.Store(false)
	eventually(t, "successful GC clears streak", func() bool { return leader.Status().GCFailures == 0 })
	families := metricFamilies(t, leader)
	if before < 1 || sample(t, families, "lavik_meta_raft_snapshot_errors_total").GetCounter().GetValue() != before {
		t.Fatal("snapshot cumulative failure reset")
	}
	if gcBefore < 1 || sample(t, families, "lavik_meta_raft_gc_errors_total").GetCounter().GetValue() < gcBefore {
		t.Fatal("GC cumulative failure reset")
	}
	if sample(t, families, "lavik_meta_raft_operations_total", "stage", "prepare", "result", "error").GetCounter().GetValue() < 1 {
		t.Fatal("failed prepare missing")
	}
}

func TestMetricsBufferBound(t *testing.T) {
	var b metricsBuffer
	if _, err := b.Write([]byte(strings.Repeat("x", MaxMetricsBytes))); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write([]byte("x")); err == nil || b.Len() != MaxMetricsBytes {
		t.Fatal("response grew beyond bound")
	}
}

// Exercise expfmt's enhanced-writer selection, not just a direct Write call.
func TestEncoderCannotBypassMetricsResponseBound(t *testing.T) {
	var b metricsBuffer
	encoder := expfmt.NewEncoder(&b, expfmt.NewFormat(expfmt.TypeTextPlain))
	family := &dto.MetricFamily{Name: new("oversized"), Help: new(strings.Repeat("x", MaxMetricsBytes)), Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: new(1.0)}}}}
	if err := encoder.Encode(family); err == nil || b.Len() > MaxMetricsBytes {
		t.Fatal("encoder escaped response bound")
	}
}

func TestWALErrorsIdentifyOnlyTheFailedBoundary(t *testing.T) {
	disk, _, err := openDisk(storageConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	writeEntries(t, disk, 1, 4, 1)
	r := &Runtime{disk: disk}
	for _, stage := range []struct{ fault, label string }{{"wal-write", "save"}, {"wal-sync", "sync"}, {"snapshot-publication", "snapshot_marker"}} {
		disk.beforeIO = func(point string) error {
			if point == stage.fault {
				return errors.New("injected")
			}
			return nil
		}
		if stage.label == "snapshot_marker" {
			err = disk.writeSnapshotMarker(testImage(t, 4, 1, []byte("image")))
		} else {
			err = disk.saveWAL(nil, nil)
		}
		if err == nil {
			t.Fatal("fault not reached", stage)
		}
		if sample(t, metricFamilies(t, r), "lavik_meta_raft_wal_errors_total", "stage", stage.label).GetCounter().GetValue() != 1 {
			t.Fatal("wrong failure boundary", stage)
		}
	}
	disk.beforeIO = nil
	if err = disk.saveWAL(nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{"save", "sync", "snapshot_marker"} {
		if sample(t, metricFamilies(t, r), "lavik_meta_raft_wal_errors_total", "stage", label).GetCounter().GetValue() != 1 {
			t.Fatal("successful retry reset a counter")
		}
	}
}

func TestFailedGatherNeverReturnsHealthyZeroText(t *testing.T) {
	original := upstreamGatherer
	defer func() { upstreamGatherer = original }()
	upstreamGatherer = prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return nil, errors.New("collection failure") })
	if data, err := CollectMetrics(nil); err == nil || data != nil {
		t.Fatal("failed collection returned a valid body")
	}
}

func TestMissingUpstreamCollectorIsNotReplacedWithZero(t *testing.T) {
	empty := prometheus.NewRegistry()
	if _, err := upstreamRegistry(empty); err == nil {
		t.Fatal("missing upstream accepted")
	}
	families, err := empty.Gather()
	if err != nil || len(families) != 0 {
		t.Fatal("descriptor probe left a fake metric behind")
	}
}

// A bad unrelated default collector must not be called by our narrow interface.
type unrelatedCollector struct{ called atomic.Bool }

func (c *unrelatedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("unrelated", "Not supported", nil, nil)
}
func (c *unrelatedCollector) Collect(ch chan<- prometheus.Metric) {
	c.called.Store(true)
	ch <- prometheus.NewInvalidMetric(prometheus.NewDesc("unrelated", "Not supported", nil, nil), errors.New("not used"))
}
func TestExcludedCollectorsAreNeverCollected(t *testing.T) {
	c := &unrelatedCollector{}
	prometheus.MustRegister(c)
	defer prometheus.Unregister(c)
	metricFamilies(t, nil)
	if c.called.Load() {
		t.Fatal("scrape called an excluded collector")
	}
}

// A replaced socket's deferred cleanup must not clear its successor's gauge.
// Exercise the real authorized member handshake and receive ownership.
func TestLaneMetricsSurviveConnectionReplacementAndDropRemovedMembers(t *testing.T) {
	members := testMembers()[:2]
	networks := make([]*Network, 2)
	for i := range networks {
		n, err := NewNetwork(Config{Local: members[i], Listen: "127.0.0.1:0"})
		if err != nil {
			t.Fatal(err)
		}
		networks[i] = n
		t.Cleanup(n.Close)
		members[i].Raft = n.listener.Addr().String()
	}
	for _, n := range networks {
		n.Update(members)
		n.Start(func(*pb.Message) error { return nil })
	}
	sender, receiver := networks[0], networks[1]
	peer := receiver.peers.Load().peers[members[0].ID]
	dial := func() net.Conn {
		conn, err := sender.dial(members[1], laneControl)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sender.untrack(conn); _ = conn.Close() })
		return conn
	}
	first := dial()
	eventually(t, "first inbound connection", func() bool { return peer.metrics[laneControl].in.Load() })
	second := dial()
	eventually(t, "old reader retired", func() bool {
		var metric dto.Metric
		_ = peer.metrics[laneControl].readErrors.Write(&metric)
		return metric.GetCounter().GetValue() >= 1
	})
	if !peer.metrics[laneControl].in.Load() {
		t.Fatal("old socket cleared replacement connection")
	}
	_ = first.Close()
	r := &Runtime{cfg: Config{Local: members[1]}, transport: receiver, disk: &diskStore{metrics: newRuntimeMetrics()}}
	r.status.Store(&Status{Published: time.Now(), Members: members})
	r.disk.metrics.registry.MustRegister(runtimeCollector{r})
	if sample(t, metricFamilies(t, r), "lavik_meta_raft_lane_connected", "peer_id", "1", "lane", "control", "direction", "in").GetGauge().GetValue() != 1 {
		t.Fatal("replacement not exported")
	}
	_ = second.Close()
	eventually(t, "last reader retired", func() bool { return !peer.metrics[laneControl].in.Load() })
	// The send byte budget rejects without changing the authorized connection.
	if !receiver.sendBytes[laneControl].take(laneBudget(laneControl), laneBudget(laneControl)) {
		t.Fatal("nonempty budget")
	}
	if receiver.Send(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1))}) {
		t.Fatal("full budget admitted message")
	}
	receiver.sendBytes[laneControl].release(laneBudget(laneControl))
	if sample(t, metricFamilies(t, r), "lavik_meta_raft_lane_queue_rejections_total", "peer_id", "1", "lane", "control").GetCounter().GetValue() != 1 {
		t.Fatal("queue rejection lost")
	}
	receiver.Update(members[1:])
	if metricFamilies(t, r)["lavik_meta_raft_lane_connected"] != nil {
		t.Fatal("removed lane still exported")
	}
}

// Observe the receive loop's deadline installation so the test can expire it
// after writing exactly the desired portion of a frame, without timing sleeps.
type readDeadlineConn struct {
	net.Conn
	deadlines chan struct{}
}

func (c *readDeadlineConn) SetReadDeadline(deadline time.Time) error {
	err := c.Conn.SetReadDeadline(deadline)
	c.deadlines <- struct{}{}
	return err
}

func TestLaneReadErrorsExcludeOnlyIdleHeaderDeadlines(t *testing.T) {
	for lane, name := range laneNames {
		for _, tc := range []struct {
			name         string
			frame        []byte
			end          string
			wantFailures float64
		}{
			{"idle-deadline", nil, "deadline", 0},
			{"partial-header-deadline", []byte{0, 0}, "deadline", 1},
			{"body-deadline", []byte{0, 0, 0, 2}, "deadline", 1},
			{"partial-body-deadline", []byte{0, 0, 0, 2, 0}, "deadline", 1},
			{"header-eof", nil, "eof", 1},
			{"partial-header-eof", []byte{0, 0}, "eof", 1},
			{"body-eof", []byte{0, 0, 0, 2}, "eof", 1},
			{"closed-connection", nil, "close", 1},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				members := testMembers()[:2]
				n, err := NewNetwork(Config{Local: members[1], Listen: "127.0.0.1:0"})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(n.Close)
				n.Update(members)
				client, server := net.Pipe()
				conn := &readDeadlineConn{Conn: server, deadlines: make(chan struct{}, 2)}
				done := make(chan struct{})
				go func() { n.receive(conn); close(done) }()
				t.Cleanup(func() { _ = client.Close(); _ = server.Close(); <-done })
				_ = client.SetDeadline(time.Now().Add(5 * time.Second))

				// Authenticate a current member before testing ordinary frame reads.
				var hello [40]byte
				copy(hello[:], "LRT1")
				hello[4], hello[5] = byte(lane), 1
				binary.BigEndian.PutUint64(hello[8:16], members[0].ID)
				binary.BigEndian.PutUint64(hello[16:24], members[1].ID)
				if err := writeAll(client, append(hello[:], 0, 0, 0, 2, '{', '}')); err != nil {
					t.Fatal(err)
				}
				var ack [1]byte
				if _, err := io.ReadFull(client, ack[:]); err != nil || ack[0] != 1 {
					t.Fatalf("handshake: ack=%v err=%v", ack, err)
				}
				// Wait for both the handshake and frame deadlines, then advance only
				// the latter. The production lane-specific timeout stays unchanged.
				for range 2 {
					select {
					case <-conn.deadlines:
					case <-time.After(5 * time.Second):
						t.Fatal("receive deadline was not installed")
					}
				}
				if err := writeAll(client, tc.frame); err != nil {
					t.Fatal(err)
				}
				switch tc.end {
				case "deadline":
					_ = server.SetReadDeadline(time.Now().Add(-time.Second))
				case "eof":
					_ = client.Close()
				case "close":
					_ = server.Close()
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("receive did not retire the connection")
				}
				metrics := &n.peers.Load().peers[members[0].ID].metrics[lane]
				var metric dto.Metric
				if err := metrics.readErrors.Write(&metric); err != nil {
					t.Fatal(err)
				}
				if got := metric.GetCounter().GetValue(); got != tc.wantFailures {
					t.Fatalf("read failures=%v, want %v", got, tc.wantFailures)
				}
				if metrics.in.Load() {
					t.Fatal("retired connection remains active")
				}
			})
		}
	}
}
