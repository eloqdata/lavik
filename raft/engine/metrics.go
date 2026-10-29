// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// Upstream collectors are process scoped, even in tests hosting several Raft
// runtimes. Preserve their names, types and buckets; never attribute them to a
// particular runtime or expose unused etcd server/backend collectors.
var upstreamMetrics = map[string]string{
	"etcd_disk_wal_fsync_duration_seconds":                  "The latency distributions of fsync called by WAL.",
	"etcd_disk_wal_write_duration_seconds":                  "The latency distributions of write called by WAL.",
	"etcd_disk_wal_write_bytes_total":                       "Total number of bytes written in WAL.",
	"etcd_debugging_snap_save_marshalling_duration_seconds": "The marshalling cost distributions of save called by snapshot.",
	"etcd_debugging_snap_save_total_duration_seconds":       "The total latency distributions of save called by snapshot.",
	"etcd_snap_fsync_duration_seconds":                      "The latency distributions of fsync called by snap.",
}

// etcd registers each private collector in DefaultRegisterer. The public
// AlreadyRegisteredError API lets us reuse those exact collectors in a narrow
// registry without observing unrelated Go/process collectors or mutating the
// default registry. Descriptor contracts are pinned to our etcd version. A
// missing/changed descriptor is an export error, never a replacement zero.
func upstreamRegistry(registerer prometheus.Registerer) (prometheus.Gatherer, error) {
	registry := prometheus.NewRegistry()
	for name, help := range upstreamMetrics {
		probe := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		err := registerer.Register(probe)
		var existing prometheus.AlreadyRegisteredError
		if !errors.As(err, &existing) {
			if err == nil {
				registerer.Unregister(probe)
			}
			return nil, errors.New("upstream metric descriptor unavailable: " + name)
		}
		if err := registry.Register(existing.ExistingCollector); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

var upstreamGatherer, upstreamError = upstreamRegistry(prometheus.DefaultRegisterer)

// MaxMetricsBytes bounds the owned text buffer returned through the scrape ABI.
const MaxMetricsBytes = 8 << 20

var metricEpoch = time.Now()
var durationBuckets = []float64{.0001, .0005, .001, .005, .01, .05, .1, .3, 1, 5, 30, 120}

const (
	stagePersist = iota
	stageCapture
	stageInstall
	stagePrepare
	stagePublish
	stageCount
)

type operationMetric struct {
	duration prometheus.Observer
	results  [2]prometheus.Counter
	// Exactly one executor writes each stage. An active start uses the same
	// monotonic epoch as collection; it never depends on wall-clock changes.
	started atomic.Int64
}

func (m *operationMetric) begin() time.Time {
	now := time.Now()
	m.started.Store(now.Sub(metricEpoch).Nanoseconds() + 1)
	return now
}

func (m *operationMetric) end(start time.Time, err error) {
	m.duration.Observe(time.Since(start).Seconds())
	result := 0
	if err != nil {
		result = 1
	}
	m.results[result].Inc()
	m.started.Store(0)
}

type runtimeMetrics struct {
	registry       *prometheus.Registry
	operations     [stageCount]operationMetric
	walErrors      [3]prometheus.Counter
	gcErrors       prometheus.Counter
	snapshotErrors prometheus.Counter
}

func newRuntimeMetrics() *runtimeMetrics {
	m := &runtimeMetrics{registry: prometheus.NewRegistry()}
	durations := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "lavik_meta_raft_operation_duration_seconds", Help: "Executor operation duration, including the named stage's encoding or durability work; excludes queue wait.", Buckets: durationBuckets}, []string{"stage"})
	results := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lavik_meta_raft_operations_total", Help: "Completed executor operations by stage and result."}, []string{"stage", "result"})
	errors := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lavik_meta_raft_wal_errors_total", Help: "Failed WAL API operations, including failures injected before the operation."}, []string{"stage"})
	m.gcErrors = prometheus.NewCounter(prometheus.CounterOpts{Name: "lavik_meta_raft_gc_errors_total", Help: "Failed reclamation attempts; successful retries do not reset this counter."})
	m.snapshotErrors = prometheus.NewCounter(prometheus.CounterOpts{Name: "lavik_meta_raft_snapshot_errors_total", Help: "Failed local snapshot rounds; successful retries do not reset this counter."})
	m.registry.MustRegister(durations, results, errors, m.gcErrors, m.snapshotErrors)
	for i, name := range operationStages {
		m.operations[i].duration = durations.WithLabelValues(name)
		m.operations[i].results[0] = results.WithLabelValues(name, "success")
		m.operations[i].results[1] = results.WithLabelValues(name, "error")
	}
	for i, name := range []string{"save", "sync", "snapshot_marker"} {
		m.walErrors[i] = errors.WithLabelValues(name)
	}
	return m
}

var operationStages = [stageCount]string{"persist", "capture", "install", "prepare", "publish"}

type runtimeCollector struct{ r *Runtime }

// A fixed slot belongs to one currently authorized peer/lane. Reconnects keep
// its counters; membership removal drops the slot and all its exported labels.
type laneMetrics struct {
	in, out                                       atomic.Bool
	dialErrors, readErrors, writeErrors, rejected prometheus.Counter
}

var laneNames = [laneCount]string{"control", "log", "snapshot"}

func (m *laneMetrics) initialize(id uint64, lane string) {
	labels := prometheus.Labels{"peer_id": strconv.FormatUint(id, 10), "lane": lane}
	m.rejected = prometheus.NewCounter(prometheus.CounterOpts{Name: "lavik_meta_raft_lane_queue_rejections_total", Help: "Outbound queue count or byte-budget rejections.", ConstLabels: labels})
	counter := func(stage string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: "lavik_meta_raft_lane_errors_total", Help: "Connection or IO failures for a known peer, including EOF and incomplete-frame deadlines; excludes idle read deadlines.", ConstLabels: prometheus.Labels{"peer_id": labels["peer_id"], "lane": lane, "stage": stage}})
	}
	m.dialErrors, m.readErrors, m.writeErrors = counter("connect"), counter("read"), counter("write")
	// These prebound collectors follow the immutable peer table's lifetime.
	// Membership never registers/unregisters while a scrape holds registry locks.
}

func (n *Network) collectMetrics(ch chan<- prometheus.Metric) {
	for id, peer := range n.peers.Load().peers {
		for lane, name := range laneNames {
			m := &peer.metrics[lane]
			labels := []string{"peer_id", strconv.FormatUint(id, 10), "lane", name}
			emit(ch, "lane_connected", "Current authorized connection; log and snapshot lanes connect on demand.", prometheus.GaugeValue, boolMetric(m.in.Load()), append(labels, "direction", "in")...)
			emit(ch, "lane_connected", "Current authorized connection; log and snapshot lanes connect on demand.", prometheus.GaugeValue, boolMetric(m.out.Load()), append(labels, "direction", "out")...)
			emit(ch, "lane_queued_messages", "Queued outbound messages excluding the current socket write.", prometheus.GaugeValue, float64(len(peer.queues[lane])), labels...)
			emit(ch, "lane_queue_capacity", "Maximum queued outbound messages.", prometheus.GaugeValue, float64(cap(peer.queues[lane])), labels...)
			m.rejected.Collect(ch)
			m.dialErrors.Collect(ch)
			m.readErrors.Collect(ch)
			m.writeErrors.Collect(ch)
		}
	}
}

// This is an unchecked collector: membership changes the bounded peer series.
// Collect only reads already-published status and atomic transport statistics.
func (c runtimeCollector) Describe(chan<- *prometheus.Desc) {}

func emit(ch chan<- prometheus.Metric, name, help string, typ prometheus.ValueType, value float64, labels ...string) {
	names, values := make([]string, 0, len(labels)/2), make([]string, 0, len(labels)/2)
	for i := 0; i < len(labels); i += 2 {
		names = append(names, labels[i])
		values = append(values, labels[i+1])
	}
	ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("lavik_meta_raft_"+name, help, names, nil), typ, value, values...)
}

func (c runtimeCollector) Collect(ch chan<- prometheus.Metric) {
	r := c.r
	s := r.status.Load()
	if s == nil {
		return
	}
	gauge := func(name, help string, value uint64) { emit(ch, name, help, prometheus.GaugeValue, float64(value)) }
	gauge("member_id", "Local Meta member identity.", r.cfg.Local.ID)
	gauge("term", "Sampled Raft term; not an authority certificate.", s.Term)
	gauge("leader_id", "Sampled leader hint; zero means unknown.", s.Leader)
	gauge("commit_index", "Raft committed index.", s.Commit)
	gauge("applied_index", "Actually completed C++ application index, including configuration and no-op entries.", s.Applied)
	gauge("durable_index", "Proven durable prefix of the current log branch; may regress on replacement.", s.Durable)
	gauge("snapshot_index", "Published and installed local snapshot index.", s.Snapshot)
	gauge("first_log_index", "First retained logical log index.", s.FirstIndex)
	gauge("config_index", "Applied configuration index.", s.ConfigIndex)
	gauge("retained_log_bytes", "Retained logical log bytes, not filesystem usage.", s.RetainedBytes)
	gauge("pending_durability_bytes", "Bytes reserved by outstanding durability work.", s.PendingBytes)
	gauge("gc_consecutive_failures", "Consecutive reclamation failures; resets on success.", s.GCFailures)
	gauge("snapshot_consecutive_failures", "Consecutive local snapshot failures; resets on success.", s.SnapshotFailures)
	emit(ch, "status_age_seconds", "Age of the protocol owner's immutable observation.", prometheus.GaugeValue, time.Since(s.Published).Seconds())
	emit(ch, "leadership_validity_seconds", "Maximum heartbeat evidence validity interval.", prometheus.GaugeValue, r.livenessWindow().Seconds())
	emit(ch, "learner", "Whether the local member is a learner.", prometheus.GaugeValue, boolMetric(s.Learners[r.cfg.Local.ID]))
	for _, role := range []string{"StateFollower", "StateCandidate", "StateLeader", "StatePreCandidate"} {
		emit(ch, "role", "Protocol role, independent of Lavik leader authority.", prometheus.GaugeValue, boolMetric(s.ProtocolRole == role), "role", role)
	}
	emit(ch, "peer_send_failures_total", "Outbound dial or write failures; excludes inbound protocol errors.", prometheus.CounterValue, float64(s.RPCFailures))
	emit(ch, "vote_responses_total", "Vote and pre-vote responses handed to transport, not delivery acknowledgements.", prometheus.CounterValue, float64(s.VoteGrants), "result", "granted")
	emit(ch, "vote_responses_total", "Vote and pre-vote responses handed to transport, not delivery acknowledgements.", prometheus.CounterValue, float64(s.VoteRejections), "result", "rejected")
	for _, member := range s.Members {
		id := strconv.FormatUint(member.ID, 10)
		emit(ch, "member", "Current member directory and learner status.", prometheus.GaugeValue, 1, "peer_id", id, "kind", memberKind(s.Learners[member.ID]))
		if member.ID == r.cfg.Local.ID || s.ProtocolRole != "StateLeader" {
			continue
		}
		if p, ok := s.Progress[member.ID]; ok {
			for name, value := range map[string]uint64{"match_index": p.Match, "next_index": p.Next, "pending_snapshot_index": p.PendingSnapshot} {
				emit(ch, "peer_"+name, "Leader-observed replication progress; not peer application progress.", prometheus.GaugeValue, float64(value), "peer_id", id)
			}
			for _, state := range []string{"StateProbe", "StateReplicate", "StateSnapshot"} {
				emit(ch, "peer_replication_state", "Leader replication state for a current peer.", prometheus.GaugeValue, boolMetric(p.State == state), "peer_id", id, "state", state)
			}
		}
		age, known := s.PeerAgeMicros[member.ID]
		ageSeconds := float64(age)/1e6 + time.Since(s.Published).Seconds()
		_, valid := s.PeerApplied[member.ID]
		fresh := known && valid && ageSeconds < r.livenessWindow().Seconds()
		emit(ch, "peer_fresh", "Valid current-term heartbeat evidence at the last publication, aged at collection.", prometheus.GaugeValue, boolMetric(fresh), "peer_id", id)
		if known {
			emit(ch, "peer_heartbeat_age_seconds", "Age measured from challenge send, not reply receipt.", prometheus.GaugeValue, ageSeconds, "peer_id", id)
		}
		if fresh {
			emit(ch, "peer_applied_index", "Actual peer application index from valid heartbeat evidence.", prometheus.GaugeValue, float64(s.PeerApplied[member.ID]), "peer_id", id)
		}
	}
	for i, name := range operationStages {
		start := r.disk.metrics.operations[i].started.Load()
		age := float64(0)
		if start != 0 {
			age = float64(time.Since(metricEpoch).Nanoseconds()-start+1) / 1e9
		}
		emit(ch, "operation_inflight", "Whether the stage's executor is processing an operation.", prometheus.GaugeValue, boolMetric(start != 0), "stage", name)
		emit(ch, "operation_age_seconds", "Monotonic age of the active operation; zero when idle.", prometheus.GaugeValue, max(0, age), "stage", name)
	}
	if n, ok := r.transport.(*Network); ok {
		n.collectMetrics(ch)
	}
}

func boolMetric(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
func memberKind(learner bool) string {
	if learner {
		return "learner"
	}
	return "voter"
}

// Do not embed bytes.Buffer: expfmt detects WriteString/WriteByte and would
// bypass the response cap through those promoted methods.
type metricsBuffer struct{ buffer bytes.Buffer }

func (b *metricsBuffer) Len() int      { return b.buffer.Len() }
func (b *metricsBuffer) Bytes() []byte { return b.buffer.Bytes() }

func (b *metricsBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > MaxMetricsBytes {
		return 0, errors.New("Meta metrics exceed response budget")
	}
	return b.buffer.Write(p)
}

// CollectMetrics runs only on the scrape consumer. A nil runtime exposes
// upstream process metrics during recovery. It never calls RawNode, storage,
// application callbacks, or the protocol mailbox. The caller serializes and
// drains these calls before closing the runtime.
func CollectMetrics(r *Runtime) ([]byte, error) {
	var out metricsBuffer
	encoder := expfmt.NewEncoder(&out, expfmt.NewFormat(expfmt.TypeTextPlain))
	if upstreamError != nil {
		return nil, upstreamError
	}
	families, err := upstreamGatherer.Gather()
	if err != nil {
		return nil, err
	}
	for _, family := range families {
		if upstreamMetrics[family.GetName()] != "" {
			if err = encoder.Encode(family); err != nil {
				return nil, err
			}
		}
	}
	if r != nil {
		families, err = r.disk.metrics.registry.Gather()
		if err != nil {
			return nil, err
		}
		for _, family := range families {
			if err = encoder.Encode(family); err != nil {
				return nil, err
			}
		}
	}
	return out.Bytes(), nil
}
