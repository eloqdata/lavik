// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { randomBytes } from "node:crypto";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AdminError, endpoint, identifier } from "./meta.mjs";
import { command, text, jsonReply, keyBytes, slot } from "./resp.mjs";

const now = () => new Date().toISOString();
const fields = (line) =>
  Object.fromEntries(
    line
      .split(" ")
      .slice(1)
      .map((field) => {
        const index = field.indexOf("=");
        return [field.slice(0, index), field.slice(index + 1)];
      }),
  );
const READ_COMMANDS = new Set(
  "PING INFO DBSIZE GET MGET EXISTS TYPE TTL PTTL STRLEN HGET HGETALL HSCAN HLEN LRANGE LLEN SSCAN SCARD SMEMBERS ZRANGE ZSCAN ZCARD XRANGE XLEN SCAN SLOWLOG MEMORY OBJECT CLUSTER".split(
    " ",
  ),
);
const WRITE_COMMANDS = new Set(
  "SET MSET DEL UNLINK EXPIRE PEXPIRE PERSIST HSET HDEL LPUSH RPUSH LPOP RPOP SADD SREM ZADD ZREM XADD XDEL INCR DECR INCRBY DECRBY APPEND".split(
    " ",
  ),
);

/** Shared fleet application boundary for HTTP and lavik-ctl's Unix protocol. */
export class Fleet {
  constructor(store, meta) {
    this.store = store;
    this.meta = meta;
    this.views = new Map();
    this.pendingViews = new Map();
    this.busy = new Set();
    this.samples = new Map();
  }
  async start() {
    // A process can disappear after sending a mutation but before recording
    // its reply. Startup only observes these jobs; it never resubmits them.
    await this.store.query(
      "UPDATE jobs SET state='uncertain',detail='Admin restarted; checking committed state' WHERE state='running'",
      [],
      "run",
    );
    this.timer = setInterval(() => this.tick().catch(() => {}), 2000);
    await this.tick();
  }
  async stop() {
    clearInterval(this.timer);
  }
  async audit(action, cluster, detail = "") {
    await this.store.query(
      "INSERT INTO audit(created_at,action,cluster_id,detail) VALUES(?,?,?,?)",
      [now(), action, cluster, detail],
      "run",
    );
  }
  async clusters() {
    return this.store.query("SELECT * FROM clusters ORDER BY name,id");
  }
  async cluster(id) {
    const cluster = await this.store.query(
      "SELECT * FROM clusters WHERE id=?",
      [identifier(id)],
      "get",
    );
    if (!cluster) throw new AdminError("Cluster not found", 404);
    return cluster;
  }
  async add(input) {
    const id = identifier(input.id, "cluster name");
    const name = typeof input.name === "string" ? input.name.trim() : id;
    if (!name || name.length > 100)
      throw new AdminError("Choose a cluster name of 1–100 characters");
    const seeds = input.seeds;
    if (!Array.isArray(seeds) || !seeds.length || seeds.length > 9)
      throw new AdminError("Supply 1–9 Meta seed addresses");
    seeds.forEach(endpoint);
    const profile = input.profile || "default";
    this.meta.profile({ profile });
    try {
      await this.store.query(
        "INSERT INTO clusters VALUES(?,?,?,?,?)",
        [id, name, JSON.stringify([...new Set(seeds)]), profile, now()],
        "run",
      );
    } catch (error) {
      if (error.message.includes("UNIQUE"))
        throw new AdminError("That cluster name already exists", 409);
      throw error;
    }
    await this.audit("cluster-added", id);
    return this.cluster(id);
  }
  async forget(id) {
    await this.cluster(id);
    const active = await this.store.query(
      "SELECT id FROM jobs WHERE cluster_id=? AND state IN ('queued','running','uncertain')",
      [id],
    );
    if (active.length)
      throw new AdminError(
        "Resolve active operations before removing the connection",
        409,
      );
    // Retain operation history; forgetting is only allowed for unused entries.
    const history = await this.store.query(
      "SELECT id FROM jobs WHERE cluster_id=? LIMIT 1",
      [id],
    );
    if (history.length)
      throw new AdminError(
        "This connection has operation history and must be retained",
        409,
      );
    await this.store.query("DELETE FROM clusters WHERE id=?", [id], "run");
    this.views.delete(id);
    await this.audit("cluster-forgotten", id);
    return { removed: id };
  }
  async view(id, fresh = false) {
    const cached = this.views.get(id);
    if (!fresh && cached && Date.now() - cached.time < 3000)
      return cached.value;
    if (this.pendingViews.has(id)) return this.pendingViews.get(id);
    const promise = (async () => {
      const cluster = await this.cluster(id);
      const status = await this.meta.status(cluster);
      const nodes = await this.meta.nodes(cluster, status);
      const value = {
        ...cluster,
        seeds: JSON.parse(cluster.seeds),
        status,
        nodes,
        observed_at: now(),
      };
      this.views.set(id, { value, time: Date.now() });
      return value;
    })();
    this.pendingViews.set(id, promise);
    try {
      return await promise;
    } finally {
      this.pendingViews.delete(id);
    }
  }
  async nodeRequest(cluster, nodes, nodeId, args) {
    const node = nodes.find((item) => item.node_id === nodeId);
    if (!node?.endpoint)
      throw new AdminError("Node has no reachable client endpoint", 503);
    try {
      return await command(node.endpoint, args, this.meta.profile(cluster));
    } catch (error) {
      // Follow only redirects within the authenticated Meta topology. Never
      // turn a server response into an arbitrary outbound connection.
      const moved = /^MOVED \d+ (.+)$/.exec(error.message);
      const owner =
        moved &&
        nodes.find(
          (item) => item.endpoint?.replace(/^(tcp|tls):\/\//, "") === moved[1],
        );
      if (owner && owner.node_id !== nodeId)
        return command(owner.endpoint, args, this.meta.profile(cluster));
      throw error;
    }
  }
  async metrics(id) {
    const view = await this.view(id);
    const cluster = await this.cluster(id);
    const nodes = [];
    for (let i = 0; i < view.nodes.length; i += 8) {
      nodes.push(
        ...(await Promise.all(
          view.nodes
            .slice(i, i + 8)
            .filter((n) => n.group_id)
            .map(async (node) => {
              try {
                const raw = text(
                  await this.nodeRequest(cluster, view.nodes, node.node_id, [
                    "INFO",
                  ]),
                );
                const info = Object.fromEntries(
                  raw
                    .split("\r\n")
                    .filter((line) => line.includes(":"))
                    .map((line) => {
                      const at = line.indexOf(":");
                      return [line.slice(0, at), line.slice(at + 1)];
                    }),
                );
                const key = `${id}/${node.node_id}`;
                const stamp = Date.now();
                const prev = this.samples.get(key);
                const total = Number(info.total_commands_processed || 0);
                const ops =
                  prev && total >= prev.total
                    ? ((total - prev.total) * 1000) / (stamp - prev.time)
                    : null;
                this.samples.set(key, { time: stamp, total });
                const keys = Object.entries(info)
                  .filter(([name]) => /^db\d+$/.test(name))
                  .reduce(
                    (sum, [, val]) =>
                      sum + Number(/keys=(\d+)/.exec(val)?.[1] || 0),
                    0,
                  );
                return {
                  node_id: node.node_id,
                  memory: Number(info.used_memory || 0),
                  clients: Number(info.connected_clients || 0),
                  ops,
                  keys,
                  info,
                };
              } catch (error) {
                return { node_id: node.node_id, error: error.message };
              }
            }),
        )),
      );
    }
    return { nodes, observed_at: now() };
  }
  async operations(id, after = "0") {
    if (!/^\d+$/.test(after)) throw new AdminError("Invalid operation cursor");
    const cluster = await this.cluster(id);
    const jobs = await this.store.query(
      "SELECT * FROM jobs WHERE cluster_id=? ORDER BY created_at DESC LIMIT 100",
      [id],
    );
    let meta = [],
      error;
    try {
      const raw = await this.meta.command(cluster, ["listops", after, "100"]);
      meta = raw
        .split(" ")
        .slice(2)
        .filter(Boolean)
        .map((row) => {
          const [id, sequence, state, kind, phase, detail] = row.split(":");
          return {
            id,
            sequence,
            state,
            kind: Buffer.from(kind, "hex").toString(),
            phase: Buffer.from(phase, "hex").toString(),
            detail: Buffer.from(detail, "hex").toString(),
          };
        });
    } catch (e) {
      error = e.message;
    }
    return {
      jobs,
      meta,
      error,
      next: meta.length === 100 ? meta.at(-1).sequence : null,
    };
  }
  async enqueue(id, kind, input, requestId) {
    const cluster = await this.cluster(id);
    if (!["failover", "replica-add", "replica-remove", "create"].includes(kind))
      throw new AdminError("Unsupported operation");
    const jobId = requestId || randomBytes(16).toString("hex");
    if (!/^[0-9a-f]{32}$/.test(jobId))
      throw new AdminError("Invalid operation request id");
    const existing = await this.store.query(
      "SELECT * FROM jobs WHERE id=?",
      [jobId],
      "get",
    );
    if (existing) {
      const retained = JSON.parse(existing.input);
      delete retained.deadline;
      delete retained.expectedRevision;
      if (
        existing.cluster_id !== id ||
        existing.kind !== kind ||
        JSON.stringify(retained) !== JSON.stringify(input)
      )
        throw new AdminError(
          "Operation id already belongs to another request",
          409,
        );
      return existing;
    }
    if (kind !== "create") identifier(input.group, "group");
    if (kind.startsWith("replica-")) {
      if (!/^[0-9a-f]{40}$/.test(input.node))
        throw new AdminError("Node ID must be 40 lowercase hex characters");
      if (kind === "replica-add") {
        if (!/^(tcp|tls):\/\//.test(input.endpoint))
          throw new AdminError(
            "Use tcp://IP:port or tls://IP:port for the node",
          );
        endpoint(input.endpoint.replace(/^(tcp|tls):\/\//, ""));
      }
    }
    if (
      kind === "create" &&
      (typeof input.manifest !== "string" || input.manifest.length > 65536)
    )
      throw new AdminError("Supply a cluster manifest, up to 64 KiB");
    const status = await this.meta.status(cluster);
    if (
      kind === "create"
        ? status.cluster_state !== "uninitialized"
        : status.cluster_state !== "created"
    )
      throw new AdminError(
        kind === "create"
          ? "Cluster has already been initialized"
          : "Cluster must finish initialization first",
        409,
      );
    if (
      kind !== "create" &&
      !status.groups.some((g) => g.group_id === input.group)
    )
      throw new AdminError("Group not found", 404);
    if (
      kind === "replica-remove" &&
      status.groups.some((g) => g.owner_node_id === input.node)
    )
      throw new AdminError(
        "Switch the primary with controlled failover before removing this node",
        409,
      );
    const member = status.data_nodes.find((n) => n.node_id === input.node);
    if (kind === "replica-add" && member?.group_id)
      throw new AdminError(
        "This node already belongs to a group; select an unassigned node",
        409,
      );
    if (kind === "replica-remove" && member?.group_id !== input.group)
      throw new AdminError(
        "This node is not a member of the selected group",
        409,
      );
    const prepared = { ...input, deadline: Date.now() + 300000 };
    if (kind === "replica-remove") {
      const group = fields(
        await this.meta.command(cluster, ["getgroup", input.group]),
      );
      prepared.expectedRevision = group.revision;
    }
    try {
      await this.store.query(
        "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
        [
          jobId,
          id,
          kind,
          JSON.stringify(prepared),
          "queued",
          "preflight",
          "",
          kind === "create" ? null : jobId,
          now(),
          now(),
        ],
        "run",
      );
    } catch (error) {
      if (error.message.includes("UNIQUE"))
        throw new AdminError(
          "Another operation needs to finish or be resolved for this cluster",
          409,
        );
      throw error;
    }
    await this.audit(kind, id, jobId);
    void this.tick().catch(() => {});
    return this.store.query("SELECT * FROM jobs WHERE id=?", [jobId], "get");
  }
  async update(job, state, step, detail = "") {
    await this.store.query(
      "UPDATE jobs SET state=?,step=?,detail=?,updated_at=? WHERE id=?",
      [state, step, detail, now(), job.id],
      "run",
    );
    this.views.delete(job.cluster_id);
  }
  async tick() {
    const jobs = await this.store.query(
      "SELECT * FROM jobs WHERE state IN ('queued','running','uncertain') ORDER BY created_at LIMIT 100",
    );
    await Promise.all(
      jobs.map(async (job) => {
        if (this.busy.has(job.id)) return;
        this.busy.add(job.id);
        try {
          job.state === "queued"
            ? await this.execute(job)
            : await this.observe(job);
        } catch (error) {
          await this.update(
            job,
            job.state === "queued" ? "queued" : "uncertain",
            job.step,
            error.message,
          );
        } finally {
          this.busy.delete(job.id);
        }
      }),
    );
  }
  async execute(job) {
    const cluster = await this.cluster(job.cluster_id);
    const input = JSON.parse(job.input);
    if (Date.now() > input.deadline) {
      // A newly queued intent has never sent a mutation. An explicit retry
      // may refer to an earlier committed effect and remains uncertain.
      await this.update(
        job,
        job.step === "retry-requested" ? "uncertain" : "failed",
        "expired",
        "Request expired before submission",
      );
      return;
    }
    const leader = await this.meta.leader(cluster);
    await this.update(job, "running", "submitting");
    try {
      if (job.kind === "failover") {
        const result = await this.meta.exec([
          "failover",
          input.group,
          ...this.meta.options(cluster, leader, true),
          "--operation-id",
          job.id,
          "--deadline-unix-ms",
          String(input.deadline),
        ]);
        if (result.code !== 0) {
          await this.update(
            job,
            result.code === 2 ? "failed" : "uncertain",
            "submitting",
            result.stderr || result.stdout,
          );
          return;
        }
      } else if (job.kind === "create") {
        const directory = await mkdtemp(join(tmpdir(), "lavik-admin-"));
        try {
          const manifest = join(directory, "cluster.toml");
          await writeFile(manifest, input.manifest, { mode: 0o600 });
          const result = await this.meta.exec([
            "cluster-create",
            "--manifest",
            manifest,
            ...this.meta.options(cluster, leader, true),
            "--yes",
          ]);
          const operation = /operation=([0-9a-f]{32})/.exec(result.stdout)?.[1];
          if (operation)
            await this.store.query(
              "UPDATE jobs SET meta_operation_id=? WHERE id=?",
              [operation, job.id],
              "run",
            );
          if (result.code !== 0) {
            await this.update(
              job,
              result.code === 2 ? "failed" : "uncertain",
              "submitting",
              result.stderr || result.stdout,
            );
            return;
          }
        } finally {
          await rm(directory, { recursive: true });
        }
      } else {
        await this.meta.command(
          cluster,
          [
            "submitop",
            job.id,
            `lavik-admin-${job.kind}-v1`,
            Buffer.from(job.input).toString("hex"),
          ],
          leader,
        );
        const operation = await this.meta.command(
          cluster,
          ["getop", job.id],
          leader,
        );
        if (
          operation.startsWith("OK aborted") ||
          operation.startsWith("OK completed")
        ) {
          await this.update(
            job,
            operation.startsWith("OK aborted") ? "failed" : "completed",
            "terminal",
            operation.slice(3),
          );
          return;
        }
        if (job.kind === "replica-add") {
          await this.update(job, "running", "registering");
          await this.meta.command(
            cluster,
            [
              "registernode",
              input.node,
              `lavik://node/${input.node}`,
              "replica",
              input.endpoint,
            ],
            leader,
          );
          await this.update(job, "running", "assigning");
          await this.meta.command(
            cluster,
            ["assignnode", input.group, input.node, "replica"],
            leader,
          );
        } else {
          const group = fields(
            await this.meta.command(cluster, ["getgroup", input.group], leader),
          );
          if (group.owner === input.node || group.transition !== "0")
            throw new AdminError(
              "Primary or active failover cannot be removed",
              409,
            );
          if (
            input.expectedRevision &&
            input.expectedRevision !== group.revision
          )
            throw new AdminError(
              "Membership changed since this removal was reviewed; abandon it and review a new request",
              409,
            );
          input.expectedRevision ||= group.revision;
          await this.store.query(
            "UPDATE jobs SET input=? WHERE id=?",
            [JSON.stringify(input), job.id],
            "run",
          );
          await this.update(job, "running", "removing");
          await this.meta.command(
            cluster,
            ["unassignnode", input.group, input.node, group.revision],
            leader,
          );
        }
      }
      await this.update(
        job,
        "running",
        "waiting",
        "Waiting for Meta and Data to converge",
      );
    } catch (error) {
      await this.update(job, "uncertain", "checking", error.message);
    }
  }
  async observe(job) {
    const cluster = await this.cluster(job.cluster_id);
    const status = await this.meta.status(cluster);
    const input = JSON.parse(job.input);
    if (job.kind.startsWith("replica-")) {
      try {
        const operation = await this.meta.command(cluster, ["getop", job.id]);
        if (operation.startsWith("OK aborted")) {
          await this.update(job, "failed", "aborted", operation.slice(3));
          return;
        }
      } catch {
        /* An uncertain submission may not yet have a visible record. */
      }
    }
    if (job.kind === "create" || job.kind === "failover") {
      const opId = job.meta_operation_id || status.root_operation_id;
      if (opId) {
        const reply = await this.meta.command(cluster, ["getop", opId]);
        if (reply.startsWith("OK completed")) {
          if (job.kind !== "create" || status.cluster_ready)
            await this.update(job, "completed", "completed", reply.slice(3));
          return;
        }
        if (reply.startsWith("OK aborted")) {
          await this.update(job, "failed", "aborted", reply.slice(3));
          return;
        }
        await this.update(job, "running", "waiting", reply.slice(3));
      }
    } else {
      const node = status.data_nodes.find((n) => n.node_id === input.node);
      const satisfied =
        job.kind === "replica-add"
          ? node?.group_id === input.group &&
            node.population_current &&
            node.projection_current &&
            node.health_fresh
          : // Unassigned identities have no group runtime row, so Meta reports
            // projection_current=false for them. Completion uses committed
            // removal and convergence of the remaining group, not a nonexistent
            // detached-node population acknowledgement.
            node &&
            !node.group_id &&
            status.groups.find((g) => g.group_id === input.group)
              ?.topology_converged;
      if (satisfied) {
        const reply = await this.meta.command(cluster, ["getop", job.id]);
        if (reply.startsWith("OK aborted")) {
          await this.update(job, "failed", "aborted", reply);
          return;
        }
        if (!reply.startsWith("OK completed"))
          await this.meta.command(cluster, [
            "completeop",
            job.id,
            "topology-converged",
          ]);
        await this.update(
          job,
          "completed",
          "completed",
          job.kind === "replica-add"
            ? "Membership and Data projection converged"
            : "Replica removed; remaining group members converged",
        );
        return;
      }
    }
    if (Date.now() > input.deadline)
      await this.update(
        job,
        "uncertain",
        "checking",
        "Deadline elapsed. Inspect topology and the Meta operation before taking further action.",
      );
  }
  async resume(id, jobId) {
    await this.cluster(id);
    const job = await this.store.query(
      "SELECT * FROM jobs WHERE id=? AND cluster_id=?",
      [jobId, id],
      "get",
    );
    if (!job || job.state !== "uncertain" || this.busy.has(jobId))
      throw new AdminError(
        "Only an idle uncertain operation can be resumed",
        409,
      );
    if (job.kind === "create")
      throw new AdminError(
        "Creation recovery is owned by Meta; inspect the root operation and cluster lifecycle",
        409,
      );
    const input = JSON.parse(job.input);
    if (Date.now() > input.deadline)
      throw new AdminError(
        "The retained deadline has expired. Inspect Meta and abort the old operation before submitting a new request.",
        409,
      );
    if (job.kind === "replica-remove" && !input.expectedRevision)
      throw new AdminError(
        "This request has no retained membership revision; abandon it and review a new removal",
        409,
      );
    // A remove retry retains its original membership CAS. Failover retains
    // both operation id and absolute deadline. Register/assign are verified
    // idempotent by Meta; no retry invents a new destructive intent.
    this.busy.add(jobId);
    try {
      await this.update(
        job,
        "queued",
        "retry-requested",
        "Operator requested retry with the original identity",
      );
      await this.audit("operation-resumed", id, jobId);
    } finally {
      this.busy.delete(jobId);
    }
    void this.tick().catch(() => {});
    return { resumed: jobId };
  }
  async abandon(id, jobId) {
    const cluster = await this.cluster(id);
    const job = await this.store.query(
      "SELECT * FROM jobs WHERE id=? AND cluster_id=?",
      [jobId, id],
      "get",
    );
    if (
      !job ||
      job.state !== "uncertain" ||
      !job.kind.startsWith("replica-") ||
      this.busy.has(jobId)
    )
      throw new AdminError(
        "Only an idle uncertain replica request can be abandoned",
        409,
      );
    this.busy.add(jobId);
    try {
      const result = await this.meta.command(cluster, ["getop", jobId]);
      if (result.startsWith("OK completed")) {
        await this.observe(job);
        return { completed: jobId };
      }
      if (!result.startsWith("OK aborted"))
        await this.meta.command(cluster, [
          "abortop",
          jobId,
          "operator-abandoned",
        ]);
      await this.update(
        job,
        "failed",
        "abandoned",
        "Operator abandoned this request. Committed membership changes remain.",
      );
      await this.audit("operation-abandoned", id, jobId);
      return { abandoned: jobId };
    } finally {
      this.busy.delete(jobId);
    }
  }
  async keys(id, query) {
    const view = await this.view(id);
    const cluster = await this.cluster(id);
    const owners = view.status.groups
      .map((g) => g.owner_node_id)
      .filter(Boolean)
      .sort();
    let position = 0,
      cursor = "0",
      epoch = view.status.capture.topology_epoch;
    if (query.cursor) {
      try {
        ({ position, cursor, epoch } = JSON.parse(
          Buffer.from(query.cursor, "base64url").toString(),
        ));
      } catch {
        throw new AdminError("Invalid scan cursor");
      }
    }
    if (
      !Number.isInteger(position) ||
      position < 0 ||
      position >= owners.length ||
      !/^\d+$/.test(cursor) ||
      epoch !== view.status.capture.topology_epoch
    )
      throw new AdminError("Scan topology changed; restart the scan");
    const pattern = query.pattern || "*";
    if (pattern.length > 1024)
      throw new AdminError("Search pattern is too long");
    const keys = [];
    // Lavik bounds SCAN by examined partitions, so sparse databases produce
    // empty pages. Coalesce a bounded 16 pages without blocking the event loop
    // or asking a DBA to click through every empty partition range.
    for (
      let page = 0;
      page < 16 && position < owners.length && !keys.length;
      page++
    ) {
      const result = await this.nodeRequest(
        cluster,
        view.nodes,
        owners[position],
        ["SCAN", cursor, "MATCH", pattern, "COUNT", "256"],
      );
      cursor = text(result[0]);
      keys.push(
        ...result[1].map((key) => ({
          id: key.toString("base64"),
          name: jsonReply(key),
          node: owners[position],
        })),
      );
      if (cursor === "0") position++;
    }
    return {
      keys,
      cursor:
        position < owners.length
          ? Buffer.from(JSON.stringify({ position, cursor, epoch })).toString(
              "base64url",
            )
          : null,
    };
  }
  async key(id, encoded) {
    const bytes = keyBytes(encoded);
    const view = await this.view(id);
    const cluster = await this.cluster(id);
    const keySlot = slot(bytes);
    const range = view.status.slot_ranges.find(
      (r) => +r.first <= keySlot && +r.last >= keySlot,
    );
    const owner = view.status.groups.find(
      (g) => g.group_id === range?.group_id,
    )?.owner_node_id;
    if (!owner) throw new AdminError("No primary owns this key slot", 503);
    const send = (args) => this.nodeRequest(cluster, view.nodes, owner, args);
    const type = text(await send(["TYPE", bytes]));
    const ttl = text(await send(["PTTL", bytes]));
    const reads = {
      string: ["GET", bytes],
      hash: ["HSCAN", bytes, "0", "COUNT", "100"],
      list: ["LRANGE", bytes, "0", "99"],
      set: ["SSCAN", bytes, "0", "COUNT", "100"],
      zset: ["ZRANGE", bytes, "0", "99", "WITHSCORES"],
      stream: ["XRANGE", bytes, "-", "+", "COUNT", "100"],
    };
    const value = reads[type] ? jsonReply(await send(reads[type])) : null;
    return {
      id: encoded,
      type,
      ttl,
      value,
      slot: keySlot,
      node: owner,
      bounded: type !== "string",
    };
  }
  async send(id, input) {
    const args = input.args;
    if (
      !Array.isArray(args) ||
      !args.length ||
      args.length > 1000 ||
      args.some((a) => typeof a !== "string")
    )
      throw new AdminError("Supply command arguments as strings");
    const name = args[0].toUpperCase();
    if (!READ_COMMANDS.has(name) && !WRITE_COMMANDS.has(name))
      throw new AdminError(
        "This command is not available in the data console; use cluster controls for administration",
      );
    if (WRITE_COMMANDS.has(name) && input.confirm !== true)
      throw new AdminError("Confirm the data write before executing it", 409);
    if (
      name === "CLUSTER" &&
      !["INFO", "NODES", "SLOTS", "KEYSLOT", "MYID"].includes(
        args[1]?.toUpperCase(),
      )
    )
      throw new AdminError("Use the Topology controls for cluster changes");
    if (name === "SLOWLOG" && args[1]?.toUpperCase() !== "GET")
      throw new AdminError("Only SLOWLOG GET is available here");
    const view = await this.view(id);
    const cluster = await this.cluster(id);
    const node = input.node || view.status.groups[0]?.owner_node_id;
    if (Buffer.byteLength(JSON.stringify(args)) > 1024 * 1024)
      throw new AdminError("Command exceeds 1 MiB");
    await this.audit("data-command", id, `${node} ${name}`);
    return {
      reply: jsonReply(await this.nodeRequest(cluster, view.nodes, node, args)),
    };
  }
  async activity(id) {
    const view = await this.view(id);
    const cluster = await this.cluster(id);
    const entries = [];
    for (const node of view.nodes.filter((n) => n.group_id)) {
      try {
        const rows = await this.nodeRequest(cluster, view.nodes, node.node_id, [
          "SLOWLOG",
          "GET",
          "50",
        ]);
        for (const row of rows)
          entries.push({
            node: node.node_id,
            id: text(row[0]),
            time: text(row[1]),
            duration: text(row[2]),
            args: jsonReply(row[3]),
          });
      } catch (error) {
        entries.push({ node: node.node_id, error: error.message });
      }
    }
    return {
      entries: entries.sort(
        (a, b) => Number(b.time || 0) - Number(a.time || 0),
      ),
    };
  }
}
