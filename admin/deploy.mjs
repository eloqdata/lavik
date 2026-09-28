// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { randomBytes, createHash } from "node:crypto";
import { AdminError, identifier, endpoint } from "./meta.mjs";
import { nodeLayout } from "./public/placement.js";
import { SSH, sshHost } from "./ssh.mjs";
import { Releases } from "./releases.mjs";
import { encode, decode, RedisError } from "./resp.mjs";

const stamp = () => new Date().toISOString();
const hash = (value) =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");
const address = (ip, port) => `${ip.includes(":") ? `[${ip}]` : ip}:${port}`;
const integer = (value, fallback, min, max, label) => {
  const result = Number(value ?? fallback);
  if (!Number.isInteger(result) || result < min || result > max)
    throw new AdminError(`${label} must be between ${min} and ${max}`);
  return result;
};

/** Normalize only supported tuning, before any SSH or filesystem side effect. */
export function settings(input) {
  const id = identifier(input.id, "cluster name");
  if (id.length > 40)
    throw new AdminError("Cluster names are limited to 40 characters");
  if (
    !Array.isArray(input.hosts) ||
    !input.hosts.length ||
    input.hosts.length > 64
  )
    throw new AdminError("Add 1–64 SSH hosts");
  const hosts = input.hosts.map(sshHost);
  if (new Set(hosts.map((h) => `${h.host}:${h.port}`)).size !== hosts.length)
    throw new AdminError("List each SSH host only once");
  const baseDir = input.baseDir || ".local/share/lavik/clusters";
  if (
    typeof baseDir !== "string" ||
    !/^[a-zA-Z0-9_./-]{1,200}$/.test(baseDir) ||
    baseDir.split("/").includes("..")
  )
    throw new AdminError(
      "Storage directory must be a simple path without spaces or parent traversal",
    );
  const supervisor = input.supervisor || "systemd";
  if (!["systemd", "process"].includes(supervisor))
    throw new AdminError("Choose systemd or process supervision");
  const clientMode = input.clientMode || "cluster";
  if (!["single", "cluster"].includes(clientMode))
    throw new AdminError("Choose single or cluster client mode");
  const groups = integer(input.groups, 1, 1, 16, "Primary groups");
  const followers = integer(input.followers, 2, 0, 8, "Followers per primary");
  if (groups * (followers + 1) > 64)
    throw new AdminError("A setup can contain at most 64 Data nodes");
  if (clientMode === "single" && groups !== 1)
    throw new AdminError("Single client mode supports one primary group");
  const metaCount = integer(input.metaCount, 3, 1, 5, "Meta voters");
  if (![1, 3, 5].includes(metaCount))
    throw new AdminError("Choose 1, 3, or 5 Meta voters");
  const name = input.name?.trim() || id;
  if (name.length > 100) throw new AdminError("Display name is too long");
  return {
    id,
    name,
    hosts,
    baseDir,
    supervisor,
    clientMode,
    groups,
    followers,
    metaCount,
    release: input.release || "nightly",
    placement: input.placement,
    threads: integer(input.threads, 2, 1, 128, "Workers"),
    dataGiB: integer(input.dataGiB, 4, 1, 1048576, "Storage GiB per node"),
    dataPort: integer(input.dataPort, 6379, 1024, 65400, "Data base port"),
    metaPort: integer(input.metaPort, 7100, 1024, 65400, "Raft base port"),
    ctlPort: integer(input.ctlPort, 7200, 1024, 65400, "Admin base port"),
    controlPort: integer(
      input.controlPort,
      7300,
      1024,
      65400,
      "Control base port",
    ),
  };
}

/** Generate stable identities for the chosen placement and reject overlapping host ports. */
export function topology(config) {
  let layout;
  try {
    layout = nodeLayout(config);
  } catch (error) {
    throw new AdminError(error.message);
  }
  const dataCounts = new Map();
  const nodes = layout.map(({ index, ...node }) => {
    // Distinct hosts can share the normal Data port. Count by advertised IP,
    // not SSH endpoint: separate SSH routes may reach the same listening IP.
    let port = config.metaPort + index;
    if (node.kind === "data") {
      const ip = config.hosts[node.host].address;
      const localIndex = dataCounts.get(ip) || 0;
      port = config.dataPort + localIndex;
      dataCounts.set(ip, localIndex + 1);
    }
    return {
      ...node,
      id: node.kind === "meta" ? index + 1 : randomBytes(20).toString("hex"),
      port,
      ...(node.kind === "meta"
        ? { ctl: config.ctlPort + index, control: config.controlPort + index }
        : {}),
    };
  });
  const ports = new Set();
  for (const node of nodes)
    for (const port of [node.port, node.ctl, node.control].filter(Boolean)) {
      const key = address(config.hosts[node.host].address, port);
      if (ports.has(key))
        throw new AdminError(`Port assignments overlap: ${key}`);
      ports.add(key);
    }
  return nodes;
}

/** Keep Genesis membership immutable when later follower installations are added. */
export function manifest(plan) {
  const lines = [
    "schema_version = 1",
    ...(plan.modern ? [`client_mode = "${plan.clientMode}"`] : []),
    'slot_strategy = "contiguous-even"',
  ];
  for (const node of plan.nodes.filter((n) => n.kind === "meta")) {
    const ip = plan.hosts[node.host].address;
    lines.push(
      "",
      "[[meta_members]]",
      `id = ${node.id}`,
      `raft_endpoint = "tcp://${address(ip, node.port)}"`,
      `ctl_endpoint = "tcp://${address(ip, node.ctl)}"`,
      `data_control_endpoint = "tcp://${address(ip, node.control)}"`,
    );
  }
  for (const node of plan.nodes.filter((n) => n.kind === "data" && !n.added))
    lines.push(
      "",
      "[[data_nodes]]",
      `id = "${node.id}"`,
      `client_endpoint = "tcp://${address(
        plan.hosts[node.host].address,
        node.port,
      )}"`,
    );
  for (let i = 1; i <= plan.groups; i++) {
    const group = plan.nodes.filter(
      (n) => n.group === `group-${i}` && !n.added,
    );
    lines.push(
      "",
      "[[groups]]",
      `id = "group-${i}"`,
      `primary = "${group.find((n) => n.role === "primary").id}"`,
      `replicas = ${JSON.stringify(
        group.filter((n) => n.role === "replica").map((n) => n.id),
      )}`,
    );
  }
  return lines.join("\n") + "\n";
}

/** Select the flags accepted by the pinned binary; paths resolve only on its host. */
export function nodeArguments(plan, node) {
  const ip = plan.hosts[node.host].address;
  const directory = `@ROOT@/${node.name}`;
  if (node.kind === "meta")
    return [
      "--id",
      String(node.id),
      "--addr",
      address(ip, node.port),
      "--ctl-addr",
      address(ip, node.ctl),
      "--data-control-addr",
      address(ip, node.control),
      "--data-dir",
      `${directory}/state`,
    ];
  const prefix = plan.modern ? "--" : "--cluster-";
  return [
    "--bind",
    ip,
    "--port",
    String(node.port),
    "--threads",
    String(plan.threads),
    "--no-pin-workers",
    "--data-file",
    `${directory}/lavik.data`,
    "--log-dir",
    `${directory}/logs`,
    "--alsologtostderr",
    ...(plan.modern ? [] : ["--cluster-enabled"]),
    `${prefix}node-id`,
    node.id,
    `${prefix}announce-ip`,
    ip,
    ...plan.nodes
      .filter((n) => n.kind === "meta")
      .flatMap((n) => [
        `${prefix}meta-seed`,
        address(plan.hosts[n.host].address, n.control),
      ]),
  ];
}

/** Provisioning and Meta work use the same job admission and durable catalog. */
export class Deployments {
  constructor(
    fleet,
    { ssh = new SSH(), releases = new Releases(), hosts } = {},
  ) {
    this.fleet = fleet;
    this.store = fleet.store;
    this.ssh = ssh;
    this.releases = releases;
    this.hosts = hosts;
    this.previews = new Map();
  }
  async plan(id) {
    const row = await this.store.query(
      "SELECT plan FROM deployments WHERE cluster_id=?",
      [id],
      "get",
    );
    return row ? JSON.parse(row.plan) : null;
  }
  async save(plan) {
    await this.store.query(
      "UPDATE deployments SET plan=? WHERE cluster_id=?",
      [JSON.stringify(plan), plan.id],
      "run",
    );
  }
  request(plan, action, extra = {}) {
    return {
      action,
      cluster: plan.id,
      owner: plan.owner,
      baseDir: plan.baseDir,
      ...extra,
    };
  }
  async install(plan, index) {
    const host = plan.hosts[index];
    const asset = plan.release.assets[plan.checks[index].arch];
    const file = await this.releases.download(asset);
    const { directory } = await this.ssh.call(
      host,
      this.request(plan, "reserve"),
    );
    await this.ssh.upload(host, file, `${directory}/.incoming-${asset.sha256}`);
    return this.ssh.call(
      host,
      this.request(plan, "prepare", { asset }),
      120000,
    );
  }
  retain(token, preview) {
    for (const [key, value] of this.previews)
      if (value.expires < Date.now()) this.previews.delete(key);
    if (this.previews.size >= 100)
      throw new AdminError("Too many setup previews; retry shortly", 503);
    this.previews.set(token, { ...preview, expires: Date.now() + 15 * 60000 });
  }
  /** Check prerequisites without changing hosts; retain the exact reviewed plan. */
  async preview(input) {
    if (Array.isArray(input.hosts) && input.hosts.length > 64)
      throw new AdminError("Add 1–64 SSH hosts");
    if (this.hosts && Array.isArray(input.hosts))
      input = {
        ...input,
        hosts: await Promise.all(
          input.hosts.map((host) => this.hosts.resolve(host)),
        ),
      };
    const config = settings(input);
    const release = await this.releases.resolve(config.release);
    const nodes = topology(config);
    const checks = [];
    for (let i = 0; i < config.hosts.length; i++) {
      const owned = nodes.filter((n) => n.host === i);
      try {
        const result = await this.ssh.call(
          config.hosts[i],
          this.request(config, "probe", {
            address: config.hosts[i].address,
            supervisor: config.supervisor,
            ports: owned.flatMap((n) =>
              [n.port, n.ctl, n.control].filter(Boolean),
            ),
            bytes:
              owned.filter((n) => n.kind === "data").length *
              config.dataGiB *
              1024 ** 3,
          }),
        );
        if (!release.assets[result.arch]) {
          result.ok = false;
          result.errors.push(
            "This release has no package for this host architecture",
          );
        }
        checks.push({ host: i, ...result });
      } catch (error) {
        checks.push({ host: i, ok: false, errors: [error.message] });
      }
    }
    const plan = {
      ...config,
      owner: randomBytes(16).toString("hex"),
      release,
      nodes,
      checks,
    };
    const token = randomBytes(24).toString("hex");
    // The submitted token refers to server-retained, checked inputs. A browser
    // cannot replace the release digest or host settings after review.
    this.retain(token, { plan });
    return {
      token,
      ...plan,
      ready: checks.every((c) => c.ok),
      warnings: [
        ...(new Set(
          nodes
            .filter((n) => n.kind === "meta")
            .map((n) => config.hosts[n.host].address),
        ).size < config.metaCount
          ? [
              "Meta voters share hosts; this does not tolerate a whole-host failure.",
            ]
          : []),
        ...(Array.from({ length: config.groups }, (_, i) =>
          nodes.filter((n) => n.group === `group-${i + 1}`),
        ).some(
          (group) =>
            new Set(group.map((n) => config.hosts[n.host].address)).size <
            group.length,
        )
          ? [
              "Some primary/follower nodes share a host; a host failure can remove multiple group members.",
            ]
          : []),
        ...(config.supervisor === "process"
          ? [
              "Process mode is for development; processes are not restarted on crash or host reboot.",
            ]
          : []),
        "Cluster traffic uses the hosts' trusted private network; SSH protects Admin connections.",
      ],
    };
  }
  /** Atomically admit a reviewed deployment, idempotently under its ownership ID. */
  async create(input) {
    const preview = this.previews.get(input.token);
    if (!preview || preview.expires < Date.now())
      throw new AdminError(
        "Run host checks again; the reviewed plan expired",
        409,
      );
    const plan = preview.plan;
    if (preview.follower)
      throw new AdminError("This review is for a follower addition");
    if (input.confirm !== plan.id)
      throw new AdminError("Confirm the cluster name to deploy");
    if (!plan.checks.every((c) => c.ok))
      throw new AdminError("Resolve host prerequisites before deploying", 409);
    const existing = await this.store.query(
      "SELECT * FROM jobs WHERE id=?",
      [plan.owner],
      "get",
    );
    if (existing) return existing;
    const seeds = plan.nodes
      .filter((n) => n.kind === "meta")
      .map((n) => address(plan.hosts[n.host].address, n.ctl));
    const time = stamp();
    try {
      await this.store.batch([
        {
          sql: "INSERT INTO clusters VALUES(?,?,?,?,?)",
          params: [plan.id, plan.name, JSON.stringify(seeds), "default", time],
        },
        {
          sql: "INSERT INTO deployments VALUES(?,?)",
          params: [plan.id, JSON.stringify(plan)],
        },
        {
          sql: "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
          params: [
            plan.owner,
            plan.id,
            "deploy",
            "{}",
            "queued",
            "prepare",
            "Preparing hosts",
            null,
            time,
            time,
          ],
        },
      ]);
    } catch (error) {
      if (error.message.includes("UNIQUE"))
        throw new AdminError("Cluster name already exists", 409);
      throw error;
    }
    await this.fleet.audit("deployment-created", plan.id, plan.owner);
    void this.fleet.tick().catch(() => {});
    return this.store.query(
      "SELECT * FROM jobs WHERE id=?",
      [plan.owner],
      "get",
    );
  }
  /** Review a fresh node for an existing group using that cluster's original release. */
  async previewFollower(id, input) {
    const prepared = !!(this.hosts && input.host?.hostId);
    if (this.hosts)
      input = { ...input, host: await this.hosts.resolve(input.host) };
    const current = await this.plan(id);
    if (!current)
      throw new AdminError(
        "Connect an already-running follower for a cluster not deployed by Admin",
      );
    if (!current.modern)
      throw new AdminError(
        "This older release lacks the membership APIs required for safe resizing; use a current release",
      );
    const view = await this.fleet.view(id, true);
    const group = identifier(input.group, "group");
    if (!view.status.groups.some((g) => g.group_id === group))
      throw new AdminError("Group not found");
    const host = sshHost(input.host);
    const plan = structuredClone(current);
    let index = plan.hosts.findIndex(
      (h) => h.host === host.host && h.port === host.port,
    );
    if (index < 0) {
      index = plan.hosts.length;
      plan.hosts.push(host);
    } else if (JSON.stringify(plan.hosts[index]) !== JSON.stringify(host)) {
      const saved = plan.hosts[index];
      if (
        !prepared ||
        saved.user !== host.user ||
        saved.address !== host.address
      )
        throw new AdminError(
          "Use this cluster's saved SSH user and cluster IP when adding a node on an existing host",
        );
      // A verified inventory identity may replace legacy login paths for the
      // same account. The reviewed plan is committed atomically with the job.
      plan.hosts[index] = host;
    }
    const port = integer(input.dataPort, 6379, 1024, 65535, "Data port");
    if (
      plan.nodes.some(
        (n) =>
          plan.hosts[n.host].address === host.address &&
          [n.port, n.ctl, n.control].includes(port),
      )
    )
      throw new AdminError(
        "This port is reserved by another node in the cluster",
      );
    const node = {
      name: `data-${randomBytes(6).toString("hex")}`,
      id: randomBytes(20).toString("hex"),
      kind: "data",
      host: index,
      port,
      group,
      role: "replica",
      added: true,
    };
    let check;
    try {
      check = await this.ssh.call(
        host,
        this.request(plan, "probe", {
          address: host.address,
          supervisor: plan.supervisor,
          allowOwned: true,
          ports: [port],
          bytes: plan.dataGiB * 1024 ** 3,
        }),
      );
      if (!plan.release.assets[check.arch]) {
        check.ok = false;
        check.errors.push(
          "The pinned release has no package for this architecture",
        );
      }
    } catch (error) {
      check = { ok: false, errors: [error.message] };
    }
    plan.nodes.push(node);
    plan.checks[index] = { ...check, host: index };
    const token = randomBytes(24).toString("hex");
    this.retain(token, {
      plan,
      node,
      follower: true,
      baseHash: hash(current),
    });
    return {
      ...plan,
      token,
      nodes: [node],
      checks: [{ ...check, host: index }],
      ready: check.ok,
      warnings: [
        "The follower uses this cluster's pinned release and storage settings.",
      ],
    };
  }
  /** Admit a follower only while its reviewed installation plan remains current. */
  async createFollower(id, input) {
    const preview = this.previews.get(input.token);
    if (
      !preview?.follower ||
      preview.expires < Date.now() ||
      preview.plan.id !== id
    )
      throw new AdminError("Run follower host checks again", 409);
    if (input.confirm !== id || !preview.plan.checks[preview.node.host].ok)
      throw new AdminError(
        "Resolve prerequisites and confirm the cluster name",
      );
    const jobId = (preview.jobId ||= randomBytes(16).toString("hex"));
    const existing = await this.store.query(
      "SELECT * FROM jobs WHERE id=?",
      [jobId],
      "get",
    );
    if (existing) return existing;
    const current = await this.plan(id);
    if (hash(current) !== preview.baseHash)
      throw new AdminError(
        "The deployment changed; review the follower again",
        409,
      );
    const node = preview.node;
    const payload = {
      node: node.id,
      group: node.group,
      endpoint: `tcp://${address(
        preview.plan.hosts[node.host].address,
        node.port,
      )}`,
    };
    const time = stamp();
    try {
      await this.store.batch([
        {
          sql: "UPDATE deployments SET plan=? WHERE cluster_id=? AND plan=?",
          params: [JSON.stringify(preview.plan), id, JSON.stringify(current)],
          expectedChanges: 1,
        },
        {
          sql: "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
          params: [
            jobId,
            id,
            "deploy-follower",
            JSON.stringify(payload),
            "queued",
            "prepare",
            "Preparing a new follower",
            jobId,
            time,
            time,
          ],
        },
      ]);
    } catch (error) {
      if (error.message.includes("UNIQUE"))
        throw new AdminError(
          "Finish or resolve the active cluster operation first",
          409,
        );
      throw error;
    }
    await this.fleet.audit("follower-deployment", id, jobId);
    void this.fleet.tick().catch(() => {});
    return this.store.query("SELECT * FROM jobs WHERE id=?", [jobId], "get");
  }
  async runFollower(job) {
    const plan = await this.plan(job.cluster_id);
    const input = JSON.parse(job.input);
    const node = plan.nodes.find((n) => n.id === input.node);
    await this.fleet.update(
      job,
      "running",
      "prepare",
      "Installing the pinned release for the new follower",
    );
    try {
      const version = await this.install(plan, node.host);
      if (version.revision !== plan.revision)
        throw new AdminError(
          "Follower binary revision does not match its cluster",
        );
      await this.ssh.call(
        plan.hosts[node.host],
        this.request(plan, "start", {
          node: { ...node, args: nodeArguments(plan, node) },
          manifest: manifest(plan),
          dataGiB: plan.dataGiB,
          supervisor: plan.supervisor,
        }),
        60000,
      );
      input.deadline = Date.now() + 300000;
      await this.store.query(
        "UPDATE jobs SET kind='replica-add',input=?,state='queued',step='preflight' WHERE id=?",
        [JSON.stringify(input), job.id],
        "run",
      );
      await this.fleet.execute(
        await this.store.query(
          "SELECT * FROM jobs WHERE id=?",
          [job.id],
          "get",
        ),
      );
    } catch (error) {
      await this.fleet.update(job, "uncertain", "prepare", error.message);
    }
  }
  /** Invoke the release-matched client; read must be set by a known read command. */
  async ctl(cluster, args, read = false) {
    const plan = await this.plan(cluster.id);
    if (!plan) return null;
    let error;
    // A transport failure may be a submitted mutation. Only read commands can
    // try another SSH host; writes retain the original host and job identity.
    const indexes = [
      ...new Set(
        plan.nodes.filter((n) => n.kind === "meta").map((n) => n.host),
      ),
    ];
    for (const i of read ? indexes : indexes.slice(0, 1)) {
      try {
        return await this.ssh.call(
          plan.hosts[i],
          this.request(plan, "ctl", { args }),
          16000,
        );
      } catch (failure) {
        error = failure;
      }
    }
    throw error;
  }
  /** Reach a Data endpoint validated against Meta topology over SSH without exposing private IPs locally. */
  async data(cluster, target, args) {
    const plan = await this.plan(cluster.id);
    if (!plan) return null;
    const node = plan.nodes.find(
      (n) =>
        n.kind === "data" &&
        `tcp://${address(plan.hosts[n.host].address, n.port)}` === target,
    );
    // Fleet admits only endpoints learned from this cluster's Meta topology.
    // Already-running additions have no local installation record; reach those
    // through the first Meta host on the same private cluster network.
    if (!target.startsWith("tcp://"))
      throw new AdminError(
        "SSH-deployed clusters require plaintext Data endpoints inside their private network",
      );
    const gateway =
      node?.host ?? plan.nodes.find((n) => n.kind === "meta").host;
    const remote = endpoint(target.replace(/^tcp:\/\//, ""));
    const result = await this.ssh.call(
      plan.hosts[gateway],
      this.request(plan, "resp", {
        ...remote,
        payload: encode(args).toString("base64"),
      }),
      10000,
    );
    const reply = decode(Buffer.from(result.reply, "base64"));
    if (!reply) throw new AdminError("Incomplete remote Data reply", 502);
    if (reply.value instanceof RedisError) throw reply.value;
    return { value: reply.value };
  }
  async run(job) {
    const plan = await this.plan(job.cluster_id);
    const update = (step, detail) =>
      this.fleet.update(job, "running", step, detail);
    try {
      if (job.step !== "creating" && job.step !== "waiting") {
        await update(
          "prepare",
          "Installing the checksummed release on each host",
        );
        const versions = [];
        for (let i = 0; i < plan.hosts.length; i++) {
          const result = await this.install(plan, i);
          versions.push(result);
        }
        if (
          new Set(versions.map((v) => `${v.revision}/${v.modern}`)).size !== 1
        )
          throw new AdminError(
            "Hosts resolved different release builds; deployment stopped",
          );
        plan.modern = versions[0].modern;
        plan.version = versions[0].version;
        plan.revision = versions[0].revision;
        if (!plan.modern && plan.clientMode === "single")
          throw new AdminError(
            "This older release supports Cluster client mode only; choose a newer release for Single mode",
          );
        await this.save(plan);
        let checkedNetwork = false;
        for (const node of plan.nodes) {
          if (node.kind === "data" && !checkedNetwork) {
            const endpoints = plan.nodes
              .filter((n) => n.kind === "meta")
              .flatMap((n) =>
                [n.port, n.ctl, n.control].map((port) => ({
                  host: plan.hosts[n.host].address,
                  port,
                })),
              );
            await update(
              "starting",
              "Checking Meta connectivity from every host",
            );
            for (const host of plan.hosts)
              await this.ssh.call(
                host,
                this.request(plan, "connectivity", { endpoints }),
                45000,
              );
            checkedNetwork = true;
          }
          await update(
            "starting",
            `Starting ${node.name} on ${plan.hosts[node.host].host}`,
          );
          await this.ssh.call(
            plan.hosts[node.host],
            this.request(plan, "start", {
              node: { ...node, args: nodeArguments(plan, node) },
              manifest: manifest(plan),
              dataGiB: plan.dataGiB,
              supervisor: plan.supervisor,
            }),
            60000,
          );
        }
      }
      const cluster = await this.fleet.cluster(plan.id);
      // Never infer permission to replay Genesis from a transport timeout. The
      // durable 'creating' checkpoint survives an Admin crash before its reply.
      let status = await this.fleet.meta.status(cluster);
      if (status.cluster_state === "uninitialized") {
        if (["creating", "waiting"].includes(job.step))
          throw new AdminError(
            "Creation outcome is uncertain. Inspect Meta before submitting any new creation request.",
          );
        await update("creating", "Initializing the cluster through lavik-ctl");
        const result = await this.ctl(cluster, [
          "cluster-create",
          "--manifest",
          "cluster.toml",
          ...this.fleet.meta.options(
            cluster,
            JSON.parse(cluster.seeds)[0],
            true,
          ),
          "--yes",
        ]);
        if (result.code !== 0)
          throw new AdminError(
            result.stderr ||
              result.stdout ||
              "Cluster creation outcome is uncertain",
          );
      } else if (!["creating", "created"].includes(status.cluster_state))
        throw new AdminError(
          `Cluster lifecycle requires inspection: ${status.cluster_state}`,
        );
      await update(
        "waiting",
        "Waiting for primary and followers to become ready",
      );
    } catch (error) {
      const latest = await this.store.query(
        "SELECT step FROM jobs WHERE id=?",
        [job.id],
        "get",
      );
      await this.fleet.update(job, "uncertain", latest.step, error.message);
    }
  }
  async observe(job) {
    // An interrupted install/start is never replayed by the periodic observer.
    // Explicit resume reuses ownership, immutable files, digests, and node IDs.
    if (!["creating", "waiting"].includes(job.step)) return;
    try {
      const cluster = await this.fleet.cluster(job.cluster_id);
      const status = await this.fleet.meta.status(cluster);
      if (status.cluster_state === "created" && status.cluster_ready)
        await this.fleet.update(
          job,
          "completed",
          "completed",
          "Cluster ready. Open the dashboard to manage and observe it.",
        );
      else if (status.cluster_state === "provisioning-failed")
        await this.fleet.update(
          job,
          "failed",
          "failed",
          status.provisioning_failure_summary || "Meta provisioning failed",
        );
    } catch (error) {
      await this.fleet.update(job, "uncertain", job.step, error.message);
    }
  }
  async resume(job) {
    if (["creating", "waiting"].includes(job.step)) {
      await this.observe(job);
      return { observing: job.id };
    }
    await this.fleet.update(
      job,
      "queued",
      job.step,
      "Resuming the original deployment",
    );
    void this.fleet.tick().catch(() => {});
    return { resumed: job.id };
  }
}
