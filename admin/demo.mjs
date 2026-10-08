// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import {
  mkdir,
  readFile,
  writeFile,
  access,
  open,
  copyFile,
} from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { AdminError } from "./meta.mjs";

const ID = "demo-cluster";
const stamp = () => new Date().toISOString();
const services = ["meta-1", "meta-2", "meta-3", "data-1", "data-2", "data-3"];

/** Compose owns only the demo nodes and their release; the caller already runs Admin. */
export function demoCompose(plan) {
  const labels = { "io.lavik.admin.owner": plan.owner };
  const runtime = { image: `${plan.project}:runtime`, build: "." };
  const volumes = Object.fromEntries(
    ["release", ...services].map((name) => [name, { labels }]),
  );
  const config = {
    services: {
      download: {
        ...runtime,
        labels,
        profiles: ["tools"],
        entrypoint: ["bash", "/quickstart/download.sh"],
        volumes: [
          ".:/quickstart:ro",
          "release:/release",
          ...(plan.bundled ? ["./bundled:/bundled:ro"] : []),
          ...services.map((s) => `${s}:/existing/${s}:ro`),
        ],
        environment: { LAVIK_QUICKSTART_VERSION: plan.release.tag },
      },
    },
    networks: {
      default: { labels },
      cluster: { external: true, name: `${plan.project}-cluster` },
    },
    volumes,
  };
  for (const node of plan.nodes) {
    config.services[node.name] = {
      ...runtime,
      labels,
      init: true,
      restart: "unless-stopped",
      stop_grace_period: "60s",
      security_opt: ["seccomp=unconfined"],
      entrypoint: ["sh", "/quickstart/start.sh"],
      command: [node.kind, node.name.split("-")[1]],
      environment: { LAVIK_DEMO_PREFIX: plan.prefix },
      volumes: [
        ".:/quickstart:ro",
        "release:/release:ro",
        `${node.name}:/data`,
      ],
      networks: { cluster: { ipv4_address: plan.hosts[node.host].address } },
    };
  }
  return config;
}

/** Bounded Docker argv transport. No browser input is interpolated into a shell. */
export function dockerCommand(
  args,
  { input, timeout = 30000, onOutput, signal, env } = {},
) {
  return new Promise((resolve, reject) => {
    const child = spawn("docker", args, {
      stdio: ["pipe", "pipe", "pipe"],
      signal,
      env,
    });
    let stdout = "",
      stderr = "",
      failure;
    const timer = setTimeout(() => {
      failure = new Error(
        "Docker command timed out; inspect progress before retrying",
      );
      child.kill("SIGTERM");
    }, timeout);
    for (const [stream, name] of [
      [child.stdout, "stdout"],
      [child.stderr, "stderr"],
    ]) {
      stream.on("data", (data) => {
        if (name === "stdout") stdout += data;
        else stderr += data;
        onOutput?.(data.toString());
        if (
          Buffer.byteLength(stdout) + Buffer.byteLength(stderr) >
          12 * 1024 * 1024
        ) {
          failure = new Error("Docker command output exceeded its limit");
          child.kill("SIGTERM");
        }
      });
    }
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (code) => {
      clearTimeout(timer);
      failure
        ? reject(failure)
        : resolve({ code, stdout: stdout.trim(), stderr: stderr.trim() });
    });
    child.stdin.on("error", () => {});
    child.stdin.end(input);
  });
}

/** Durable local demo setup using the shared catalog, ownership plan and job executor. */
export class Demo {
  constructor(fleet, directory, { run = dockerCommand } = {}) {
    this.fleet = fleet;
    this.store = fleet.store;
    this.directory = directory;
    this.abort = new AbortController();
    this.run = (args, options = {}) =>
      run(args, { ...options, signal: this.abort.signal });
    this.logs = new Map();
  }
  stop() {
    this.abort.abort();
  }
  async plan() {
    const plan = await this.fleet.deployments.plan(ID);
    return plan?.kind === "docker-demo" ? plan : null;
  }
  async status() {
    const plan = await this.plan();
    if (!plan) return { state: "idle" };
    const job = await this.store.query(
      "SELECT * FROM jobs WHERE cluster_id=? AND kind='demo' ORDER BY created_at DESC LIMIT 1",
      [ID],
      "get",
    );
    let log = this.logs.get(plan.owner);
    if (log === undefined) {
      try {
        log = await readFile(join(plan.directory, "progress.log"), "utf8");
      } catch {
        log = "";
      }
    }
    return {
      id: ID,
      state: job?.state || "idle",
      step: job?.step,
      detail: job?.detail,
      log: log.slice(-16000),
      project: plan.project,
    };
  }
  /** Repeated clicks return the same intent. Only explicit retry resumes interrupted setup. */
  async start() {
    const profile = this.fleet.meta.profile({ profile: "lavik-demo" });
    if (!profile.allowPlaintext || profile.ca || profile.cert || profile.key)
      throw new AdminError(
        "The reserved lavik-demo profile must permit plaintext on the isolated Docker network.",
      );
    let plan = await this.plan();
    if (plan) {
      const job = await this.store.query(
        "SELECT * FROM jobs WHERE cluster_id=? AND kind='demo' ORDER BY created_at DESC LIMIT 1",
        [ID],
        "get",
      );
      if (job && ["completed", "failed", "uncertain"].includes(job.state)) {
        if (this.fleet.busy.has(job.id))
          throw new AdminError(
            "Demo setup is still stopping; retry shortly",
            409,
          );
        await this.resume(job);
      }
      return this.status();
    }
    const owner = randomBytes(16).toString("hex");
    plan = {
      kind: "docker-demo",
      id: ID,
      name: ID,
      owner,
      project: `lavik-demo-${owner.slice(0, 16)}`,
      directory: join(this.directory, "demos", owner),
      release: { tag: "nightly" },
      storage: "file",
      modern: true,
      hosts: [],
      nodes: [],
      monitorHosts: [],
    };
    const time = stamp();
    try {
      await this.store.batch([
        {
          sql: "INSERT INTO clusters VALUES(?,?,?,?,?)",
          params: [ID, ID, "[]", "lavik-demo", time],
        },
        {
          sql: "INSERT INTO deployments VALUES(?,?)",
          params: [ID, JSON.stringify(plan)],
        },
        {
          sql: "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
          params: [
            owner,
            ID,
            "demo",
            "{}",
            "queued",
            "docker",
            "Checking Docker on the Admin machine",
            null,
            time,
            time,
          ],
        },
      ]);
    } catch (error) {
      if (error.message.includes("UNIQUE")) {
        if (await this.plan()) return this.status();
        throw new AdminError(
          "demo-cluster already names another connection. Remove that connection or use another Admin workspace.",
          409,
        );
      }
      throw error;
    }
    await this.fleet.audit("demo-requested", ID, owner);
    void this.fleet.tick().catch(() => {});
    return this.status();
  }
  args(plan, args) {
    return [
      "compose",
      "--project-name",
      plan.project,
      "--project-directory",
      plan.directory,
      "-f",
      join(plan.directory, "compose.json"),
      ...args,
    ];
  }
  async checked(args, options) {
    let result;
    try {
      result = await this.run(args, options);
    } catch (error) {
      if (error.code === "ENOENT")
        throw new AdminError(
          "Docker CLI is not installed. Install Docker Desktop (Mac) or Docker Engine with Compose v2 (Linux), then retry. Run ./lavik-admin on that computer.",
        );
      throw error;
    }
    if (result.code !== 0)
      throw new AdminError(
        (result.stderr || result.stdout || "Docker command failed").slice(
          -4000,
        ),
      );
    return result.stdout;
  }
  context(plan, options = {}) {
    return plan.dockerHost
      ? {
          ...options,
          env: {
            ...process.env,
            DOCKER_CONTEXT: "",
            DOCKER_HOST: plan.dockerHost,
          },
        }
      : options;
  }
  async checkDocker() {
    // Bind mounts and container execution must refer to the Admin computer.
    // Never silently send a local demo to a selected production Docker context.
    let host = process.env.DOCKER_HOST;
    if (process.env.DOCKER_CONTEXT || !host) {
      const contexts = JSON.parse(await this.checked(["context", "inspect"]));
      host = contexts[0]?.Endpoints?.docker?.Host;
    }
    if (!host?.startsWith("unix://"))
      throw new AdminError(
        "Select a local Docker Desktop/Engine context before starting the demo; remote Docker contexts are not supported.",
      );
    const info = JSON.parse(
      await this.checked(["info", "--format", "{{json .}}"]),
    );
    if (info.OSType !== "linux")
      throw new AdminError(
        "Switch Docker to Linux containers before starting the demo.",
      );
    await this.checked(["compose", "version"]);
    try {
      await access("/.dockerenv");
    } catch {
      return { host, arch: info.Architecture };
    }
    throw new AdminError(
      "Start ./lavik-admin on the Docker host to use the demo button. An Admin container does not need Docker socket access.",
    );
  }
  async network(plan) {
    const name = `${plan.project}-cluster`;
    let result = await this.run(
      ["network", "inspect", name],
      this.context(plan),
    );
    if (result.code !== 0) {
      await this.checked(
        [
          "network",
          "create",
          "--label",
          `io.lavik.admin.owner=${plan.owner}`,
          ...(plan.subnet ? ["--subnet", plan.subnet] : []),
          name,
        ],
        this.context(plan),
      );
      result = await this.run(["network", "inspect", name], this.context(plan));
    }
    if (result.code !== 0)
      throw new AdminError(result.stderr || "Cannot inspect demo network");
    const network = JSON.parse(result.stdout)[0];
    if (network.Labels?.["io.lavik.admin.owner"] !== plan.owner)
      throw new AdminError(
        "Demo network ownership does not match; no existing resources were changed",
        409,
      );
    const subnet = network.IPAM?.Config?.map((c) => c.Subnet).find((s) =>
      /^\d+\.\d+\.\d+\.0\/\d+$/.test(s),
    );
    if (!subnet || Number(subnet.split("/")[1]) > 27)
      throw new AdminError(
        "Docker did not allocate a suitable IPv4 network for six demo nodes",
      );
    const prefix = subnet.split(".").slice(0, 3).join(".");
    if (plan.prefix && plan.prefix !== prefix)
      throw new AdminError(
        "The demo network changed; restore its original network before restarting",
        409,
      );
    if (!plan.prefix) {
      plan.prefix = prefix;
      plan.subnet = subnet;
      plan.hosts = services.map((name) => ({ host: name, address: "" }));
      plan.nodes = services.map((name, i) => {
        const kind = name.startsWith("meta") ? "meta" : "data";
        const index = Number(name.split("-")[1]);
        plan.hosts[i].address = `${prefix}.${
          (kind === "meta" ? 10 : 20) + index
        }`;
        return {
          name,
          kind,
          host: i,
          id: kind === "data" ? String(index).padStart(40, "0") : index,
          port: kind === "meta" ? 7100 : 6379,
        };
      });
      await this.store.batch([
        {
          sql: "UPDATE deployments SET plan=? WHERE cluster_id=?",
          params: [JSON.stringify(plan), ID],
        },
        {
          sql: "UPDATE clusters SET seeds=? WHERE id=?",
          params: [
            JSON.stringify([11, 12, 13].map((n) => `${prefix}.${n}:7200`)),
            ID,
          ],
        },
      ]);
    }
  }
  /** Copy only release runtime inputs, and only when every ELF matches Docker. */
  async bundle(plan, arch) {
    if (plan.bundled) return;
    try {
      await access(join(plan.directory, "compose.json"));
      return;
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    const machine = ["aarch64", "arm64"].includes(arch)
      ? 183
      : ["x86_64", "amd64"].includes(arch)
      ? 62
      : null;
    if (!machine) return;
    const binaries = ["lavik", "lavik-meta", "lavik-ctl", "runtime/bin/node"];
    const source = new URL("../", import.meta.url);
    for (const name of binaries) {
      let file;
      try {
        file = await open(new URL(name, source), "r");
        const header = Buffer.alloc(20);
        const { bytesRead } = await file.read(header, 0, 20, 0);
        if (
          bytesRead !== 20 ||
          header.toString("hex", 0, 4) !== "7f454c46" ||
          header[5] !== 1 ||
          header.readUInt16LE(18) !== machine
        )
          return;
      } catch (error) {
        if (error.code === "ENOENT") return;
        throw error;
      } finally {
        await file?.close();
      }
    }
    // The workspace copy keeps Docker mounts independent of the extracted
    // archive location. Never copy a user's whole extraction directory.
    const destination = join(plan.directory, "bundled");
    await mkdir(join(destination, "runtime/bin"), {
      recursive: true,
      mode: 0o700,
    });
    for (const name of [
      ...binaries,
      "VERSION",
      "REVISION",
      "LICENSE",
      "NOTICE",
      "THIRD_PARTY_NOTICES",
      "OPENSSL-LICENSE.txt",
      "runtime/LICENSE",
    ]) {
      try {
        await copyFile(new URL(name, source), join(destination, name));
      } catch (error) {
        if (error.code !== "ENOENT" || ["VERSION", "REVISION"].includes(name))
          throw error;
      }
    }
    plan.bundled = true;
    plan.release.tag = "bundled";
    await this.fleet.deployments.save(plan);
  }
  async stage(plan) {
    await mkdir(plan.directory, { recursive: true, mode: 0o700 });
    const files = ["Dockerfile", "download.sh", "start.sh", "client.mjs"];
    const contents = Object.fromEntries(
      await Promise.all(
        files.map(async (name) => [
          name,
          await readFile(new URL(`./quickstart/${name}`, import.meta.url)),
        ]),
      ),
    );
    contents["cluster.toml"] = (
      await readFile(
        new URL("./quickstart/cluster.toml", import.meta.url),
        "utf8",
      )
    ).replaceAll("172.29.91", plan.prefix);
    contents["resp.mjs"] = await readFile(
      new URL("./resp.mjs", import.meta.url),
    );
    contents["meta.mjs"] = await readFile(
      new URL("./meta.mjs", import.meta.url),
    );
    contents["compose.json"] = JSON.stringify(demoCompose(plan), null, 2);
    for (const [name, content] of Object.entries(contents)) {
      try {
        await writeFile(join(plan.directory, name), content, {
          flag: "wx",
          mode: 0o600,
        });
      } catch (error) {
        if (error.code !== "EEXIST") throw error;
      }
    }
  }
  async compose(plan, args, timeout = 30000, log = false) {
    return this.checked(
      this.args(plan, args),
      this.context(plan, {
        timeout,
        onOutput: log
          ? (chunk) =>
              this.logs.set(
                plan.owner,
                ((this.logs.get(plan.owner) || "") + chunk).slice(-16000),
              )
          : undefined,
      }),
    );
  }
  /** Checkpoint creation before sending it; Docker retries never replay Genesis. */
  async execute(job) {
    const plan = await this.plan();
    try {
      if (["creating", "waiting"].includes(job.step))
        return await this.observe(job);
      await this.fleet.update(
        job,
        "running",
        "docker",
        "Checking Docker on the Admin machine",
      );
      const engine = await this.checkDocker();
      const host = engine?.host;
      if (plan.dockerHost && host !== plan.dockerHost)
        throw new AdminError(
          "Select the original local Docker context before resuming this demo.",
          409,
        );
      if (host && !plan.dockerHost) {
        plan.dockerHost = host;
        await this.fleet.deployments.save(plan);
      }
      await this.network(plan);
      await this.bundle(plan, engine?.arch);
      await this.stage(plan);
      this.logs.set(plan.owner, "");
      await this.fleet.update(
        job,
        "running",
        "runtime",
        "Preparing the Docker runtime image",
      );
      await this.compose(plan, ["build", "download"], 1200000, true);
      await this.fleet.update(
        job,
        "running",
        "download",
        plan.bundled
          ? "Installing the Linux release included in this package"
          : "Downloading and verifying the Linux release",
      );
      await this.compose(
        plan,
        ["run", "--rm", "--no-deps", "download"],
        1500000,
        true,
      );
      await this.fleet.update(
        job,
        "running",
        "starting",
        "Starting three Meta and three Data containers",
      );
      await this.compose(
        plan,
        ["up", "-d", "--no-build", ...services],
        180000,
        true,
      );
      const cluster = await this.fleet.cluster(ID);
      let status;
      for (let attempt = 0; attempt < 30; attempt++) {
        try {
          status = await this.fleet.meta.status(cluster);
          break;
        } catch (error) {
          if (attempt === 29) throw error;
          await delay(1000);
        }
      }
      if (status.cluster_ready) {
        await this.complete(job, plan);
        return;
      }
      if (status.cluster_state === "uninitialized") {
        if (plan.initialized)
          throw new AdminError(
            "This demo was already initialized. Restore its Meta volumes; setup will not recreate lost cluster state.",
          );
        await this.store.query(
          "UPDATE jobs SET state='running',step='creating',input=?,detail=?,updated_at=? WHERE id=?",
          [
            JSON.stringify({ deadline: Date.now() + 300000 }),
            "Initializing demo-cluster",
            stamp(),
            job.id,
          ],
          "run",
        );
        const result = await this.ctl(plan, [
          "cluster-create",
          "--manifest",
          "/quickstart/cluster.toml",
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
      }
      await this.store.query(
        "UPDATE jobs SET input=? WHERE id=?",
        [JSON.stringify({ deadline: Date.now() + 300000 }), job.id],
        "run",
      );
      await this.fleet.update(
        job,
        "running",
        "waiting",
        "Waiting for the primary and replicas to become ready",
      );
    } catch (error) {
      const saved = await this.store.query(
        "SELECT step FROM jobs WHERE id=?",
        [job.id],
        "get",
      );
      await this.fleet.update(
        job,
        ["creating", "waiting"].includes(saved.step) ? "uncertain" : "failed",
        saved.step,
        error.message,
      );
    } finally {
      if (plan) {
        try {
          await writeFile(
            join(plan.directory, "progress.log"),
            this.logs.get(plan.owner) || "",
            { mode: 0o600 },
          );
        } catch {
          /* Catalog detail still reports the error if staging failed. */
        }
      }
    }
  }
  async complete(job, plan) {
    plan.initialized = true;
    await this.fleet.deployments.save(plan);
    await this.fleet.update(
      job,
      "completed",
      "completed",
      "Demo ready. Three Meta voters, one primary and two replicas.",
    );
  }
  async observe(job) {
    if (!["creating", "waiting"].includes(job.step)) return;
    try {
      const status = await this.fleet.meta.status(await this.fleet.cluster(ID));
      if (status.cluster_ready) await this.complete(job, await this.plan());
      else if (
        status.cluster_state === "provisioning-failed" ||
        Date.now() > (JSON.parse(job.input).deadline || 0)
      )
        await this.fleet.update(
          job,
          "uncertain",
          job.step,
          status.provisioning_failure_summary ||
            "Demo is not ready. Inspect Operations; creation will not be submitted again.",
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
      "Retrying demo setup with the retained containers and data",
    );
    void this.fleet.tick().catch(() => {});
    return { resumed: job.id };
  }
  async ctl(plan, args, read = false) {
    const forwarded = [...args];
    const manifest = forwarded.indexOf("--manifest");
    if (manifest >= 0) forwarded[manifest + 1] = "/quickstart/cluster.toml";
    let result;
    // Reads may try another Meta container. A mutation is sent once only,
    // since losing docker exec's reply does not prove that Meta rejected it.
    for (const gateway of read ? ["meta-1", "meta-2", "meta-3"] : ["meta-1"]) {
      result = await this.run(
        this.args(plan, [
          "exec",
          "-T",
          gateway,
          "/release/current/lavik-ctl",
          ...forwarded,
        ]),
        this.context(plan, { timeout: 20000 }),
      );
      if (result.code === 0) break;
    }
    return result;
  }

  async data(plan, address, args) {
    const endpoints = plan.nodes
      .filter((n) => n.kind === "data")
      .map((n) => `tcp://${plan.hosts[n.host].address}:${n.port}`);
    if (!endpoints.includes(address))
      throw new AdminError(
        "Endpoint is outside this demo's retained topology",
        409,
      );
    const node = plan.nodes.find(
      (n) => `tcp://${plan.hosts[n.host].address}:${n.port}` === address,
    );
    const raw = await this.checked(
      this.args(plan, [
        "exec",
        "-T",
        node.name,
        "/release/current/runtime/bin/node",
        "/quickstart/client.mjs",
      ]),
      this.context(plan, {
        input: JSON.stringify({ address, args }),
        timeout: 15000,
      }),
    );
    const value = JSON.parse(raw, (_key, item) =>
      item?.base64 !== undefined ? Buffer.from(item.base64, "base64") : item,
    );
    if (value.error) throw new AdminError(value.error, 502);
    return value;
  }
  async teardown(plan, job) {
    try {
      await this.fleet.update(
        job,
        "running",
        "teardown",
        "Removing this workspace's demo containers and volumes",
      );
      const host = (await this.checkDocker())?.host;
      if (plan.dockerHost && host !== plan.dockerHost)
        throw new AdminError(
          "Select the original local Docker context before removing this demo.",
          409,
        );
      // The random project is retained before any Docker work. Every resource
      // must carry its nonce before Compose is allowed to remove that project.
      for (const [kind, filter] of [
        ["container", "ps"],
        ["volume", "volume"],
        ["network", "network"],
      ]) {
        const list =
          kind === "container"
            ? [
                "ps",
                "-aq",
                "--filter",
                `label=com.docker.compose.project=${plan.project}`,
              ]
            : [
                filter,
                "ls",
                "-q",
                "--filter",
                `label=com.docker.compose.project=${plan.project}`,
              ];
        const ids = (await this.checked(list, this.context(plan)))
          .split(/\s+/)
          .filter(Boolean);
        if (ids.length) {
          const objects = JSON.parse(
            await this.checked([kind, "inspect", ...ids], this.context(plan)),
          );
          if (
            objects.some(
              (o) =>
                (kind === "container" ? o.Config.Labels : o.Labels)?.[
                  "io.lavik.admin.owner"
                ] !== plan.owner,
            )
          )
            throw new AdminError(
              "Docker resource ownership changed; inspect the demo before removing it",
              409,
            );
        }
      }
      try {
        await access(join(plan.directory, "compose.json"));
        await this.compose(
          plan,
          ["--profile", "tools", "down", "--volumes", "--remove-orphans"],
          180000,
          true,
        );
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
      const network = await this.run(
        ["network", "inspect", `${plan.project}-cluster`],
        this.context(plan),
      );
      if (network.code === 0) {
        if (
          JSON.parse(network.stdout)[0].Labels?.["io.lavik.admin.owner"] !==
          plan.owner
        )
          throw new AdminError("Demo network ownership changed", 409);
        await this.checked(
          ["network", "rm", `${plan.project}-cluster`],
          this.context(plan),
        );
      }
      await this.fleet.forget(ID, job.id);
    } catch (error) {
      await this.fleet.update(job, "uncertain", "teardown", error.message);
    }
  }
}
