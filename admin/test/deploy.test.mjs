// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../store.mjs";
import { Hosts } from "../hosts.mjs";
import { Fleet } from "../fleet.mjs";
import {
  Deployments,
  settings,
  topology,
  manifest,
  nodeArguments,
} from "../deploy.mjs";
import { sshHost, sshArguments } from "../ssh.mjs";
import { Releases } from "../releases.mjs";
import { createHash } from "node:crypto";

const input = {
  id: "test",
  hosts: [{ host: "127.0.0.1", user: "ubuntu" }],
  supervisor: "process",
  dataGiB: 1,
};
const release = {
  tag: "nightly",
  assets: {
    aarch64: {
      sha256: "a".repeat(64),
      url: "https://github.com/eloqdata/lavik/releases/download/nightly/package.tar.gz",
    },
  },
};

test("setup validates before SSH; topology and both CLI generations agree", () => {
  assert.throws(() => settings({ ...input, baseDir: "../../existing" }));
  assert.throws(() => sshHost({ host: "-oProxyCommand=oops", user: "ubuntu" }));
  assert.throws(() => sshHost({ host: "host;touch", user: "ubuntu" }));
  assert.throws(() => settings({ ...input, clientMode: "single", groups: 2 }));
  const plan = { ...settings(input), modern: true };
  plan.nodes = topology(plan);
  assert.equal(plan.nodes.length, 6);
  assert.match(manifest(plan), /client_mode = "cluster"/);
  assert.ok(nodeArguments(plan, plan.nodes[3]).includes("--meta-seed"));
  assert.ok(nodeArguments(plan, plan.nodes[0]).includes("@ROOT@/meta-1/state"));
  plan.modern = false;
  assert.doesNotMatch(manifest(plan), /client_mode/);
  assert.ok(nodeArguments(plan, plan.nodes[3]).includes("--cluster-enabled"));
  assert.ok(
    sshArguments(sshHost(input.hosts[0])).includes("StrictHostKeyChecking=yes"),
  );
  assert.ok(sshArguments(sshHost(input.hosts[0])).includes("BatchMode=yes"));
});

async function fixture() {
  const directory = await mkdtemp(join(tmpdir(), "lavik-deploy-unit-"));
  const store = new Store(join(directory, "fleet.sqlite"));
  const meta = {
    profiles: { default: { allowPlaintext: true } },
    async status() {
      return { cluster_state: "created", cluster_ready: true };
    },
  };
  const fleet = new Fleet(store, meta);
  fleet.tick = async () => {};
  const calls = [];
  const ssh = {
    async call(_host, request) {
      calls.push(request);
      return {
        ok: true,
        errors: [],
        arch: "aarch64",
        cpus: 4,
        freeBytes: 50 * 1024 ** 3,
      };
    },
  };
  const deployments = new Deployments(fleet, {
    ssh,
    releases: {
      async resolve() {
        return release;
      },
    },
  });
  fleet.deployments = deployments;
  return {
    directory,
    store,
    fleet,
    deployments,
    calls,
    async close() {
      await store.close();
      await rm(directory, { recursive: true, force: true });
    },
  };
}

test("review is retained server-side; duplicate submits and cluster conflicts are atomic", async () => {
  const f = await fixture();
  try {
    const preview = await f.deployments.preview(input);
    assert.equal(preview.ready, true);
    assert.ok(f.calls.every((c) => c.action === "probe"));
    await assert.rejects(
      f.deployments.create({ token: preview.token, confirm: "wrong" }),
    );
    const job = await f.deployments.create({
      token: preview.token,
      confirm: "test",
      release: { url: "evil" },
    });
    assert.equal(
      (await f.deployments.create({ token: preview.token, confirm: "test" }))
        .id,
      job.id,
    );
    assert.equal((await f.deployments.plan("test")).release.tag, "nightly");
    const second = await f.deployments.preview(input);
    await assert.rejects(
      f.deployments.create({ token: second.token, confirm: "test" }),
      /already exists/,
    );
    assert.equal((await f.fleet.clusters()).length, 1);
    assert.equal((await f.store.query("SELECT * FROM jobs")).length, 1);
  } finally {
    await f.close();
  }
});

test("restart observes creation but never replays installation or an uncertain Genesis", async () => {
  const f = await fixture();
  try {
    const preview = await f.deployments.preview(input);
    const job = await f.deployments.create({
      token: preview.token,
      confirm: "test",
    });
    await f.fleet.update(job, "uncertain", "starting", "interrupted");
    await f.deployments.observe({
      ...job,
      state: "uncertain",
      step: "starting",
    });
    assert.ok(f.calls.every((c) => c.action === "probe"));
    await f.deployments.observe({
      ...job,
      state: "uncertain",
      step: "creating",
    });
    assert.equal(
      (
        await f.store.query(
          "SELECT state FROM jobs WHERE id=?",
          [job.id],
          "get",
        )
      ).state,
      "completed",
    );
    f.fleet.meta.status = async () => ({ cluster_state: "uninitialized" });
    await f.fleet.update(job, "uncertain", "creating", "timeout");
    await f.deployments.run({ ...job, step: "creating" });
    assert.equal(
      (
        await f.store.query(
          "SELECT state FROM jobs WHERE id=?",
          [job.id],
          "get",
        )
      ).state,
      "uncertain",
    );
    assert.ok(f.calls.every((c) => c.action === "probe"));
  } finally {
    await f.close();
  }
});

test("cached release survives moving nightly, and checksum mismatch never publishes cache bytes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lavik-cache-test-"));
  try {
    const data = Buffer.from("verified release");
    const asset = {
      sha256: createHash("sha256").update(data).digest("hex"),
      url: "https://github.com/eloqdata/lavik/releases/download/nightly/test.tar.gz",
    };
    let calls = 0;
    const releases = new Releases(async () => {
      calls++;
      return new Response(data);
    }, directory);
    const path = await releases.download(asset);
    releases.fetcher = async () => {
      throw new Error("nightly moved");
    };
    assert.equal(await releases.download(asset), path);
    assert.equal(calls, 1);
    assert.deepEqual(await readFile(path), data);
    releases.fetcher = async () => new Response("wrong");
    await assert.rejects(
      releases.download({ ...asset, sha256: "0".repeat(64) }),
      /checksum mismatch/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("failed host checks cannot admit a cluster or run a mutating SSH action", async () => {
  const f = await fixture();
  try {
    f.deployments.ssh.call = async () => {
      throw new Error("Host key verification failed");
    };
    const preview = await f.deployments.preview(input);
    assert.equal(preview.ready, false);
    await assert.rejects(
      f.deployments.create({ token: preview.token, confirm: "test" }),
      /prerequisites/,
    );
    assert.deepEqual(await f.fleet.clusters(), []);
    assert.deepEqual(await f.store.query("SELECT * FROM jobs"), []);
  } finally {
    await f.close();
  }
});

test("a stale follower review cannot replace a changed plan or create an orphan job", async () => {
  const f = await fixture();
  try {
    const preview = await f.deployments.preview(input);
    const job = await f.deployments.create({
      token: preview.token,
      confirm: "test",
    });
    await f.fleet.update(job, "completed", "completed", "ready");
    const plan = await f.deployments.plan("test");
    plan.modern = true;
    await f.deployments.save(plan);
    f.fleet.view = async () => ({
      status: { groups: [{ group_id: "group-1" }] },
    });
    const follower = await f.deployments.previewFollower("test", {
      group: "group-1",
      host: input.hosts[0],
      dataPort: 16390,
    });
    // Simulate another admission between the read and the transactional update.
    const batch = f.store.batch.bind(f.store);
    f.store.batch = async (statements) => {
      await f.deployments.save({ ...plan, revision: "changed concurrently" });
      return batch(statements);
    };
    await assert.rejects(
      f.deployments.createFollower("test", {
        token: follower.token,
        confirm: "test",
      }),
      /changed/,
    );
    assert.equal(
      (await f.deployments.plan("test")).revision,
      "changed concurrently",
    );
    assert.equal((await f.store.query("SELECT * FROM jobs")).length, 1);
  } finally {
    await f.close();
  }
});

test("only explicit reads may fall back to another SSH host after a transport error", async () => {
  const f = await fixture();
  try {
    const preview = await f.deployments.preview({
      ...input,
      hosts: [...input.hosts, { host: "127.0.0.2", user: "ubuntu" }],
    });
    await f.deployments.create({ token: preview.token, confirm: "test" });
    const cluster = await f.fleet.cluster("test");
    const calls = [];
    f.deployments.ssh.call = async (host) => {
      calls.push(host.host);
      if (host.host === "127.0.0.1") throw new Error("SSH transport failed");
      return { code: 0, stdout: "OK" };
    };
    await assert.rejects(
      f.deployments.ctl(cluster, ["failover", "status"]),
      /transport/,
    );
    assert.deepEqual(calls, ["127.0.0.1"]);
    calls.length = 0;
    assert.equal(
      (await f.deployments.ctl(cluster, ["getnode", "node"], true)).code,
      0,
    );
    assert.deepEqual(calls, ["127.0.0.1", "127.0.0.2"]);
  } finally {
    await f.close();
  }
});

test("explicit six-host placement preserves roles in manifests and cannot refer to missing hosts", () => {
  const hosts = Array.from({ length: 6 }, (_, i) => ({
    host: `10.0.0.${i + 1}`,
    user: "ubuntu",
  }));
  const placement = {
    "meta-1": 0,
    "meta-2": 1,
    "meta-3": 2,
    "data-1": 3,
    "data-2": 4,
    "data-3": 5,
  };
  const config = settings({ ...input, hosts, placement });
  const nodes = topology(config);
  assert.deepEqual(
    nodes.map((n) => n.host),
    [0, 1, 2, 3, 4, 5],
  );
  assert.equal(nodes[3].role, "primary");
  assert.equal(nodes[4].role, "replica");
  assert.match(
    manifest({ ...config, nodes, modern: true }),
    /client_endpoint = "tcp:\/\/10.0.0.4:6379"/,
  );
  assert.throws(
    () => topology({ ...config, placement: { ...placement, "data-1": 6 } }),
    /valid host/,
  );
  assert.throws(
    () => topology({ ...config, placement: { "meta-1": 0 } }),
    /every node/,
  );
});

test("prepared hosts can replace legacy key paths for the same follower account, only after admission", async () => {
  const f = await fixture();
  try {
    const preview = await f.deployments.preview(input);
    const job = await f.deployments.create({
      token: preview.token,
      confirm: "test",
    });
    await f.fleet.update(job, "completed", "completed", "ready");
    const plan = await f.deployments.plan("test");
    plan.modern = true;
    await f.deployments.save(plan);
    f.fleet.view = async () => ({
      status: { groups: [{ group_id: "group-1" }] },
    });
    const inventory = new Hosts(f.store, f.directory);
    f.deployments.hosts = inventory;
    await inventory.save({
      ...plan.hosts[0],
      id: "prepared",
      identityFile: "/managed/key",
      knownHostsFile: "/managed/trust",
    });
    const follower = await f.deployments.previewFollower("test", {
      group: "group-1",
      host: { hostId: "prepared" },
      dataPort: 16390,
    });
    assert.equal(follower.ready, true);
    assert.equal(
      (await f.deployments.plan("test")).hosts[0].identityFile,
      undefined,
    );
    await assert.rejects(
      f.deployments.previewFollower("test", {
        group: "group-1",
        host: { hostId: "prepared", address: "10.0.0.99" },
        dataPort: 16390,
      }),
      /saved SSH user and cluster IP/,
    );
    await f.deployments.createFollower("test", {
      token: follower.token,
      confirm: "test",
    });
    assert.equal(
      (await f.deployments.plan("test")).hosts[0].identityFile,
      "/managed/key",
    );
  } finally {
    await f.close();
  }
});

test("Data ports start at the base on each IP and increment only for colocated nodes", () => {
  const hosts = [13, 11, 12].map((n) => ({
    host: "127.0.0.1",
    port: 2200 + n,
    address: `172.30.95.${n}`,
    user: "lavik",
  }));
  const config = settings({ ...input, hosts });
  const nodes = topology(config);
  const data = nodes.filter((n) => n.kind === "data");
  assert.deepEqual(
    data.map((n) => n.port),
    [6379, 6379, 6379],
  );
  assert.equal(config.hosts[data[0].host].address, "172.30.95.13");
  const placement = Object.fromEntries(nodes.map((n) => [n.name, n.host]));
  placement["data-2"] = 0;
  assert.deepEqual(
    topology({ ...config, placement })
      .filter((n) => n.kind === "data")
      .map((n) => n.port),
    [6379, 6380, 6379],
  );
  const sharedIP = settings({
    ...input,
    hosts: hosts.map((h) => ({ ...h, address: "172.30.95.13" })),
  });
  assert.deepEqual(
    topology(sharedIP)
      .filter((n) => n.kind === "data")
      .map((n) => n.port),
    [6379, 6380, 6381],
  );
  assert.throws(
    () => topology({ ...config, dataPort: config.metaPort }),
    /overlap/,
  );
  // Old manifests remain valid: starting and rendering a retained node must
  // use its saved port instead of reapplying the new planning default.
  const retained = { ...nodes[4], port: 6380 };
  assert.ok(
    nodeArguments({ ...config, nodes, modern: true }, retained).includes(
      "6380",
    ),
  );
});
