// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../store.mjs";
import { Fleet } from "../fleet.mjs";
import { Deployments } from "../deploy.mjs";

async function fixture(t, managed = true) {
  const directory = await mkdtemp(join(tmpdir(), "lavik-removal-"));
  const store = new Store(join(directory, "fleet.sqlite"));
  const fleet = new Fleet(store, {
    profile() {},
    async status() {
      throw new Error("offline");
    },
  });
  fleet.tick = async () => {};
  const calls = [];
  const ssh = {
    async call(host, request) {
      calls.push({ host: host.host, ...request });
      return { ok: true };
    },
  };
  const deployments = new Deployments(fleet, { ssh });
  fleet.deployments = deployments;
  await fleet.add({ id: "test", seeds: ["127.0.0.1:7200"] });
  const plan = {
    id: "test",
    owner: "retained-owner",
    baseDir: ".local/share/lavik/clusters",
    storage: "file",
    supervisor: "process",
    hosts: [{ host: "host1" }, { host: "host2" }],
    nodes: [
      { name: "meta-1", host: 0 },
      { name: "data-1", host: 1 },
    ],
    monitorHosts: [1],
  };
  if (managed)
    await store.query(
      "INSERT INTO deployments VALUES(?,?)",
      ["test", JSON.stringify(plan)],
      "run",
    );
  t.after(async () => {
    await store.close();
    await rm(directory, { recursive: true });
  });
  const addJob = (state) =>
    store.query(
      "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
      [
        "history",
        "test",
        "deploy",
        "{}",
        state,
        "done",
        "retained history",
        null,
        "now",
        "now",
      ],
      "run",
    );
  return { store, fleet, deployments, ssh, calls, plan, addJob };
}

test("disconnect archives completed history and ownership even when Meta is offline", async (t) => {
  const f = await fixture(t);
  await f.addJob("completed");
  const review = await f.deployments.reviewRemoval("test");
  await assert.rejects(
    f.deployments.remove("test", {
      token: review.token,
      confirm: "wrong",
      mode: "disconnect",
    }),
    /exact cluster name/,
  );
  await f.deployments.remove("test", {
    token: review.token,
    confirm: "test",
    mode: "disconnect",
  });
  assert.deepEqual(await f.fleet.clusters(), []);
  assert.deepEqual(f.calls, []);
  const archived = JSON.parse(
    (await f.store.query("SELECT snapshot FROM cluster_archives", [], "get"))
      .snapshot,
  );
  assert.equal(archived.jobs[0].detail, "retained history");
  assert.equal(JSON.parse(archived.deployment.plan).owner, "retained-owner");
  await assert.rejects(f.fleet.view("test"), /not found/);
  await f.fleet.add({ id: "test", seeds: ["127.0.0.1:7200"] });
  assert.equal((await f.fleet.clusters()).length, 1);
});

test("active admission and stale review reject removal without dropping catalog state", async (t) => {
  const f = await fixture(t);
  const review = await f.deployments.reviewRemoval("test");
  await f.addJob("queued");
  for (const mode of ["disconnect", "teardown"])
    await assert.rejects(
      f.deployments.remove("test", {
        token: review.token,
        confirm: "test",
        mode,
      }),
      /active operations/,
    );
  assert.equal((await f.store.query("SELECT * FROM jobs")).length, 1);
  assert.equal((await f.fleet.clusters()).length, 1);
  assert.deepEqual(await f.store.query("SELECT * FROM cluster_archives"), []);
  await f.deployments.save({ ...f.plan, revision: "changed" });
  await assert.rejects(
    f.deployments.remove("test", {
      token: review.token,
      confirm: "test",
      mode: "teardown",
    }),
    /changed/,
  );
  assert.deepEqual(f.calls, []);
});

test("teardown checks all hosts, stops all hosts, then removes owned data and archives", async (t) => {
  const f = await fixture(t);
  const review = await f.deployments.reviewRemoval("test");
  const job = await f.deployments.remove("test", {
    token: review.token,
    confirm: "test",
    mode: "teardown",
    hosts: [{ host: "attacker" }],
  });
  await f.fleet.execute(job);
  assert.deepEqual(
    f.calls.map((r) => `${r.phase}:${r.host}`),
    [
      "check:host1",
      "check:host2",
      "stop:host1",
      "stop:host2",
      "delete:host1",
      "delete:host2",
    ],
  );
  assert.ok(f.calls.every((r) => r.owner === "retained-owner"));
  assert.deepEqual(await f.fleet.clusters(), []);
  const archive = JSON.parse(
    (await f.store.query("SELECT snapshot FROM cluster_archives", [], "get"))
      .snapshot,
  );
  assert.equal(archive.jobs[0].state, "completed");
});

test("unreachable hosts and lost delete replies retain a resumable teardown", async (t) => {
  const f = await fixture(t);
  const review = await f.deployments.reviewRemoval("test");
  const job = await f.deployments.remove("test", {
    token: review.token,
    confirm: "test",
    mode: "teardown",
  });
  const original = f.ssh.call;
  f.ssh.call = async (host, req) => {
    if (host.host === "host2") throw new Error("offline");
    return original(host, req);
  };
  await f.fleet.execute(job);
  assert.ok(f.calls.every((r) => r.phase === "check"));
  let saved = await f.store.query(
    "SELECT * FROM jobs WHERE id=?",
    [job.id],
    "get",
  );
  assert.equal(saved.state, "uncertain");
  const before = f.calls.length;
  await f.fleet.observe(saved);
  assert.equal(f.calls.length, before, "observer must not replay deletion");
  f.ssh.call = async (host, req) => {
    if (req.phase === "delete" && host.host === "host2")
      throw new Error("reply lost");
    return original(host, req);
  };
  await f.fleet.resume("test", job.id);
  await f.fleet.execute(saved);
  assert.equal((await f.fleet.clusters()).length, 1);
  f.ssh.call = original;
  await f.fleet.resume("test", job.id);
  await f.fleet.execute(saved);
  assert.deepEqual(await f.fleet.clusters(), []);
});

test("imported and SPDK clusters cannot acquire destructive ownership via request inputs", async (t) => {
  const f = await fixture(t, false);
  let review = await f.deployments.reviewRemoval("test");
  assert.equal(review.teardown, false);
  await assert.rejects(
    f.deployments.remove("test", {
      token: review.token,
      confirm: "test",
      mode: "teardown",
      plan: f.plan,
    }),
    /Admin-owned/,
  );
  await f.store.query(
    "INSERT INTO deployments VALUES(?,?)",
    ["test", JSON.stringify({ ...f.plan, storage: "spdk" })],
    "run",
  );
  review = await f.deployments.reviewRemoval("test");
  assert.equal(review.teardown, false);
  assert.match(review.reason, /SPDK/);
  await assert.rejects(
    f.deployments.remove("test", {
      token: review.token,
      confirm: "test",
      mode: "teardown",
    }),
    /file-storage/,
  );
  assert.deepEqual(f.calls, []);
});

test("an in-flight status read cannot republish a removed connection", async (t) => {
  const f = await fixture(t, false);
  let release, started;
  const entered = new Promise((r) => (started = r));
  f.fleet.meta.status = async () => {
    started();
    return new Promise((r) => (release = r));
  };
  f.fleet.meta.nodes = async () => [];
  const read = f.fleet.view("test");
  await entered;
  await f.fleet.forget("test");
  release({});
  await read;
  assert.equal(f.fleet.views.has("test"), false);
  await assert.rejects(f.fleet.view("test"), /not found/);
});
