// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../store.mjs";
import { Fleet } from "../fleet.mjs";
import { Deployments } from "../deploy.mjs";
import { Demo, demoCompose } from "../demo.mjs";

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), "lavik-demo-test-"));
  const store = new Store(join(directory, "fleet.sqlite"));
  let ready = false,
    network;
  const calls = [];
  const meta = {
    profile: () => ({ allowPlaintext: true }),
    options: () => [],
    status: async () => ({
      cluster_state: ready ? "created" : "uninitialized",
      cluster_ready: ready,
    }),
  };
  const fleet = new Fleet(store, meta);
  fleet.tick = async () => {};
  fleet.deployments = new Deployments(fleet);
  const run = async (args) => {
    calls.push(args);
    if (args[0] === "context")
      return {
        code: 0,
        stdout: JSON.stringify([
          { Endpoints: { docker: { Host: "unix:///test.sock" } } },
        ]),
      };
    if (args[0] === "info") return { code: 0, stdout: '{"OSType":"linux"}' };
    if (args[0] === "network" && args[1] === "create") {
      network = {
        Labels: { "io.lavik.admin.owner": args[3].split("=")[1] },
        IPAM: { Config: [{ Subnet: "172.31.0.0/16" }] },
      };
      return { code: 0, stdout: "network" };
    }
    if (args[0] === "network" && args[1] === "inspect")
      return { code: network ? 0 : 1, stdout: JSON.stringify([network]) };
    if (args.includes("cluster-create")) ready = true;
    return { code: 0, stdout: "", stderr: "" };
  };
  const demo = new Demo(fleet, directory, { run });
  fleet.demo = demo;
  // Other tests exercise local-context rejection; this fixture is portable to CI containers.
  demo.checkDocker = async () => {};
  const job = async () =>
    store.query("SELECT * FROM jobs WHERE kind='demo'", [], "get");
  t.after(async () => {
    await store.close();
    await rm(directory, { recursive: true, force: true });
  });
  return { store, fleet, demo, directory, calls, meta, job };
}

test("double-clicking admits one demo and starts six nodes without another Admin", async (t) => {
  const f = await fixture(t);
  await Promise.all([f.demo.start(), f.demo.start()]);
  assert.equal((await f.store.query("SELECT * FROM jobs")).length, 1);
  await f.demo.execute(await f.job());
  const plan = await f.demo.plan();
  const config = JSON.parse(
    await readFile(join(plan.directory, "compose.json"), "utf8"),
  );
  assert.equal(config.services.admin, undefined);
  assert.equal(
    Object.keys(config.services).length,
    7,
    "six nodes and a one-shot downloader",
  );
  assert.equal(
    Object.keys(config.services).filter((s) => s !== "download").length,
    6,
  );
  assert.ok(config.services.download.profiles.includes("tools"));
  assert.deepEqual(
    config.services["data-1"].ports,
    undefined,
    "no host port conflicts",
  );
  assert.equal(
    config.services["meta-1"].networks.cluster.ipv4_address,
    "172.31.0.11",
  );
  assert.equal(
    config.volumes.release.labels["io.lavik.admin.owner"],
    plan.owner,
  );
  assert.equal(
    f.calls.filter((args) => args.includes("cluster-create")).length,
    1,
  );
  await f.demo.observe(await f.job());
  assert.equal((await f.demo.status()).state, "completed");
  await f.demo.start();
  assert.equal(
    f.calls.filter((args) => args.includes("cluster-create")).length,
    1,
  );
});

test("missing Docker gives a retryable error without creating host resources", async (t) => {
  const f = await fixture(t);
  f.demo.checkDocker = Demo.prototype.checkDocker;
  f.demo.run = async () => {
    throw Object.assign(new Error("missing"), { code: "ENOENT" });
  };
  await f.demo.start();
  await f.demo.execute(await f.job());
  const status = await f.demo.status();
  assert.equal(status.state, "failed");
  assert.match(status.detail, /Docker CLI is not installed/);
  assert.equal(f.calls.length, 0);
  await f.demo.start();
  assert.equal((await f.job()).state, "queued");
});

test("remote Docker contexts and foreign networks never receive provisioning", async (t) => {
  const f = await fixture(t);
  f.demo.checkDocker = Demo.prototype.checkDocker;
  f.demo.run = async () => ({
    code: 0,
    stdout: JSON.stringify([
      { Endpoints: { docker: { Host: "ssh://production" } } },
    ]),
  });
  await assert.rejects(f.demo.checkDocker(), /remote Docker contexts/);
  await f.demo.start();
  const plan = await f.demo.plan();
  f.demo.run = async () => ({
    code: 0,
    stdout: JSON.stringify([
      { Labels: { "io.lavik.admin.owner": "different" } },
    ]),
  });
  await assert.rejects(f.demo.network(plan), /ownership/);
});

test("download failure keeps owned intent and retries before creation; uncertain Genesis is never replayed", async (t) => {
  const f = await fixture(t);
  await f.demo.start();
  const run = f.demo.run;
  f.demo.run = async (args, options) =>
    args.includes("run")
      ? { code: 35, stdout: "", stderr: "TLS handshake failed" }
      : run(args, options);
  await f.demo.execute(await f.job());
  assert.equal((await f.job()).state, "failed");
  assert.match((await f.job()).detail, /TLS/);
  assert.equal(
    f.calls.filter((args) => args.includes("cluster-create")).length,
    0,
  );
  const owner = (await f.demo.plan()).owner;
  f.demo.run = run;
  await f.demo.start();
  await f.demo.execute(await f.job());
  assert.equal((await f.demo.plan()).owner, owner);
  await f.store.query(
    "UPDATE jobs SET state='uncertain',step='creating' WHERE kind='demo'",
    [],
    "run",
  );
  f.meta.status = async () => ({
    cluster_state: "uninitialized",
    cluster_ready: false,
  });
  const count = f.calls.length;
  await f.demo.start();
  await f.demo.execute(await f.job());
  assert.equal(f.calls.length, count, "uncertain creation only observes Meta");
});

test("demo transports run Linux clients in containers and preserve binary RESP replies", async (t) => {
  const f = await fixture(t);
  await f.demo.start();
  const plan = await f.demo.plan();
  await f.demo.network(plan);
  await f.demo.stage(plan);
  const calls = [];
  f.demo.run = async (args, options) => {
    calls.push({ args, options });
    if (args.includes("/quickstart/client.mjs"))
      return {
        code: 0,
        stdout: JSON.stringify({ value: [{ base64: "AP8=" }] }),
      };
    return {
      code: args.includes("meta-1") ? 1 : 0,
      stdout: "",
      stderr: "unavailable",
    };
  };
  const result = await f.demo.data(plan, "tcp://172.31.0.21:6379", [
    "GET",
    "key",
  ]);
  assert.deepEqual(result.value, [Buffer.from([0, 255])]);
  assert.ok(calls[0].args.includes("data-1"));
  await assert.rejects(
    f.demo.data(plan, "tcp://1.2.3.4:6379", ["PING"]),
    /outside this demo/,
  );
  calls.length = 0;
  await f.demo.ctl(plan, ["cluster-status"], true);
  assert.equal(calls.length, 2);
  calls.length = 0;
  await f.demo.ctl(plan, ["cluster-create"], false);
  assert.equal(
    calls.length,
    1,
    "a mutation must never retry via another container",
  );
});

test("teardown rejects a Docker resource whose retained owner changed", async (t) => {
  const f = await fixture(t);
  await f.demo.start();
  await f.demo.execute(await f.job());
  await f.demo.observe(await f.job());
  const review = await f.fleet.deployments.reviewRemoval("demo-cluster");
  assert.match(review.impact, /Admin.*remain/);
  const job = await f.fleet.deployments.remove("demo-cluster", {
    token: review.token,
    confirm: "demo-cluster",
    mode: "teardown",
  });
  f.demo.run = async (args) => {
    if (args[0] === "ps") return { code: 0, stdout: "foreign" };
    if (args[0] === "container")
      return {
        code: 0,
        stdout: JSON.stringify([
          { Config: { Labels: { "io.lavik.admin.owner": "foreign" } } },
        ]),
      };
    throw new Error("Unexpected destructive command");
  };
  await f.fleet.deployments.runTeardown(job);
  const saved = await f.store.query(
    "SELECT * FROM jobs WHERE id=?",
    [job.id],
    "get",
  );
  assert.equal(saved.state, "uncertain");
  assert.match(saved.detail, /ownership/);
  assert.equal((await f.fleet.clusters()).length, 1);
});
