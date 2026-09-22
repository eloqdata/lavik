// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../store.mjs";
import { Fleet } from "../fleet.mjs";

const node = "b".repeat(40),
  owner = "a".repeat(40);
const status = () => ({
  cluster_state: "created",
  groups: [{ group_id: "g", owner_node_id: owner, topology_converged: true }],
  data_nodes: [
    {
      node_id: node,
      group_id: "g",
      population_current: true,
      projection_current: true,
      health_fresh: true,
    },
  ],
});

async function fixture(meta) {
  const directory = await mkdtemp(join(tmpdir(), "lavik-admin-workflow-"));
  const store = new Store(join(directory, "fleet.sqlite"));
  const fleet = new Fleet(store, meta);
  await store.query(
    "INSERT INTO clusters VALUES(?,?,?,?,?)",
    [
      "cluster",
      "cluster",
      '["127.0.0.1:7200"]',
      "default",
      new Date().toISOString(),
    ],
    "run",
  );
  return {
    store,
    fleet,
    async close() {
      await fleet.stop();
      while (fleet.busy.size) await new Promise((r) => setTimeout(r, 5));
      await store.close();
      await rm(directory, { recursive: true });
    },
  };
}

test("restart observes uncertain mutations without replaying them", async () => {
  const calls = [];
  const meta = {
    async status() {
      return { ...status(), data_nodes: [] };
    },
    async command(_cluster, args) {
      calls.push(args);
      return "OK submitted";
    },
  };
  const f = await fixture(meta);
  try {
    const stamp = new Date().toISOString();
    await f.store.query(
      "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
      [
        "1".repeat(32),
        "cluster",
        "replica-add",
        JSON.stringify({ node, group: "g", deadline: Date.now() + 10000 }),
        "running",
        "assigning",
        "",
        "1".repeat(32),
        stamp,
        stamp,
      ],
      "run",
    );
    await f.fleet.start();
    assert.equal(
      (await f.store.query("SELECT state FROM jobs", [], "get")).state,
      "uncertain",
    );
    assert.ok(calls.every((args) => args[0] === "getop"));
  } finally {
    await f.close();
  }
});

test("removal refuses a membership revision that changes after review", async () => {
  let reads = 0;
  const calls = [];
  const meta = {
    async status() {
      return status();
    },
    async leader() {
      return "127.0.0.1:7200";
    },
    async command(_cluster, args) {
      calls.push(args);
      if (args[0] === "getgroup")
        return `OK revision=${
          ++reads === 1 ? 7 : 8
        } owner=${owner} transition=0`;
      return "OK submitted";
    },
  };
  const f = await fixture(meta);
  try {
    const job = await f.fleet.enqueue(
      "cluster",
      "replica-remove",
      { group: "g", node },
      "2".repeat(32),
    );
    for (let i = 0; i < 100; i++) {
      if (
        (
          await f.store.query(
            "SELECT state FROM jobs WHERE id=?",
            [job.id],
            "get",
          )
        ).state === "uncertain"
      )
        break;
      await new Promise((r) => setTimeout(r, 5));
    }
    const saved = await f.store.query(
      "SELECT * FROM jobs WHERE id=?",
      [job.id],
      "get",
    );
    assert.equal(saved.state, "uncertain");
    assert.equal(JSON.parse(saved.input).expectedRevision, "7");
    assert.ok(!calls.some((args) => args[0] === "unassignnode"));
    await assert.rejects(
      f.fleet.enqueue("cluster", "replica-add", {
        group: "g",
        node: "c".repeat(40),
        endpoint: "tcp://127.0.0.1:6379",
      }),
      /Another operation/,
    );
    const retry = await f.fleet.enqueue(
      "cluster",
      "replica-remove",
      { group: "g", node },
      job.id,
    );
    assert.equal(retry.id, job.id);
    await assert.rejects(
      f.fleet.enqueue("cluster", "failover", { group: "g" }, job.id),
      /another request/,
    );
  } finally {
    await f.close();
  }
});

test("expired new requests fail without sending; expired retries remain uncertain", async () => {
  const calls = [];
  const f = await fixture({
    async leader() {
      calls.push("leader");
      throw new Error("unreachable");
    },
  });
  try {
    const stamp = new Date().toISOString();
    for (const [step, expected] of [
      ["preflight", "failed"],
      ["retry-requested", "uncertain"],
    ]) {
      await f.store.query("DELETE FROM jobs", [], "run");
      const job = {
        id: "3".repeat(32),
        cluster_id: "cluster",
        kind: "replica-add",
        input: JSON.stringify({ deadline: Date.now() - 1 }),
        state: "queued",
        step,
      };
      await f.store.query(
        "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
        [
          job.id,
          job.cluster_id,
          job.kind,
          job.input,
          job.state,
          job.step,
          "",
          job.id,
          stamp,
          stamp,
        ],
        "run",
      );
      await f.fleet.execute(job);
      assert.equal(
        (await f.store.query("SELECT state FROM jobs", [], "get")).state,
        expected,
      );
    }
    assert.deepEqual(calls, []);
  } finally {
    await f.close();
  }
});

test("a terminal replica operation is never assigned again on explicit retry", async () => {
  const calls = [];
  const f = await fixture({
    async leader() {
      return "127.0.0.1:7200";
    },
    async command(_cluster, args) {
      calls.push(args[0]);
      return args[0] === "getop"
        ? "OK aborted operator-abandoned"
        : "OK submitted";
    },
  });
  try {
    const stamp = new Date().toISOString();
    const job = {
      id: "4".repeat(32),
      cluster_id: "cluster",
      kind: "replica-add",
      input: JSON.stringify({ node, group: "g", deadline: Date.now() + 60000 }),
      state: "queued",
      step: "retry-requested",
    };
    await f.store.query(
      "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)",
      [
        job.id,
        job.cluster_id,
        job.kind,
        job.input,
        job.state,
        job.step,
        "",
        job.id,
        stamp,
        stamp,
      ],
      "run",
    );
    await f.fleet.execute(job);
    assert.equal(
      (await f.store.query("SELECT state FROM jobs", [], "get")).state,
      "failed",
    );
    assert.deepEqual(calls, ["submitop", "getop"]);
  } finally {
    await f.close();
  }
});
