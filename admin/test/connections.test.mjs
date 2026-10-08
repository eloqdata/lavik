// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../store.mjs";
import { Fleet } from "../fleet.mjs";

test("connection preview is read-only, retains tested inputs, and cannot replay an expired preview", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lavik-connect-test-"));
  const store = new Store(join(directory, "fleet.sqlite"));
  const calls = [];
  const meta = {
    profile: () => ({}),
    async status(connection) {
      calls.push(connection);
      return { cluster_state: "created", cluster_ready: true, groups: [{}] };
    },
  };
  const fleet = new Fleet(store, meta);
  try {
    const preview = await fleet.previewConnection({
      id: "production",
      seeds: ["10.0.0.1:7200"],
    });
    assert.deepEqual(await fleet.clusters(), []);
    assert.equal(preview.status.cluster_ready, true);
    assert.match(calls[0].id, /^preview-/);
    const cluster = await fleet.connect({
      token: preview.token,
      seeds: ["127.0.0.1:1"],
      id: "changed",
    });
    assert.equal(cluster.id, "production");
    assert.equal(cluster.seeds, '["10.0.0.1:7200"]');
    await assert.rejects(fleet.connect({ token: preview.token }), /expired/);
    const next = await fleet.previewConnection({
      id: "other",
      seeds: ["10.0.0.2:7200"],
    });
    fleet.connections.get(next.token).expires = 0;
    await assert.rejects(fleet.connect({ token: next.token }), /expired/);
    meta.status = async () => {
      throw new Error("No Meta seed is reachable");
    };
    await assert.rejects(
      fleet.previewConnection({ id: "offline", seeds: ["10.0.0.3:7200"] }),
      /reachable/,
    );
    assert.equal((await fleet.clusters()).length, 1);
  } finally {
    await store.close();
    await rm(directory, { recursive: true });
  }
});
