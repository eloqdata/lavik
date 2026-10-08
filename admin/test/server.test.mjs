// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";
import { start } from "../server.mjs";
import { Meta } from "../meta.mjs";
import { DatabaseSync } from "node:sqlite";

test("HTTP and CLI share durable catalog, enforce login and reject cross-origin writes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lavik-admin-test-"));
  const meta = new Meta("/nonexistent");
  let app = await start({ directory, port: 0, meta });
  try {
    let base = `http://127.0.0.1:${app.server.address().port}`;
    assert.equal((await fetch(base + "/api/clusters")).status, 401);
    const token = (await readFile(app.tokenPath, "utf8")).trim();
    const login = await fetch(base + "/api/login", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Lavik-Admin": "1",
        Origin: base,
      },
      body: JSON.stringify({ token }),
    });
    assert.equal(login.status, 200);
    const cookie = login.headers.get("set-cookie").split(";")[0];
    assert.match(login.headers.get("set-cookie"), /HttpOnly; SameSite=Strict/);
    const headers = {
      "Content-Type": "application/json",
      "X-Lavik-Admin": "1",
      Cookie: cookie,
    };
    const rejected = await fetch(base + "/api/clusters", {
      method: "POST",
      headers: { ...headers, Origin: "https://attacker.invalid" },
      body: JSON.stringify({ id: "x", seeds: ["127.0.0.1:7200"] }),
    });
    assert.equal(rejected.status, 403);
    const cli = await new Promise((resolve, reject) => {
      const socket = net.connect(app.socketPath);
      let reply = "";
      socket.once("connect", () =>
        socket.write("fleet-add alpha 127.0.0.1:7200\n"),
      );
      socket.on("data", (bytes) => (reply += bytes));
      socket.on("end", () => resolve(reply));
      socket.on("error", reject);
    });
    assert.match(cli, /^OK /);
    const list = await (
      await fetch(base + "/api/clusters", { headers })
    ).json();
    assert.equal(list[0].id, "alpha");
    assert.equal(
      (
        await fetch(base + "/api/clusters", {
          method: "POST",
          headers,
          body: JSON.stringify({ id: "alpha", seeds: ["127.0.0.1:7201"] }),
        })
      ).status,
      409,
    );
    await app.close();
    app = await start({ directory, port: 0, meta });
    assert.equal((await app.fleet.clusters())[0].id, "alpha");
    assert.equal((await readFile(app.tokenPath, "utf8")).trim(), token);
    base = `http://127.0.0.1:${app.server.address().port}`;
    assert.equal(
      (await fetch(base + "/api/clusters", { headers })).status,
      401,
      "sessions do not survive process restart",
    );
  } finally {
    await app.close();
    await rm(directory, { recursive: true });
  }
});

test("an unsupported durable schema fails startup and releases its socket", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lavik-admin-schema-"));
  try {
    const database = new DatabaseSync(join(directory, "fleet.sqlite"));
    database.exec("PRAGMA user_version=99");
    database.close();
    await assert.rejects(
      start({ directory, port: 0, meta: new Meta("/nonexistent") }),
      /Unsupported fleet database version/,
    );
    const reset = new DatabaseSync(join(directory, "fleet.sqlite"));
    reset.exec("PRAGMA user_version=0");
    reset.close();
    const app = await start({
      directory,
      port: 0,
      meta: new Meta("/nonexistent"),
    });
    await app.close();
  } finally {
    await rm(directory, { recursive: true });
  }
});

test("CLI-reviewed setup deploys through HTTP into the same catalog and operation journal", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lavik-shared-setup-"));
  const app = await start({
    directory,
    port: 0,
    meta: new Meta("/nonexistent"),
    deploymentOptions: {
      ssh: {
        async call() {
          return { ok: true, errors: [], arch: "aarch64" };
        },
      },
      releases: {
        async resolve() {
          return {
            tag: "nightly",
            assets: { aarch64: { sha256: "a".repeat(64) } },
          };
        },
      },
    },
  });
  app.fleet.tick = async () => {};
  const cli = (command) =>
    new Promise((resolve, reject) => {
      const socket = net.connect(app.socketPath);
      let reply = "";
      socket.on("connect", () => socket.write(command + "\n"));
      socket.on("data", (chunk) => {
        reply += chunk;
      });
      socket.on("end", () => {
        try {
          assert.match(reply, /^OK /);
          resolve(JSON.parse(reply.slice(3)));
        } catch (error) {
          reject(error);
        }
      });
      socket.on("error", reject);
    });
  try {
    const base = `http://127.0.0.1:${app.server.address().port}`;
    const input = {
      id: "shared",
      hosts: [{ host: "127.0.0.1", user: "ubuntu" }],
    };
    assert.equal(
      (
        await fetch(base + "/api/setup/preview", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Lavik-Admin": "1" },
          body: JSON.stringify(input),
        })
      ).status,
      401,
    );
    const preview = await cli(
      "fleet-plan " + Buffer.from(JSON.stringify(input)).toString("base64url"),
    );
    const headers = {
      "Content-Type": "application/json",
      "X-Lavik-Admin": "1",
      Origin: base,
    };
    const login = await fetch(base + "/api/login", {
      method: "POST",
      headers,
      body: JSON.stringify({
        token: (await readFile(app.tokenPath, "utf8")).trim(),
      }),
    });
    headers.Cookie = login.headers.get("set-cookie").split(";")[0];
    await app.store.query(
      "INSERT INTO hosts VALUES(?,?,?)",
      [
        "host-one",
        JSON.stringify({
          id: "host-one",
          host: "example.com",
          user: "ubuntu",
          port: 22,
          identityFile: "/private/admin-key",
          knownHostsFile: "/private/admin-trust",
        }),
        new Date().toISOString(),
      ],
      "run",
    );
    assert.equal((await cli("fleet-hosts"))[0].id, "host-one");
    const fromInventory = await fetch(base + "/api/setup/preview", {
      method: "POST",
      headers,
      body: JSON.stringify({
        id: "from-inventory",
        hosts: [{ hostId: "host-one", address: "10.0.0.1" }],
      }),
    });
    const inventoryPreview = await fromInventory.json();
    assert.equal(fromInventory.status, 200, JSON.stringify(inventoryPreview));
    assert.equal(inventoryPreview.hosts[0].identityFile, "/private/admin-key");
    assert.equal(inventoryPreview.hosts[0].address, "10.0.0.1");
    const crossOriginPrepare = await fetch(base + "/api/hosts/prepare", {
      method: "POST",
      headers: { ...headers, Origin: "https://attacker.invalid" },
      body: "{}",
    });
    assert.equal(crossOriginPrepare.status, 403);
    const deployed = await fetch(base + "/api/setup/deploy", {
      method: "POST",
      headers,
      body: JSON.stringify({ token: preview.token, confirm: "shared" }),
    });
    assert.equal(deployed.status, 200);
    const job = await deployed.json();
    assert.equal((await cli("fleet-list"))[0].id, "shared");
    assert.equal((await cli("fleet-operations shared")).jobs[0].id, job.id);
    const retained = await (
      await fetch(base + "/api/clusters/shared/deployment", { headers })
    ).json();
    assert.equal(retained.release.assets.aarch64.sha256, "a".repeat(64));
  } finally {
    await app.close();
    await rm(directory, { recursive: true, force: true });
  }
});

for (const version of [1, 2, 3])
  test(`version-${version} catalogs retain clusters and history when removal archives are added`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "lavik-catalog-migration-"));
    let app;
    try {
      app = await start({ directory, port: 0, meta: new Meta("/nonexistent") });
      await app.fleet.add({ id: "retained", seeds: ["127.0.0.1:7200"] });
      await app.close();
      app = null;
      const old = new DatabaseSync(join(directory, "fleet.sqlite"));
      old.exec(
        `${
          version === 1 ? "DROP TABLE deployments;" : ""
        } DROP TABLE hosts; DROP TABLE cluster_archives; PRAGMA user_version=${version}`,
      );
      old
        .prepare("INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?)")
        .run(
          "saved-job",
          "retained",
          "replica-add",
          "{}",
          "completed",
          "completed",
          "history",
          null,
          "2026-01-01",
          "2026-01-01",
        );
      old.close();
      app = await start({ directory, port: 0, meta: new Meta("/nonexistent") });
      assert.equal((await app.fleet.clusters())[0].id, "retained");
      assert.equal(
        (
          await app.store.query(
            "SELECT detail FROM jobs WHERE id='saved-job'",
            [],
            "get",
          )
        ).detail,
        "history",
      );
      assert.deepEqual(await app.store.query("SELECT * FROM deployments"), []);
      assert.deepEqual(await app.store.query("SELECT * FROM hosts"), []);
      assert.equal(
        (await app.store.query("PRAGMA user_version", [], "get")).user_version,
        4,
      );
    } finally {
      if (app) await app.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
