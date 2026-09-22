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
      headers: { "Content-Type": "application/json", "X-Lavik-Admin": "1" },
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
