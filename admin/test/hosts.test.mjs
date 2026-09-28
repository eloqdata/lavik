// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../store.mjs";
import { Hosts } from "../hosts.mjs";
import { sshHost } from "../ssh.mjs";

test("host preparation is independent of placement; plans use stored SSH identities", async () => {
  assert.equal(
    sshHost(
      { host: "db.example.com", user: "ubuntu" },
      { requireAddress: false },
    ).address,
    "",
  );
  assert.throws(
    () => sshHost({ host: "db.example.com", user: "ubuntu" }),
    /numeric IP/,
  );
  const directory = await mkdtemp(join(tmpdir(), "lavik-host-catalog-"));
  const store = new Store(join(directory, "fleet.sqlite"));
  const hosts = new Hosts(store, directory);
  try {
    const stored = {
      id: "saved",
      host: "db.example.com",
      user: "ubuntu",
      port: 22,
      identityFile: "/private/managed-key",
      knownHostsFile: "/private/trust",
    };
    await hosts.save(stored);
    const resolved = await hosts.resolve({
      hostId: "saved",
      host: "attacker.invalid",
      identityFile: "/untrusted/key",
      password: "not-saved",
      address: "10.0.0.5",
    });
    assert.deepEqual(resolved, { ...stored, address: "10.0.0.5" });
    assert.equal((await hosts.list())[0].host, stored.host);
    await assert.rejects(hosts.resolve({ hostId: "missing" }), /not found/);
    await assert.rejects(hosts.verify("missing"), /not found/);
    await assert.rejects(
      hosts.prepare({
        host: "db.example.com",
        user: "ubuntu",
        method: "password",
      }),
      /password/,
    );
    await assert.rejects(
      hosts.prepare({
        host: "db.example.com",
        user: "ubuntu",
        method: "key",
        passphrase: "bad\nvalue",
      }),
      /credential/,
    );
    assert.equal((await hosts.list()).length, 1);
  } finally {
    await store.close();
    await rm(directory, { recursive: true, force: true });
  }
});
