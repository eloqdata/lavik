// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { nodeLayout, parseHostList } from "../public/placement.js";

test("bulk host entry keeps shared login settings and separate SSH/private cluster addresses", () => {
  const hosts = parseHostList(
    "127.0.0.1:2211 172.30.95.11\n127.0.0.1:2212 172.30.95.12\n127.0.0.1:2213 172.30.95.13",
    { user: "lavik", identityFile: "/tmp/lab/id_ed25519" },
  );
  assert.equal(hosts.length, 3);
  assert.equal(hosts[1].port, 2212);
  assert.equal(hosts[1].address, "172.30.95.12");
  assert.equal(hosts[1].user, "lavik");
  assert.equal(hosts[1].identityFile, "/tmp/lab/id_ed25519");
  assert.deepEqual(
    nodeLayout({ hosts }).map((n) => [n.name, n.host]),
    [
      ["meta-1", 0],
      ["meta-2", 1],
      ["meta-3", 2],
      ["data-1", 0],
      ["data-2", 1],
      ["data-3", 2],
    ],
  );
  assert.equal(parseHostList("[::1]:2222 ::2")[0].host, "::1");
  assert.throws(() => parseHostList("host:99999"), /port/);
  assert.throws(() => parseHostList("host unexpected extra"), /Line 1/);
});
