// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import assert from "node:assert/strict";
import { once } from "node:events";

const bundle = resolve(process.argv[2]);
const directory = await mkdtemp(join(tmpdir(), "lavik-package-smoke-"));
const child = spawn(join(bundle, "lavik-admin"), [], {
  env: { ...process.env, LAVIK_ADMIN_DATA: directory, LAVIK_ADMIN_PORT: "0" },
  stdio: ["ignore", "pipe", "pipe"],
});
let output = "";
child.stdout.on("data", (chunk) => {
  output += chunk;
});
child.stderr.on("data", (chunk) => {
  output += chunk;
});
const exit = once(child, "exit");
try {
  const deadline = Date.now() + 15000;
  while (!/listening on port \d+/.test(output)) {
    if (child.exitCode !== null || Date.now() > deadline)
      throw new Error(`Packaged Admin did not start: ${output}`);
    await new Promise((r) => setTimeout(r, 100));
  }
  const base = `http://127.0.0.1:${/listening on port (\d+)/.exec(output)[1]}`;
  assert.equal((await fetch(base)).status, 200);
  assert.equal((await fetch(base + "/setup.js")).status, 200);
  assert.equal((await fetch(base + "/hosts.js")).status, 200);
  assert.equal((await fetch(base + "/placement.js")).status, 200);
  const login = await fetch(base + "/api/login", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Lavik-Admin": "1" },
    body: JSON.stringify({
      token: (await readFile(join(directory, "token"), "utf8")).trim(),
    }),
  });
  assert.equal(login.status, 200);
  const clusters = await fetch(base + "/api/clusters", {
    headers: { Cookie: login.headers.get("set-cookie").split(";")[0] },
  });
  assert.deepEqual(await clusters.json(), []);
  console.log(
    "Packaged Lavik Admin starts, serves setup assets, authenticates, and opens its catalog.",
  );
} finally {
  child.kill("SIGTERM");
  await exit;
  await rm(directory, { recursive: true, force: true });
}
