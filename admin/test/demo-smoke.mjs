// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
// Manual integration: node admin/test/demo-smoke.mjs /path/to/extracted/package
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import assert from "node:assert/strict";
import { chromium } from "@playwright/test";
import { dockerCommand } from "../demo.mjs";

const bundle = resolve(process.argv[2]);
const directory = await mkdtemp(join(tmpdir(), "lavik-demo-package-smoke-"));
const child = spawn(join(bundle, "lavik-admin"), [], {
  env: { ...process.env, LAVIK_ADMIN_DATA: directory, LAVIK_ADMIN_PORT: "0" },
  stdio: ["ignore", "pipe", "pipe"],
});
let output = "",
  browser,
  plan,
  base;
child.stdout.on("data", (chunk) => (output += chunk));
child.stderr.on("data", (chunk) => (output += chunk));
const exited = once(child, "exit");
try {
  const deadline = Date.now() + 15000;
  while (!/listening on port \d+/.test(output)) {
    if (child.exitCode !== null || Date.now() > deadline)
      throw new Error("Packaged Admin did not start");
    await delay(100);
  }
  base = `http://127.0.0.1:${/listening on port (\d+)/.exec(output)[1]}`;
  browser = await chromium.launch();
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
  });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(base);
  await page
    .getByLabel("Access token")
    .fill((await readFile(join(directory, "token"), "utf8")).trim());
  await page.getByRole("button", { name: /Open workspace/ }).click();
  await page.getByRole("button", { name: /Create cluster/ }).click();
  await page
    .getByRole("button", { name: "Try a demo cluster", exact: true })
    .click();
  let previous = "";
  for (let i = 0; i < 900; i++) {
    const status = await (await page.request.get(base + "/api/demo")).json();
    if (status.step && status.step !== previous)
      console.log(`Demo step: ${status.step}`);
    previous = status.step;
    if (status.id) {
      const response = await page.request.get(
        base + "/api/clusters/demo-cluster/deployment",
      );
      if (response.ok()) plan = await response.json();
    }
    if (["failed", "uncertain"].includes(status.state))
      throw new Error(status.detail + "\n" + status.log.slice(-2000));
    if (status.state === "completed") break;
    if (i === 899) throw new Error("Demo readiness timed out");
    await delay(1000);
  }
  await page
    .getByRole("button", { name: /Open demo dashboard/ })
    .waitFor({ timeout: 15000 });
  const containers = await dockerCommand([
    "ps",
    "--filter",
    `label=com.docker.compose.project=${plan.project}`,
    "--format",
    '{{.Label "com.docker.compose.service"}}',
  ]);
  const names = containers.stdout.split("\n").sort();
  assert.deepEqual(names, [
    "data-1",
    "data-2",
    "data-3",
    "meta-1",
    "meta-2",
    "meta-3",
  ]);
  await page.getByRole("button", { name: /Open demo dashboard/ }).click();
  await page
    .getByRole("button", { name: /Manage topology/ })
    .waitFor({ timeout: 30000 });
  const headers = { "Content-Type": "application/json", "X-Lavik-Admin": "1" };
  const send = async (args) => {
    const response = await page.request.post(
      base + "/api/clusters/demo-cluster/command",
      { headers, data: { args, confirm: true } },
    );
    const value = await response.json();
    assert.equal(response.status(), 200, JSON.stringify(value));
    return value;
  };
  await send(["SET", "demo:package:smoke", "retained"]);
  assert.match(
    JSON.stringify(await send(["GET", "demo:package:smoke"])),
    /retained/,
  );
  const metrics = await (
    await page.request.get(base + "/api/clusters/demo-cluster/metrics")
  ).json();
  assert.equal(metrics.nodes.length, 3);
  assert.ok(
    metrics.nodes.every((n) => !n.error),
    JSON.stringify(metrics),
  );
  // Explicit rerun must reuse the same project, release and population.
  await page.request.post(base + "/api/demo", { headers, data: {} });
  for (let i = 0; i < 120; i++) {
    const status = await (await page.request.get(base + "/api/demo")).json();
    assert.ok(!["failed", "uncertain"].includes(status.state), status.detail);
    if (status.state === "completed") break;
    if (i === 119) throw new Error("Demo restart timed out");
    await delay(1000);
  }
  assert.match(
    JSON.stringify(await send(["GET", "demo:package:smoke"])),
    /retained/,
  );
  await page.locator("#remove-selected").click();
  await page.getByLabel("Removal type").selectOption("teardown");
  await page.getByLabel("Type demo-cluster to confirm").fill("demo-cluster");
  await page.getByLabel(/I understand that this permanently deletes/).check();
  await page
    .getByRole("button", { name: "Permanently tear down", exact: true })
    .click();
  await page
    .getByRole("heading", { name: "Your workspace is ready" })
    .waitFor({ timeout: 120000 });
  assert.equal(
    (await fetch(base)).status,
    200,
    "existing Admin remains running after teardown",
  );
  const remaining = await dockerCommand([
    "ps",
    "-aq",
    "--filter",
    `label=com.docker.compose.project=${plan.project}`,
  ]);
  assert.equal(remaining.stdout, "");
  assert.deepEqual(errors, []);
  console.log(
    "Extracted package: button created six nodes, dashboard/RESP worked, restart retained data, and teardown kept Admin running.",
  );
} finally {
  await browser?.close();
  child.kill("SIGTERM");
  await exited;
  // This test owns a fresh random project. Clean partial test resources too.
  if (plan) {
    await dockerCommand(
      [
        "compose",
        "-p",
        plan.project,
        "--project-directory",
        plan.directory,
        "-f",
        join(plan.directory, "compose.json"),
        "--profile",
        "tools",
        "down",
        "--volumes",
        "--remove-orphans",
      ],
      { timeout: 180000 },
    );
    const network = await dockerCommand([
      "network",
      "inspect",
      `${plan.project}-cluster`,
    ]);
    if (
      network.code === 0 &&
      JSON.parse(network.stdout)[0].Labels?.["io.lavik.admin.owner"] ===
        plan.owner
    )
      await dockerCommand(["network", "rm", `${plan.project}-cluster`]);
    await dockerCommand(["image", "rm", `${plan.project}:runtime`]);
  }
  await rm(directory, { recursive: true, force: true });
}
