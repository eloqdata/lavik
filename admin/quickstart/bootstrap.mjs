// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { createHash } from "node:crypto";

const id = "demo-cluster";
const manifest = await readFile(
  new URL("cluster.toml", import.meta.url),
  "utf8",
);
const seeds = [
  ...manifest.matchAll(/^ctl_endpoint\s*=\s*"tcp:\/\/([^"\n]+)"/gm),
].map((match) => match[1]);
if (seeds.length !== 3)
  throw new Error("Demo manifest must define three Meta Admin endpoints");
const headers = { "Content-Type": "application/json", "X-Lavik-Admin": "1" };
async function api(path, input) {
  const response = await fetch(`http://127.0.0.1:4173/api${path}`, {
    method: input ? "POST" : "GET",
    headers,
    body: input ? JSON.stringify(input) : undefined,
    signal: AbortSignal.timeout(30000),
  });
  const value = await response.json();
  if (!response.ok) throw new Error(value.error || `HTTP ${response.status}`);
  if (path === "/login")
    headers.Cookie = response.headers.get("set-cookie").split(";")[0];
  return value;
}
const deadline = Date.now() + 300000;
let loggedIn = false;
while (!loggedIn && Date.now() < deadline) {
  try {
    const token = (await readFile("/data/lavik-admin/token", "utf8")).trim();
    await api("/login", { token });
    loggedIn = true;
  } catch {
    await delay(1000);
  }
}
if (!loggedIn)
  throw new Error("Admin did not start. Inspect docker compose logs admin.");
const existing = (await api("/clusters")).find((c) => c.id === id);
if (
  existing &&
  (existing.profile !== "default" ||
    JSON.stringify(JSON.parse(existing.seeds)) !== JSON.stringify(seeds))
)
  throw new Error(
    "demo-cluster already refers to another deployment; no changes made.",
  );
if (!existing) await api("/clusters", { id, seeds });
// Stable identity closes the crash window between submitting creation and seeing
// its reply. Restart only observes an existing job, including uncertain outcomes.
const requestId = createHash("sha256")
  .update(id + manifest)
  .digest("hex")
  .slice(0, 32);
let lastError = "Waiting for Meta and Data to start";
while (Date.now() < deadline) {
  let view;
  try {
    view = await api(`/clusters/${id}?fresh=1`);
  } catch (error) {
    lastError = error.message;
    await delay(2000);
    continue;
  }
  if (view.status.cluster_ready) {
    console.log(
      "demo-cluster is ready: one primary, two followers, and three Meta voters.",
    );
    process.exit(0);
  }
  const { jobs } = await api(`/clusters/${id}/operations`);
  const job = jobs.find((j) => j.kind === "create");
  if (job && ["failed", "uncertain"].includes(job.state))
    throw new Error(
      `Creation ${job.state}: ${job.detail}. Inspect Operations in Admin; setup will not replay an uncertain creation.`,
    );
  if (view.status.cluster_state === "uninitialized" && !job) {
    console.log("Initializing demo-cluster…");
    await api(`/clusters/${id}/operations`, {
      kind: "create",
      input: { manifest },
      requestId,
    });
  }
  lastError = job?.detail || view.status.cluster_state;
  await delay(2000);
}
throw new Error(
  `Demo did not become ready: ${lastError}. Inspect Admin Operations and Compose logs; retain the volumes when retrying.`,
);
