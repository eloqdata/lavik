// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
// Real SSH, published binaries, HTTP, and the shared CLI socket; no cloud hosts.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, readFile, writeFile, rm, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { start } from "../server.mjs";
import { Releases } from "../releases.mjs";
import { command } from "../resp.mjs";

const exec = promisify(execFile);
const docker = async (...args) =>
  (
    await exec("docker", args, { timeout: 120000, maxBuffer: 8 * 1024 * 1024 })
  ).stdout.trim();
const prefix = `lavik-ssh-${randomBytes(4).toString("hex")}`;
const directory = await mkdtemp(join(tmpdir(), `${prefix}-`));
const workspace = join(directory, "workspace");
const privateKey = join(directory, "id_ed25519");
const knownHosts = join(directory, "known_hosts");
const password = "lavik-fixture-password";
const passphrase = "lavik-fixture-passphrase";
const encryptedKey = join(directory, "encrypted_ed25519");
const containers = [];
const publishedDataPorts = [];
let app, base, headers;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function wait(label, predicate, timeout = 180000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try {
      const result = await predicate();
      if (result) {
        console.log(`PASS: ${label}`);
        return result;
      }
    } catch (error) {
      last = error;
    }
    await sleep(1000);
  }
  throw new Error(`${label}: ${last?.message || "timed out"}`);
}
async function openAdmin() {
  app = await start({
    directory: workspace,
    port: 0,
    deploymentOptions: {
      releases: new Releases(
        fetch,
        process.env.LAVIK_ADMIN_TEST_RELEASE_CACHE ||
          join(workspace, "releases"),
      ),
    },
  });
  base = `http://127.0.0.1:${app.server.address().port}`;
  const token = (await readFile(app.tokenPath, "utf8")).trim();
  const response = await fetch(`${base}/api/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Lavik-Admin": "1" },
    body: JSON.stringify({ token }),
  });
  assert.equal(response.status, 200);
  headers = {
    "Content-Type": "application/json",
    "X-Lavik-Admin": "1",
    Cookie: response.headers.get("set-cookie").split(";")[0],
  };
}
async function api(path, method = "GET", value) {
  const response = await fetch(`${base}/api${path}`, {
    method,
    headers,
    body: value === undefined ? undefined : JSON.stringify(value),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(JSON.stringify(data));
  return data;
}
async function jobDone(cluster, id) {
  return wait(
    `${cluster} operation ${id}`,
    async () => {
      const jobs = await app.store.query("SELECT * FROM jobs WHERE id=?", [id]);
      if (jobs[0]?.state === "uncertain" || jobs[0]?.state === "failed")
        throw new Error(JSON.stringify(jobs[0]));
      return jobs[0]?.state === "completed";
    },
    300000,
  );
}
try {
  await exec("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-f", privateKey]);
  await writeFile(join(directory, "password"), password, { mode: 0o600 });
  await writeFile(encryptedKey, await readFile(privateKey), { mode: 0o600 });
  await exec("ssh-keygen", [
    "-p",
    "-P",
    "",
    "-N",
    passphrase,
    "-f",
    encryptedKey,
  ]);
  await docker("network", "create", "--subnet", "172.30.94.0/24", prefix);
  const hosts = [];
  for (let i = 0; i < 3; i++) {
    const name = `${prefix}-${i + 1}`;
    containers.push(name);
    await docker(
      "run",
      "-d",
      "--name",
      name,
      "--security-opt",
      "seccomp=unconfined",
      "--network",
      prefix,
      "--ip",
      `172.30.94.${11 + i}`,
      "-p",
      "127.0.0.1::22",
      "-p",
      "127.0.0.1::6379",
      "--mount",
      `type=bind,src=${privateKey}.pub,dst=/fixture-key.pub,readonly`,
      "--mount",
      `type=bind,src=${join(
        directory,
        "password",
      )},dst=/fixture-password,readonly`,
      "lavik-admin-ssh-test:local",
    );
    const info = JSON.parse(await docker("inspect", name))[0];
    const port = Number(info.NetworkSettings.Ports["22/tcp"][0].HostPort);
    publishedDataPorts.push(
      Number(info.NetworkSettings.Ports["6379/tcp"][0].HostPort),
    );
    const publicKey = await wait(`${name} SSH host key`, async () =>
      docker("exec", name, "cat", "/etc/ssh/ssh_host_ed25519_key.pub"),
    );
    await writeFile(knownHosts, `[127.0.0.1]:${port} ${publicKey}\n`, {
      flag: "a",
      mode: 0o600,
    });
    hosts.push({
      host: "127.0.0.1",
      address: `172.30.94.${11 + i}`,
      port,
      user: "lavik",
      identityFile: privateKey,
      knownHostsFile: knownHosts,
    });
  }
  await mkdir(workspace, { mode: 0o700 });
  await openAdmin();
  console.log(`Fixture ${directory}; browser ${base}`);
  await assert.rejects(
    api("/hosts/prepare", "POST", {
      ...hosts[0],
      method: "password",
      password: "incorrect-fixture-password",
    }),
    /authentication|Permission denied/,
  );
  assert.deepEqual(await api("/hosts"), []);
  const prepared = [];
  for (let i = 0; i < hosts.length; i++) {
    prepared.push(
      await api("/hosts/prepare", "POST", {
        ...hosts[i],
        ...(i === 0
          ? { method: "password", password }
          : {
              method: "key",
              identityFile: i === 1 ? encryptedKey : privateKey,
              ...(i === 1 ? { passphrase } : {}),
            }),
      }),
    );
  }
  const keysBefore = await docker(
    "exec",
    containers[0],
    "cat",
    "/home/lavik/.ssh/authorized_keys",
  );
  await api("/hosts/prepare", "POST", {
    ...hosts[0],
    method: "password",
    password,
  });
  assert.equal(
    await docker(
      "exec",
      containers[0],
      "cat",
      "/home/lavik/.ssh/authorized_keys",
    ),
    keysBefore,
    "key installation preserves existing keys and is idempotent",
  );
  assert.equal(
    keysBefore.split("\n").filter((line) => line.startsWith("ssh-ed25519 "))
      .length,
    2,
  );
  const savedTrust = await readFile(prepared[0].knownHostsFile, "utf8");
  // Replace only this fixture's pinned key to simulate a changed server identity.
  const wrongKey = (await readFile(privateKey + ".pub", "utf8")).trim();
  await writeFile(
    prepared[0].knownHostsFile,
    `[127.0.0.1]:${hosts[0].port} ${wrongKey}\n`,
  );
  await assert.rejects(
    api(`/hosts/${prepared[0].id}/verify`, "POST", {}),
    /HOST IDENTIFICATION HAS CHANGED/,
  );
  await assert.rejects(
    api("/hosts/prepare", "POST", {
      ...hosts[0],
      method: "password",
      password,
    }),
    /HOST IDENTIFICATION HAS CHANGED/,
  );
  await writeFile(prepared[0].knownHostsFile, savedTrust);
  await api(`/hosts/${prepared[0].id}/verify`, "POST", {});
  const catalog = JSON.stringify(await app.store.query("SELECT * FROM hosts"));
  for (const secret of [password, passphrase, "incorrect-fixture-password"]) {
    assert.equal(catalog.includes(secret), false);
    assert.equal(
      (await readFile(join(workspace, "fleet.sqlite"))).includes(
        Buffer.from(secret),
      ),
      false,
    );
  }
  assert.equal(
    catalog.includes(encryptedKey),
    false,
    "bootstrap key is not retained",
  );
  hosts.splice(
    0,
    hosts.length,
    ...prepared.map((h) => ({ hostId: h.id, address: h.address, ...h })),
  );
  console.log(
    "PASS: password, encrypted and plain keys, fresh key-only login, idempotence, changed-host rejection, and credential-free catalog",
  );
  for (const config of [
    {
      id: "alpha",
      release: "nightly",
      metaCount: 3,
      followers: 2,
      dataPort: 16379,
      metaPort: 17100,
      ctlPort: 17200,
      controlPort: 17300,
    },
    {
      id: "beta",
      release: "v0.1.0-beta.1",
      metaCount: 1,
      followers: 1,
      dataPort: 18379,
      metaPort: 18100,
      ctlPort: 18200,
      controlPort: 18400,
    },
  ]) {
    const preview = await api("/setup/preview", "POST", {
      ...config,
      hosts,
      threads: 1,
      dataGiB: 1,
      supervisor: "process",
    });
    assert.equal(preview.ready, true, JSON.stringify(preview.checks));
    const job = await api("/setup/deploy", "POST", {
      token: preview.token,
      confirm: config.id,
    });
    await jobDone(config.id, job.id);
    const view = await api(`/clusters/${config.id}`);
    assert.equal(view.status.cluster_ready, true);
    assert.equal(
      (
        await api(`/clusters/${config.id}/command`, "POST", {
          args: ["SET", "ssh:proof", config.id],
          confirm: true,
        })
      ).reply,
      "OK",
    );
    assert.equal(
      (
        await api(`/clusters/${config.id}/command`, "POST", {
          args: ["GET", "ssh:proof"],
        })
      ).reply,
      config.id,
    );
    const metrics = await api(`/clusters/${config.id}/metrics`);
    assert.ok(
      metrics.nodes.every((n) => !n.error),
      JSON.stringify(metrics),
    );
    console.log(
      `PASS: ${config.id} deployment, isolated data, and observability over SSH`,
    );
  }
  const cli = await exec(
    process.execPath,
    [resolve("admin/cli.mjs"), "fleet-list"],
    { env: { ...process.env, LAVIK_ADMIN_DATA: workspace } },
  );
  assert.deepEqual(
    JSON.parse(cli.stdout.slice(3)).map((c) => c.id),
    ["alpha", "beta"],
  );
  const preview = await api("/clusters/alpha/follower-preview", "POST", {
    group: "group-1",
    host: hosts[0],
    dataPort: 16390,
  });
  assert.equal(preview.ready, true, JSON.stringify(preview.checks));
  const addition = await api("/clusters/alpha/follower-deploy", "POST", {
    token: preview.token,
    confirm: "alpha",
  });
  await jobDone("alpha", addition.id);
  const view = await api("/clusters/alpha?fresh=1");
  const added = view.nodes.find((n) => n.node_id === preview.nodes[0].id);
  assert.ok(added.population_current && added.health_fresh);
  const removal = await api("/clusters/alpha/operations", "POST", {
    kind: "replica-remove",
    input: { group: "group-1", node: added.node_id },
  });
  await jobDone("alpha", removal.id);
  const failover = await api("/clusters/alpha/operations", "POST", {
    kind: "failover",
    input: { group: "group-1" },
  });
  await jobDone("alpha", failover.id);
  await app.close();
  app = null;
  await openAdmin();
  assert.equal((await api("/clusters")).length, 2);
  assert.equal(
    (await api("/hosts")).length,
    3,
    "prepared host inventory survives restart",
  );
  await api(`/hosts/${prepared[0].id}/verify`, "POST", {});
  assert.equal(
    (
      await api("/clusters/alpha/command", "POST", {
        args: ["GET", "ssh:proof"],
      })
    ).reply,
    "alpha",
  );
  assert.equal(
    (
      await api("/clusters/beta/command", "POST", {
        args: ["GET", "ssh:proof"],
      })
    ).reply,
    "beta",
  );
  console.log(
    "PASS: new follower, removal, failover, shared CLI catalog, and Admin restart",
  );
  if (process.env.LAVIK_ADMIN_TEST_BROWSER === "1") {
    const { chromium } = await import("@playwright/test");
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage({
        viewport: { width: 1440, height: 1000 },
      });
      page.on("pageerror", (error) =>
        console.error("Browser error:", error.message),
      );
      page.on("console", (message) => {
        if (message.type() === "error")
          console.error("Browser console:", message.text());
      });
      page.on("response", async (response) => {
        if (response.status() >= 400 && response.url().includes("/api/"))
          console.error(
            "Browser API failure:",
            response.url(),
            await response.text().catch(() => ""),
          );
      });
      await page.goto(base);
      await page
        .getByLabel("Access token", { exact: true })
        .fill((await readFile(app.tokenPath, "utf8")).trim());
      await page.getByRole("button", { name: "Open workspace" }).click();
      await page
        .getByRole("button", { name: /Create cluster/ })
        .click({ timeout: 60000 })
        .catch(async (error) => {
          await page.screenshot({
            path: "/private/tmp/lavik-admin-browser-failure.png",
            fullPage: true,
          });
          console.error(await page.locator("body").innerText());
          throw error;
        });
      assert.equal(
        await page.getByLabel("Cluster name", { exact: true }).count(),
        0,
        "placement starts only after host preparation",
      );
      await page
        .getByLabel("Host list", { exact: true })
        .fill(hosts.map((h) => `${h.host}:${h.port} ${h.address}`).join("\n"));
      await page.getByLabel("SSH user", { exact: true }).fill("lavik");
      await page.getByLabel("SSH password", { exact: true }).fill(password);
      await page
        .getByRole("button", { name: "Prepare hosts", exact: true })
        .click();
      await page
        .getByRole("button", {
          name: "Continue to node placement",
          exact: true,
        })
        .waitFor();
      await wait(
        "browser password preparation",
        async () =>
          (await page
            .locator("#host-progress")
            .getByText(/Admin SSH access verified/)
            .count()) === 3,
      );
      assert.equal(
        await page.getByLabel("SSH password", { exact: true }).inputValue(),
        "",
      );
      await page
        .getByRole("button", {
          name: "Continue to node placement",
          exact: true,
        })
        .click();
      await page
        .getByLabel("Cluster name", { exact: true })
        .fill("browser-check");
      assert.equal(await page.locator("[data-address]").count(), 3);
      assert.equal(
        await page.getByLabel("Host for data-1", { exact: true }).inputValue(),
        "0",
      );
      await page
        .getByLabel("Host for data-1", { exact: true })
        .selectOption("1");
      await page
        .getByLabel("Host for data-2", { exact: true })
        .selectOption("0");
      assert.equal(
        await page.getByLabel("Service lifecycle").isVisible(),
        true,
      );
      assert.equal(
        await page.getByLabel("Service lifecycle").inputValue(),
        "systemd",
      );
      await page.getByText("Tune advanced settings", { exact: true }).click();
      await page.getByLabel("Storage GiB per Data node").fill("1");
      await page.getByLabel("Workers per Data node").fill("1");
      await page
        .getByRole("button", { name: "Check hosts & review", exact: true })
        .click();
      await page
        .getByRole("button", {
          name: "Use development processes & recheck",
          exact: true,
        })
        .waitFor({ timeout: 90000 });
      assert.equal(
        await page.getByLabel("Service lifecycle").inputValue(),
        "systemd",
        "host checks never silently change the requested lifecycle",
      );
      await page
        .getByRole("button", {
          name: "Use development processes & recheck",
          exact: true,
        })
        .click();
      assert.equal(
        await page.getByLabel("Service lifecycle").inputValue(),
        "process",
      );
      await wait(
        "browser preview",
        async () => {
          const error = await page.locator("#setup-error").textContent();
          if (error) throw new Error(error);
          return await page
            .getByRole("heading", { name: "Ready to deploy", exact: true })
            .isVisible();
        },
        90000,
      );
      await page.screenshot({
        path: "/private/tmp/lavik-admin-setup-review.png",
        fullPage: true,
      });
      await page.setViewportSize({ width: 390, height: 844 });
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
      );
      await page.setViewportSize({ width: 1440, height: 1000 });
      await page
        .getByLabel("Type browser-check to confirm")
        .fill("browser-check");
      await page
        .getByRole("button", { name: "Deploy cluster", exact: true })
        .click();
      await page
        .getByRole("heading", { name: "Operations", exact: true })
        .waitFor();
      await wait(
        "browser deployment becomes ready",
        async () => (await api("/clusters/browser-check")).status.cluster_ready,
      );
      const installed = await api("/clusters/browser-check/deployment");
      assert.equal(installed.supervisor, "process");
      assert.equal(installed.nodes.find((n) => n.name === "data-1").host, 1);
      assert.equal(installed.nodes.find((n) => n.name === "data-2").host, 0);
      assert.deepEqual(
        installed.nodes.filter((n) => n.kind === "data").map((n) => n.port),
        [6379, 6379, 6379],
      );
      for (const port of publishedDataPorts)
        assert.equal(await command(`127.0.0.1:${port}`, ["PING"]), "PONG");
      const primary = installed.nodes.find((n) => n.role === "primary");
      // Prepared-host display order need not match container creation order.
      const primaryFixture = hosts.findIndex(
        (host) => host.address === installed.hosts[primary.host].address,
      );
      assert.ok(primaryFixture >= 0);
      const primaryPort = publishedDataPorts[primaryFixture];
      assert.equal(
        await command(`127.0.0.1:${primaryPort}`, [
          "SET",
          "published-port:proof",
          "from-mac",
        ]),
        "OK",
      );
      assert.equal(
        (
          await command(`127.0.0.1:${primaryPort}`, [
            "GET",
            "published-port:proof",
          ])
        ).toString(),
        "from-mac",
      );
      console.log(
        "PASS: every container uses Data port 6379; Mac can reach published ports and read/write through the explicitly placed primary",
      );
      console.log(
        "PASS: browser password onboarding, explicit placement, host checks, deployment, and mobile layout",
      );
    } finally {
      await browser.close();
    }
  }
  console.log("All SSH deployment smoke checks passed.");
} catch (error) {
  for (const name of containers) {
    try {
      console.error(
        await docker(
          "exec",
          name,
          "python3",
          "-c",
          "from pathlib import Path; [(print(str(p)), print(p.read_text(errors='replace')[-2500:])) for p in Path('/home/lavik').rglob('console.log')]",
        ),
      );
    } catch {
      /* Keep the original failure. */
    }
  }
  throw error;
} finally {
  if (app) await app.close();
  if (process.env.LAVIK_ADMIN_KEEP_TEST !== "1") {
    for (const name of containers)
      await docker("rm", "-f", name).catch(() => {});
    await docker("network", "rm", prefix).catch(() => {});
    await rm(directory, { recursive: true, force: true });
  } else console.log(`Retained isolated fixture ${prefix} in ${directory}`);
}
