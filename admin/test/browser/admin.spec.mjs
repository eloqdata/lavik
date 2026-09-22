// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { test, expect } from "@playwright/test";
import { execFileSync } from "node:child_process";

const container = process.env.LAVIK_ADMIN_TEST_CONTAINER || "lavik-admin-dev";
const directory = process.env.LAVIK_ADMIN_TEST_DATA || "/data/admin-workspace";
const docker = (...args) =>
  execFileSync("docker", ["exec", container, ...args], {
    encoding: "utf8",
    timeout: 20000,
  }).trim();
const ctl = (...args) =>
  docker("/build/lavik-ctl", "--socket", `${directory}/admin.sock`, ...args);
const token = () => docker("cat", `${directory}/token`);
const spare = "0000000000000000000000000000000000001904";

test.describe.configure({ mode: "serial" });
test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Access token", { exact: true }).fill(token());
  await page.getByRole("button", { name: "Open workspace" }).click();
  await expect(
    page.getByRole("heading", { name: "Your clusters" }),
  ).toBeVisible();
});

test("fleet created by CLI, familiar data tools, and escaped key values", async ({
  page,
}, testInfo) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await expect(
    page.getByRole("button", { name: "alpha", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "beta", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("fleet.png"),
    fullPage: true,
  });
  await page.getByRole("button", { name: "alpha", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "alpha", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Send command", exact: true }).click();
  await page
    .getByLabel("Command", { exact: true })
    .fill('SET "admin:e2e:key" "hello world"');
  await page.getByRole("button", { name: "Run command" }).click();
  await page
    .getByRole("button", { name: "Execute write", exact: true })
    .click();
  await expect(page.locator("#command-output")).toContainText('"OK"');
  expect(docker("redis-cli", "-c", "-p", "6400", "GET", "admin:e2e:key")).toBe(
    "hello world",
  );
  await page.getByRole("button", { name: "Key browser", exact: true }).click();
  await page.getByLabel("Key pattern").fill("admin:e2e:*");
  await page.getByRole("button", { name: "Search keys", exact: true }).click();
  await expect(page.locator("#key-count")).toContainText("found");
  while (
    await page
      .getByRole("button", { name: "Load more", exact: true })
      .isVisible()
  ) {
    await page.getByRole("button", { name: "Load more", exact: true }).click();
    await page.waitForTimeout(200);
  }
  await page
    .getByRole("button", { name: "admin:e2e:key", exact: true })
    .click();
  await expect(page.locator("#key-detail")).toContainText("hello world");
  await page.getByRole("button", { name: "Edit value", exact: true }).click();
  await page
    .getByLabel("Value", { exact: true })
    .fill('<img src=x onerror="window.xss=true">');
  await page.getByRole("button", { name: "Save string", exact: true }).click();
  expect(
    docker("redis-cli", "-c", "-p", "6400", "GET", "admin:e2e:key"),
  ).toContain("<img");
  await page
    .getByRole("button", { name: "admin:e2e:key", exact: true })
    .click();
  await expect(page.locator("#key-detail")).toContainText("<img");
  expect(await page.evaluate(() => window.xss)).toBeUndefined();
  await page.getByRole("button", { name: "Topology", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "group-2", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("topology.png"),
    fullPage: true,
  });
  await page.getByRole("button", { name: "Activity", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Slow commands", exact: true }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});

test("browser expansion, CLI shrink, shared operation journal, and primary protection", async ({
  page,
}, testInfo) => {
  const initial = JSON.parse(ctl("fleet-status", "alpha").slice(3));
  if (initial.nodes.find((n) => n.node_id === spare)?.group_id) {
    const cleanup = JSON.parse(
      ctl("fleet-replica-remove", "alpha", "group-1", spare).slice(3),
    );
    await expect
      .poll(
        () =>
          JSON.parse(ctl("fleet-operations", "alpha").slice(3)).jobs.find(
            (j) => j.id === cleanup.id,
          )?.state,
        { timeout: 60000 },
      )
      .toBe("completed");
  }
  await page.getByRole("button", { name: "alpha", exact: true }).click();
  await page.getByRole("button", { name: "Topology", exact: true }).click();
  const group = page.locator(".group-card").filter({
    has: page.getByRole("heading", { name: "group-1", exact: true }),
  });
  await group.getByRole("button", { name: "Add replica" }).click();
  await page.getByLabel("Node ID", { exact: true }).fill(spare);
  await page.getByLabel("Client endpoint").fill("tcp://127.0.0.1:6404");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Add replica", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Operations", exact: true }),
  ).toBeVisible();
  await expect
    .poll(
      async () =>
        JSON.parse(ctl("fleet-operations", "alpha").slice(3)).jobs.find(
          (j) => j.kind === "replica-add",
        )?.state,
      { timeout: 90000 },
    )
    .toBe("completed");
  const operations = JSON.parse(ctl("fleet-operations", "alpha").slice(3));
  const added = operations.jobs.find((j) => j.kind === "replica-add");
  expect(
    operations.meta.some(
      (op) => op.id === added.id && op.state === "completed",
    ),
  ).toBe(true);
  const view = JSON.parse(ctl("fleet-status", "alpha").slice(3));
  expect(view.nodes.find((n) => n.node_id === spare)?.population_current).toBe(
    true,
  );
  const owner = view.status.groups.find(
    (g) => g.group_id === "group-1",
  ).owner_node_id;
  const rejected = await page.request.post("/api/clusters/alpha/operations", {
    headers: { "X-Lavik-Admin": "1" },
    data: { kind: "replica-remove", input: { group: "group-1", node: owner } },
  });
  expect(rejected.status()).toBe(409);
  const removed = JSON.parse(
    ctl("fleet-replica-remove", "alpha", "group-1", spare).slice(3),
  );
  await expect
    .poll(
      async () =>
        JSON.parse(ctl("fleet-operations", "alpha").slice(3)).jobs.find(
          (j) => j.id === removed.id,
        )?.state,
      { timeout: 60000 },
    )
    .toBe("completed");
  await page.getByRole("button", { name: "Refresh", exact: false }).click();
  await expect(
    page.locator("td").filter({ hasText: removed.id }),
  ).toBeVisible();
  expect(
    docker("redis-cli", "-c", "-p", "6400", "GET", "admin:e2e:key"),
  ).toContain("<img");
  await page.screenshot({
    path: testInfo.outputPath("operations.png"),
    fullPage: true,
  });
});

test("native CLI failover appears in Admin and preserves data", async ({
  page,
}) => {
  const before = JSON.parse(ctl("fleet-status", "alpha").slice(3));
  const previous = before.status.groups.find(
    (g) => g.group_id === "group-1",
  ).owner_node_id;
  const result = docker(
    "/build/lavik-ctl",
    "failover",
    "group-1",
    "--addr",
    "127.0.0.1:8200",
    "--allow-plaintext-admin",
  );
  const operation = /operation=([0-9a-f]+)/.exec(result)[1];
  await page.getByRole("button", { name: "alpha", exact: true }).click();
  await page.getByRole("button", { name: "Operations", exact: true }).click();
  await expect(page.locator("td").filter({ hasText: operation })).toBeVisible();
  await expect
    .poll(
      () =>
        JSON.parse(ctl("fleet-operations", "alpha").slice(3)).meta.find(
          (op) => op.id === operation,
        )?.state,
      { timeout: 90000 },
    )
    .toBe("completed");
  const after = JSON.parse(ctl("fleet-status", "alpha").slice(3));
  expect(
    after.status.groups.find((g) => g.group_id === "group-1").owner_node_id,
  ).not.toBe(previous);
  // Meta commits the new authority before its population becomes readable.
  // Check the serving result after cutover, not merely the operation receipt.
  await expect
    .poll(
      () => docker("redis-cli", "-c", "-p", "6400", "GET", "admin:e2e:key"),
      { timeout: 60000 },
    )
    .toContain("<img");
});

test("browser controlled failover uses a shared durable request", async ({
  page,
}) => {
  await expect
    .poll(
      () =>
        JSON.parse(ctl("fleet-status", "alpha").slice(3)).status.cluster_ready,
      { timeout: 60000 },
    )
    .toBe(true);
  await page.getByRole("button", { name: "alpha", exact: true }).click();
  await page.getByRole("button", { name: "Topology", exact: true }).click();
  const group = page.locator(".group-card").filter({
    has: page.getByRole("heading", { name: "group-1", exact: true }),
  });
  await group
    .getByRole("button", { name: "Switch primary", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Switch primary", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Operations", exact: true }),
  ).toBeVisible();
  await expect
    .poll(
      () =>
        JSON.parse(ctl("fleet-operations", "alpha").slice(3)).jobs.find(
          (j) => j.kind === "failover",
        )?.state,
      { timeout: 90000 },
    )
    .toBe("completed");
  const operations = JSON.parse(ctl("fleet-operations", "alpha").slice(3));
  const job = operations.jobs.find((j) => j.kind === "failover");
  expect(operations.meta.find((op) => op.id === job.id)?.state).toBe(
    "completed",
  );
  await expect
    .poll(
      () => docker("redis-cli", "-c", "-p", "6400", "GET", "admin:e2e:key"),
      { timeout: 60000 },
    )
    .toContain("<img");
});

test("small-screen navigation remains usable for the smaller cluster", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "beta", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "beta", exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: testInfo.outputPath("mobile.png"),
    fullPage: true,
  });
});

test("connect and initialize an already-running fresh deployment", async ({
  page,
}) => {
  test.skip(
    !process.env.LAVIK_ADMIN_CREATE_MANIFEST,
    "Requires the optional fresh gamma fixture",
  );
  await page
    .getByRole("button", { name: "Connect cluster", exact: false })
    .first()
    .click();
  await page.getByLabel("Cluster name", { exact: true }).fill("gamma");
  await page
    .getByLabel("Meta seed addresses", { exact: true })
    .fill("127.0.0.1:9000");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Connect cluster", exact: true })
    .click();
  expect(
    JSON.parse(ctl("fleet-list").slice(3)).some((c) => c.id === "gamma"),
  ).toBe(true);
  await page.getByRole("button", { name: "Topology", exact: true }).click();
  await page
    .getByRole("button", { name: "Initialize cluster", exact: true })
    .click();
  await page
    .getByLabel("Cluster manifest (TOML)", { exact: true })
    .fill(docker("cat", process.env.LAVIK_ADMIN_CREATE_MANIFEST));
  await page
    .getByLabel("Type gamma to confirm initialization", { exact: true })
    .fill("gamma");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Initialize cluster", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Operations", exact: true }),
  ).toBeVisible();
  await expect
    .poll(
      () =>
        JSON.parse(ctl("fleet-operations", "gamma").slice(3)).jobs.find(
          (j) => j.kind === "create",
        )?.state,
      { timeout: 90000 },
    )
    .toBe("completed");
  expect(
    JSON.parse(ctl("fleet-status", "gamma").slice(3)).status.cluster_ready,
  ).toBe(true);
  expect(
    docker("redis-cli", "-p", "6600", "SET", "created:in:admin", "works"),
  ).toBe("OK");
  expect(docker("redis-cli", "-p", "6600", "GET", "created:in:admin")).toBe(
    "works",
  );
});
