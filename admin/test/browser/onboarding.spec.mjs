// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { test, expect } from "@playwright/test";
import { start } from "../../server.mjs";
import { Meta } from "../../meta.mjs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
let app, directory, token, url;
test.beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "lavik-onboarding-browser-"));
  const meta = new Meta("/nonexistent");
  meta.status = async (cluster) => {
    if (JSON.parse(cluster.seeds).includes("10.0.0.9:7200"))
      throw new Error("No Meta seed is reachable");
    return {
      cluster_state: "created",
      cluster_ready: true,
      meta_members: [],
      groups: [],
      data_nodes: [],
    };
  };
  meta.nodes = async () => [];
  app = await start({ directory, port: 0, meta });
  token = (await readFile(app.tokenPath, "utf8")).trim();
  url = `http://127.0.0.1:${app.server.address().port}`;
});
test.afterAll(async () => {
  await app?.close();
  await rm(directory, { recursive: true, force: true });
});
test.beforeEach(async ({ page }) => {
  await page.goto(url);
  await page.getByLabel("Access token").fill(token);
  await page.getByRole("button", { name: /Open workspace/ }).click();
});
test("three onboarding paths start the demo in Admin and report progress on desktop and mobile", async ({
  page,
}, testInfo) => {
  await page.getByRole("button", { name: /Create cluster/ }).click();
  await expect(
    page.getByRole("heading", { name: "Start with Lavik" }),
  ).toBeVisible();
  let requests = 0;
  let ready = false;
  await page.route("**/api/demo", (route) => {
    if (route.request().method() === "POST") requests++;
    return route.fulfill({
      json: {
        id: "demo-cluster",
        state: ready ? "completed" : "running",
        step: "download",
        detail: ready
          ? "Demo ready"
          : "Downloading and verifying the Linux release",
        log: "Downloading Lavik nightly…",
      },
    });
  });
  await page
    .getByRole("button", { name: "Try a demo cluster", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Setting up demo-cluster" }),
  ).toBeVisible();
  await expect(page.getByRole("status")).toContainText(
    "Downloading and verifying",
  );
  expect(requests).toBe(1);
  await expect(
    page.getByText("./admin/quickstart/setup.sh", { exact: true }),
  ).toHaveCount(0);
  await page.screenshot({
    path: testInfo.outputPath("onboarding-desktop.png"),
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: testInfo.outputPath("onboarding-mobile.png"),
    fullPage: true,
  });
  ready = true;
  await expect(
    page.getByRole("heading", { name: "Your demo is ready" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: /Open demo dashboard/ }),
  ).toBeVisible();
});
test("connection discovery retains failed input and saves only reviewed settings", async ({
  page,
}) => {
  await page
    .getByRole("button", { name: "Connect existing cluster", exact: true })
    .first()
    .click();
  await page.getByLabel("Cluster name", { exact: true }).fill("imported");
  await page.getByLabel("Meta seed addresses").fill("10.0.0.9:7200");
  await page
    .getByRole("button", { name: "Test connection", exact: true })
    .click();
  await expect(page.locator("#dialog-error")).toContainText(
    "No Meta seed is reachable",
  );
  await expect(page.getByLabel("Cluster name", { exact: true })).toHaveValue(
    "imported",
  );
  await page
    .getByLabel("Meta seed addresses")
    .fill("tcp://10.0.0.1:7200\n10.0.0.2:7200");
  await page
    .getByRole("button", { name: "Test connection", exact: true })
    .click();
  await expect(page.getByText("Meta connection verified")).toBeVisible();
  expect(await app.fleet.clusters()).toEqual([]);
  await page.getByLabel("Cluster name", { exact: true }).fill("imported-final");
  await page
    .getByRole("button", { name: "Test connection", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Connect cluster", exact: true })
    .click();
  await expect
    .poll(async () => (await app.fleet.clusters()).map((c) => c.id))
    .toEqual(["imported-final"]);
});
test("production prepares hosts first and reviews storage and monitoring together", async ({
  page,
}, testInfo) => {
  const hosts = [1, 2, 3].map((i) => ({
    id: `host-${i}`,
    host: `10.0.0.${i}`,
    address: `10.0.0.${i}`,
    user: "root",
    port: 22,
  }));
  await page.route("**/api/hosts", (route) => route.fulfill({ json: hosts }));
  await page.route("**/api/hosts/*/verify", (route) =>
    route.fulfill({
      json: hosts.find((h) => route.request().url().includes(h.id)),
    }),
  );
  await page.route("**/api/releases?*", (route) =>
    route.fulfill({
      json: { items: [{ tag: "v0.1.0-beta.1", name: "Beta" }], more: false },
    }),
  );
  let reviewed;
  await page.route("**/api/setup/preview", (route) => {
    reviewed = route.request().postDataJSON();
    return route.fulfill({
      json: {
        ...reviewed,
        token: "reviewed-token",
        ready: true,
        release: { tag: reviewed.release },
        hosts,
        checks: hosts.map((_, i) => ({
          host: i,
          ok: true,
          arch: "aarch64",
          cpus: 16,
          freeBytes: 100 * 1024 ** 3,
          spdk: { serial: `NVME-${i}` },
        })),
        nodes: hosts.flatMap((_, i) => [
          {
            name: `meta-${i + 1}`,
            kind: "meta",
            host: i,
            port: 7100 + i,
            ctl: 7200 + i,
          },
          {
            name: `data-${i + 1}`,
            kind: "data",
            host: i,
            port: 6379,
            metrics: 9100,
            spdk: reviewed.spdkDevices[i],
          },
        ]),
        warnings: [],
      },
    });
  });
  await page.getByRole("button", { name: /Create cluster/ }).click();
  await page.getByRole("button", { name: "Set up machines" }).click();
  await expect(
    page.getByRole("heading", { name: "Prepare hosts" }),
  ).toBeVisible();
  await expect(page.locator("#prepared-hosts input")).toHaveCount(3);
  for (const checkbox of await page.locator("#prepared-hosts input").all())
    await checkbox.check();
  await page
    .getByRole("button", { name: "Continue to node placement" })
    .click();
  await page.getByLabel("Cluster name", { exact: true }).fill("production");
  await page.getByLabel("Storage engine").selectOption("spdk");
  for (let i = 1; i <= 3; i++)
    await page
      .getByLabel(`Host ${i} dedicated NVMe namespace`)
      .fill("spdk://0000:01:00.0/1");
  await page.locator('#monitor-hosts input[value="0"]').check();
  await page.getByRole("button", { name: "Check hosts & review" }).click();
  await expect(
    page.getByRole("heading", { name: "Ready to deploy" }),
  ).toBeVisible();
  expect(reviewed.monitorHosts).toEqual([0]);
  expect(Object.keys(reviewed.spdkDevices)).toHaveLength(3);
  await expect(
    page.getByText("SPDK host configuration", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByLabel(/I confirm these are dedicated controllers/),
  ).not.toBeChecked();
  await page.screenshot({
    path: testInfo.outputPath("production-review.png"),
    fullPage: true,
  });
  await page
    .getByRole("button", { name: "Edit settings", exact: true })
    .click();
  await page.getByLabel("Storage engine").selectOption("file");
  await expect(
    page.getByRole("heading", { name: "Ready to deploy" }),
  ).not.toBeVisible();
});
