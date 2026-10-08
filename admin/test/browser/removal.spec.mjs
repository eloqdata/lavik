// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { test, expect } from "@playwright/test";
import { start } from "../../server.mjs";
import { Meta } from "../../meta.mjs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
let app, directory, token, url, calls;
test.beforeEach(async ({ page }) => {
  directory = await mkdtemp(join(tmpdir(), "lavik-removal-browser-"));
  calls = [];
  const meta = new Meta("/nonexistent");
  meta.status = async () => {
    throw new Error("Meta is offline");
  };
  app = await start({
    directory,
    port: 0,
    meta,
    deploymentOptions: {
      ssh: {
        async call(_host, request) {
          calls.push(request);
          return { ok: true };
        },
      },
    },
  });
  token = (await readFile(app.tokenPath, "utf8")).trim();
  url = `http://127.0.0.1:${app.server.address().port}`;
  await app.fleet.add({ id: "remove-me", seeds: ["127.0.0.1:7200"] });
  await page.goto(url);
  await page.getByLabel("Access token").fill(token);
  await page.getByRole("button", { name: /Open workspace/ }).click();
});
test.afterEach(async () => {
  await app?.close();
  await rm(directory, { recursive: true, force: true });
});
test("offline cluster removal supports cancel and exact-name confirmation on mobile", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page
    .getByRole("button", { name: "Remove remove-me", exact: true })
    .click();
  await expect(page.getByLabel("Removal type")).toHaveValue("disconnect");
  await expect(
    page.locator('#removal-mode option[value="teardown"]'),
  ).toBeDisabled();
  await expect(page.locator("#erase-ack-label")).toBeHidden();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(await app.fleet.clusters()).toHaveLength(1);
  await page
    .getByRole("button", { name: "Remove remove-me", exact: true })
    .click();
  await page.getByLabel("Type remove-me to confirm").fill("wrong");
  await page
    .getByRole("button", { name: "Remove from Admin", exact: true })
    .click();
  await expect(page.locator("#dialog-error")).toContainText(
    "exact cluster name",
  );
  await page.getByLabel("Type remove-me to confirm").fill("remove-me");
  await page
    .getByRole("button", { name: "Remove from Admin", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Your workspace is ready" }),
  ).toBeVisible();
  await expect(
    page.locator('#cluster-select option[value="remove-me"]'),
  ).toHaveCount(0);
  expect(await app.fleet.clusters()).toHaveLength(0);
  expect(calls).toEqual([]);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Your workspace is ready" }),
  ).toBeVisible();
});
test("teardown requires a separate erasure acknowledgement and returns to the fleet", async ({
  page,
}) => {
  const plan = {
    id: "remove-me",
    owner: "saved-owner",
    storage: "file",
    supervisor: "process",
    baseDir: ".local/share/lavik/clusters",
    hosts: [{ host: "10.0.0.1" }],
    nodes: [{ name: "data-1", host: 0 }],
    monitorHosts: [],
  };
  await app.store.query(
    "INSERT INTO deployments VALUES(?,?)",
    [plan.id, JSON.stringify(plan)],
    "run",
  );
  await page
    .getByRole("button", { name: "Remove remove-me", exact: true })
    .click();
  await page.getByLabel("Removal type").selectOption("teardown");
  await expect(page.locator("#removal-impact")).toContainText("10.0.0.1");
  await page.getByLabel("Type remove-me to confirm").fill("remove-me");
  await page
    .getByRole("button", { name: "Permanently tear down", exact: true })
    .click();
  await expect(page.locator("#dialog-error")).toContainText(
    "Confirm permanent data deletion",
  );
  expect(calls).toEqual([]);
  await page.getByLabel(/I understand that this permanently deletes/).check();
  await page
    .getByRole("button", { name: "Permanently tear down", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Your workspace is ready" }),
  ).toBeVisible();
  expect(calls.map((r) => r.phase)).toEqual(["check", "stop", "delete"]);
  expect(await app.store.query("SELECT * FROM cluster_archives")).toHaveLength(
    1,
  );
});
