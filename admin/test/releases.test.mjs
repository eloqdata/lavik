// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import { Releases } from "../releases.mjs";

const root = "https://github.com/eloqdata/lavik/releases";
const tag = "nightly";
const name = `lavik-${tag}-linux-aarch64-minimal.tar.gz`;
const asset = `${root}/download/${tag}/${name}`;
const assetLinks = `<a href="${asset}">binary</a><a href="${asset}.sha256">checksum</a>`;

test("public release discovery and resolution work without any REST API request", async () => {
  const requests = [];
  const releases = new Releases(async (url) => {
    requests.push(url);
    if (url.includes("api.github.com"))
      return new Response("rate limited", { status: 403 });
    if (url === `${root}?page=1`)
      return new Response(
        '<a href="/eloqdata/lavik/releases/tag/nightly">Nightly</a><a href="/eloqdata/lavik/releases/tag/v0.1.0-beta.1">Beta</a><a href="/eloqdata/lavik/releases/tag/nightly">Duplicate</a><a href="/eloqdata/lavik/releases?page=2">Next</a><a href="https://attacker.invalid/eloqdata/lavik/releases/tag/v9.9.9">Ignore</a>',
      );
    if (url === `${root}/expanded_assets/nightly`)
      return new Response(assetLinks);
    if (url === `${asset}.sha256`)
      return new Response(`${"a".repeat(64)}  ${name}\n`);
    throw new Error(`Unexpected URL: ${url}`);
  });
  const [first, second] = await Promise.all([releases.list(), releases.list()]);
  assert.deepEqual(first, second);
  assert.deepEqual(
    first.items.map((item) => item.tag),
    ["nightly", "v0.1.0-beta.1"],
  );
  assert.equal(first.more, true);
  assert.equal(
    requests.length,
    1,
    "simultaneous/repeated lookups share cached metadata",
  );
  const resolved = await releases.resolve(tag);
  assert.equal(resolved.assets.aarch64.sha256, "a".repeat(64));
  assert.equal(resolved.assets.aarch64.url, asset);
  assert.ok(requests.every((url) => url.startsWith(root)));
});

test("public discovery refuses changed HTML, missing checksums, and mismatched filenames", async () => {
  const noLinks = new Releases(
    async () => new Response("<html>sign in to your proxy</html>"),
  );
  await assert.rejects(noLinks.list(), /No Lavik releases/);
  await assert.rejects(noLinks.resolve(tag), /no checksummed/);
  const mismatch = new Releases(
    async (url) =>
      new Response(
        url.endsWith(".sha256")
          ? `${"b".repeat(64)} another.tar.gz`
          : assetLinks,
      ),
  );
  await assert.rejects(mismatch.resolve(tag), /Invalid release checksum/);
  await assert.rejects(mismatch.resolve("../../evil"), /Choose nightly/);
});

test("failed metadata is retried, while oversized pages and real HTTP failures stay explicit", async () => {
  let attempts = 0;
  const releases = new Releases(async () =>
    ++attempts === 1
      ? new Response("denied", { status: 403 })
      : new Response(
          '<a href="/eloqdata/lavik/releases/tag/nightly">Nightly</a>',
        ),
  );
  await assert.rejects(releases.list(), /returned 403.*proxy settings/);
  assert.equal((await releases.list()).items[0].tag, "nightly");
  await assert.rejects(
    new Releases(
      async () => new Response("x".repeat(4 * 1024 * 1024 + 1)),
    ).list(),
    /metadata limit/,
  );
});

test("SPDK resolution selects the standard asset without falling back to minimal", async () => {
  const fullName = name.replace("-minimal", "");
  const fullURL = asset.replace("-minimal", "");
  const releases = new Releases(
    async (url) =>
      new Response(
        url.endsWith(".sha256")
          ? `${"c".repeat(64)}  ${fullName}\n`
          : `${assetLinks}<a href="${fullURL}">full</a><a href="${fullURL}.sha256">checksum</a>`,
      ),
  );
  const resolved = await releases.resolve(tag, "standard");
  assert.equal(resolved.assets.aarch64.name, fullName);
  await assert.rejects(
    new Releases(async () => new Response(assetLinks)).resolve(tag, "standard"),
    /no checksummed Linux standard/,
  );
});
