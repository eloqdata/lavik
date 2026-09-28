// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { AdminError } from "./meta.mjs";
import { mkdir, stat, rename, rm } from "node:fs/promises";
import { createWriteStream, createReadStream } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
import { Transform, Readable } from "node:stream";

const RELEASES = "https://github.com/eloqdata/lavik/releases";
const TAG = /^(nightly|v\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?)$/;

// Read only href attributes and keep same-repository links. GitHub HTML is
// never injected into the Admin UI, and cannot select arbitrary download URLs.
function links(html) {
  return [...html.matchAll(/<a\b[^>]*?\bhref\s*=\s*(["'])(.*?)\1/gs)].flatMap(
    (match) => {
      try {
        const url = new URL(match[2].replaceAll("&amp;", "&"), RELEASES);
        return url.origin === "https://github.com" &&
          !url.username &&
          !url.password
          ? [url]
          : [];
      } catch {
        return [];
      }
    },
  );
}

/** GitHub is the release authority; retain asset digests before any host mutation. */
export class Releases {
  constructor(fetcher = fetch, directory) {
    this.fetcher = fetcher;
    this.directory = directory;
    this.downloads = new Map();
    this.metadata = new Map();
  }
  /** Retain exact nightly bytes for restart and follower additions after GitHub advances. */
  async download(asset) {
    if (!this.directory)
      throw new AdminError("Release cache is not configured");
    if (this.downloads.has(asset.sha256))
      return this.downloads.get(asset.sha256);
    const task = (async () => {
      await mkdir(this.directory, { recursive: true, mode: 0o700 });
      const path = join(this.directory, `${asset.sha256}.tar.gz`);
      try {
        await stat(path);
        const digest = createHash("sha256");
        for await (const chunk of createReadStream(path)) digest.update(chunk);
        if (digest.digest("hex") !== asset.sha256)
          throw new AdminError(
            "Cached release checksum mismatch; restore the Admin release cache",
            409,
          );
        return path;
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
      const temporary = `${path}.${randomBytes(8).toString("hex")}.tmp`;
      try {
        const response = await this.fetcher(asset.url, {
          signal: AbortSignal.timeout(300000),
        });
        if (!response.ok)
          throw new AdminError(
            `Release download failed (${response.status}); review a new plan if nightly moved`,
            502,
          );
        const digest = createHash("sha256");
        let total = 0;
        const verifier = new Transform({
          transform(chunk, _encoding, callback) {
            total += chunk.length;
            if (total > 1024 ** 3)
              return callback(new Error("Release exceeds 1 GiB"));
            digest.update(chunk);
            callback(null, chunk);
          },
        });
        await pipeline(
          Readable.fromWeb(response.body),
          verifier,
          createWriteStream(temporary, { mode: 0o600, flags: "wx" }),
        );
        if (digest.digest("hex") !== asset.sha256)
          throw new AdminError(
            "Release checksum mismatch; nightly may have advanced. Review a fresh plan.",
            409,
          );
        await rename(temporary, path);
        return path;
      } finally {
        await rm(temporary, { force: true });
      }
    })();
    this.downloads.set(asset.sha256, task);
    try {
      return await task;
    } finally {
      this.downloads.delete(asset.sha256);
    }
  }
  /** Cache/coalesce short-lived public metadata; release bytes remain digest-pinned. */
  async request(url) {
    const cached = this.metadata.get(url);
    if (cached?.expires > Date.now()) return cached.promise;
    const promise = (async () => {
      const response = await this.fetcher(url, {
        headers: {
          Accept: "text/html, text/plain;q=0.9",
          "User-Agent": "Lavik-Admin",
        },
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok)
        throw new AdminError(
          `GitHub release page or asset returned ${response.status}. Check access to ${RELEASES} from the Admin computer, including proxy settings.`,
          502,
        );
      const chunks = [];
      let size = 0;
      for await (const chunk of response.body) {
        size += chunk.length;
        if (size > 4 * 1024 * 1024)
          throw new AdminError(
            "GitHub response exceeded the release metadata limit",
            502,
          );
        chunks.push(Buffer.from(chunk));
      }
      return Buffer.concat(chunks).toString("utf8");
    })();
    if (this.metadata.size >= 100)
      this.metadata.delete(this.metadata.keys().next().value);
    const entry = { expires: Date.now() + 60000, promise };
    this.metadata.set(url, entry);
    try {
      return await promise;
    } catch (error) {
      if (this.metadata.get(url) === entry) this.metadata.delete(url);
      throw error;
    }
  }
  /** Discover the public releases page without consuming GitHub REST API quota. */
  async list(page = 1) {
    if (!Number.isInteger(page) || page < 1 || page > 1000)
      throw new AdminError("Invalid release page");
    const urls = links(await this.request(`${RELEASES}?page=${page}`));
    const prefix = "/eloqdata/lavik/releases/tag/";
    const tags = [
      ...new Set(
        urls
          .filter((url) => url.pathname.startsWith(prefix))
          .map((url) => url.pathname.slice(prefix.length))
          .filter((tag) => TAG.test(tag)),
      ),
    ];
    if (!tags.length)
      throw new AdminError(
        `No Lavik releases found on ${RELEASES}. Check GitHub access or enter an exact release tag.`,
        502,
      );
    return {
      items: tags.map((tag) => ({
        tag,
        name: tag === "nightly" ? "Nightly (main)" : tag,
      })),
      more: urls.some(
        (url) =>
          url.pathname === "/eloqdata/lavik/releases" &&
          Number(url.searchParams.get("page")) === page + 1,
      ),
      source: RELEASES,
    };
  }
  /** Resolve only official listed assets with matching published SHA-256 files. */
  async resolve(tag) {
    if (typeof tag !== "string" || !TAG.test(tag))
      throw new AdminError(
        "Choose nightly or a Lavik release tag such as v0.1.0-beta.1",
      );
    // This is the public asset fragment loaded by GitHub's own release page.
    // Discovery does not depend on the shared unauthenticated REST API quota.
    const urls = new Set(
      links(await this.request(`${RELEASES}/expanded_assets/${tag}`)).map(
        (url) => url.href,
      ),
    );
    const assets = {};
    for (const arch of ["x86_64", "aarch64"]) {
      const name = `lavik-${tag}-linux-${arch}-minimal.tar.gz`;
      const url = `${RELEASES}/download/${tag}/${name}`;
      if (!urls.has(url) || !urls.has(`${url}.sha256`)) continue;
      const line = await this.request(`${url}.sha256`);
      const match = /^([a-f0-9]{64})\s+\*?(\S+)\s*$/.exec(line);
      if (!match || match[2] !== name)
        throw new AdminError("Invalid release checksum", 502);
      assets[arch] = { name, url, sha256: match[1] };
    }
    if (!Object.keys(assets).length)
      throw new AdminError(
        "This release has no checksummed Linux minimal packages",
      );
    return { tag, source: `${RELEASES}/tag/${tag}`, assets };
  }
}
