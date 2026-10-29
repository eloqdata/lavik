// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { readFile } from "node:fs/promises";

async function readAsset(name) {
  try {
    return await readFile(
      new URL(`./monitoring/${name}`, import.meta.url),
      "utf8",
    );
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    return readFile(
      new URL(`../deploy/monitoring/${name}`, import.meta.url),
      "utf8",
    );
  }
}

/** Ship the same runtime allowlist used by release packaging; never copy .env. */
export async function monitoringFiles() {
  const names = (await readAsset("runtime-files.txt"))
    .split("\n")
    .map((name) => name.trim())
    .filter((name) => name && !name.startsWith("#"));
  const files = {};
  for (const name of names) files[name] = await readAsset(name);
  return files;
}
