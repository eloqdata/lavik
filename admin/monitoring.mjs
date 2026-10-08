// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { readFile } from "node:fs/promises";

/** Ship the same pinned stack and dashboards as the standalone monitoring runbook. */
export async function monitoringFiles() {
  const names = [
    "compose.yaml",
    "prometheus/prometheus.yml",
    "grafana/dashboards/lavik-overview.json",
    "grafana/provisioning/dashboards/lavik.yaml",
    "grafana/provisioning/datasources/prometheus.yaml",
  ];
  const files = {};
  for (const name of names) {
    try {
      files[name] = await readFile(
        new URL(`./monitoring/${name}`, import.meta.url),
        "utf8",
      );
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      files[name] = await readFile(
        new URL(`../deploy/monitoring/${name}`, import.meta.url),
        "utf8",
      );
    }
  }
  return files;
}
