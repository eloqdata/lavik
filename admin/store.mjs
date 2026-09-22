// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} from "node:worker_threads";

/** One worker owns the fleet database; SQLite fsync never stalls HTTP or Meta I/O. */
export class Store {
  constructor(path) {
    this.pending = new Map();
    this.sequence = 0;
    this.worker = new Worker(new URL(import.meta.url), { workerData: path });
    this.worker.on("message", ({ id, value, error }) => {
      const pending = this.pending.get(id);
      this.pending.delete(id);
      if (pending)
        error ? pending.reject(new Error(error)) : pending.resolve(value);
    });
    this.worker.on("error", (error) => {
      this.failure = error;
      for (const pending of this.pending.values()) pending.reject(error);
      this.pending.clear();
    });
  }
  query(sql, params = [], mode = "all") {
    if (this.failure) return Promise.reject(this.failure);
    return new Promise((resolve, reject) => {
      const id = ++this.sequence;
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, sql, params, mode });
    });
  }
  async close() {
    try {
      await this.query("", [], "close");
    } finally {
      await this.worker.terminate();
    }
  }
}

if (!isMainThread) {
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(workerData, { timeout: 0 });
  const version = db.prepare("PRAGMA user_version").get().user_version;
  if (version !== 0 && version !== 1)
    throw new Error(`Unsupported fleet database version: ${version}`);
  db.exec(`
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=FULL;
    PRAGMA foreign_keys=ON;
    CREATE TABLE IF NOT EXISTS clusters (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, seeds TEXT NOT NULL,
      profile TEXT NOT NULL DEFAULT 'default', created_at TEXT NOT NULL
    ) STRICT;
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY, cluster_id TEXT NOT NULL REFERENCES clusters(id),
      kind TEXT NOT NULL, input TEXT NOT NULL, state TEXT NOT NULL,
      step TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '',
      meta_operation_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    ) STRICT;
    CREATE UNIQUE INDEX IF NOT EXISTS one_active_job_per_cluster ON jobs(cluster_id)
      WHERE state IN ('queued','running','uncertain');
    CREATE TABLE IF NOT EXISTS audit (
      id INTEGER PRIMARY KEY, created_at TEXT NOT NULL,
      action TEXT NOT NULL, cluster_id TEXT, detail TEXT NOT NULL
    ) STRICT;
    PRAGMA user_version=1;
  `);
  parentPort.on("message", ({ id, sql, params, mode }) => {
    try {
      let value;
      if (mode === "close") db.close();
      else {
        const statement = db.prepare(sql);
        value =
          mode === "run"
            ? statement.run(...params)
            : mode === "get"
            ? statement.get(...params)
            : statement.all(...params);
      }
      parentPort.postMessage({ id, value });
    } catch (error) {
      parentPort.postMessage({ id, error: error.message });
    }
  });
}
