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
  /** Admit related catalog and deployment intent in one worker-owned transaction. */
  batch(statements) {
    return this.query(statements, [], "batch");
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
  if (![0, 1, 2, 3].includes(version))
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
    CREATE TABLE IF NOT EXISTS deployments (
      cluster_id TEXT PRIMARY KEY REFERENCES clusters(id), plan TEXT NOT NULL
    ) STRICT;
    CREATE TABLE IF NOT EXISTS hosts (
      id TEXT PRIMARY KEY, connection TEXT NOT NULL, verified_at TEXT NOT NULL
    ) STRICT;
    PRAGMA user_version=3;
  `);
  parentPort.on("message", ({ id, sql, params, mode }) => {
    try {
      let value;
      if (mode === "close") db.close();
      else if (mode === "batch") {
        db.exec("BEGIN IMMEDIATE");
        try {
          for (const item of sql) {
            const result = db.prepare(item.sql).run(...item.params);
            if (
              item.expectedChanges !== undefined &&
              result.changes !== item.expectedChanges
            )
              throw new Error("Deployment changed; review the follower again");
          }
          db.exec("COMMIT");
        } catch (error) {
          db.exec("ROLLBACK");
          throw error;
        }
        value = { committed: true };
      } else {
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
