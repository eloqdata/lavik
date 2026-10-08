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
  if (![0, 1, 2, 3, 4].includes(version))
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
    CREATE TABLE IF NOT EXISTS cluster_archives (
      id INTEGER PRIMARY KEY, cluster_id TEXT NOT NULL, removed_at TEXT NOT NULL,
      snapshot TEXT NOT NULL
    ) STRICT;
    CREATE TRIGGER IF NOT EXISTS block_jobs_after_teardown
      BEFORE INSERT ON jobs WHEN NEW.kind != 'teardown' AND EXISTS (
        SELECT 1 FROM jobs WHERE cluster_id=NEW.cluster_id AND kind='teardown'
      ) BEGIN SELECT RAISE(ABORT, 'Cluster teardown has been confirmed'); END;
    PRAGMA user_version=4;
  `);
  parentPort.on("message", ({ id, sql, params, mode }) => {
    try {
      let value;
      if (mode === "close") db.close();
      else if (mode === "forget") {
        // Archive and remove in one worker turn: admission cannot race the
        // active-job check, and foreign keys never leave orphan workflow state.
        db.exec("BEGIN IMMEDIATE");
        try {
          const cluster = db
            .prepare("SELECT * FROM clusters WHERE id=?")
            .get(params[0]);
          if (!cluster) throw new Error("Cluster not found");
          if (params[2]) {
            const result = db
              .prepare(
                "UPDATE jobs SET state='completed',step='completed',detail='Owned deployment removed; hosts retained',updated_at=? WHERE id=? AND cluster_id=? AND kind='teardown' AND state='running'",
              )
              .run(params[1], params[2], params[0]);
            if (result.changes !== 1)
              throw new Error("Teardown state changed; inspect operations");
          }
          const jobs = db
            .prepare("SELECT * FROM jobs WHERE cluster_id=?")
            .all(params[0]);
          if (
            jobs.some((job) =>
              ["queued", "running", "uncertain"].includes(job.state),
            )
          )
            throw new Error(
              "Resolve active operations before removing the connection",
            );
          const deployment = db
            .prepare("SELECT * FROM deployments WHERE cluster_id=?")
            .get(params[0]);
          db.prepare(
            "INSERT INTO cluster_archives(cluster_id,removed_at,snapshot) VALUES(?,?,?)",
          ).run(
            params[0],
            params[1],
            JSON.stringify({ cluster, jobs, deployment }),
          );
          db.prepare("DELETE FROM jobs WHERE cluster_id=?").run(params[0]);
          db.prepare("DELETE FROM deployments WHERE cluster_id=?").run(
            params[0],
          );
          db.prepare("DELETE FROM clusters WHERE id=?").run(params[0]);
          db.prepare(
            "INSERT INTO audit(created_at,action,cluster_id,detail) VALUES(?,?,?,?)",
          ).run(
            params[1],
            "cluster-removed",
            params[0],
            "Catalog and history archived; no remote action",
          );
          db.exec("COMMIT");
          value = { removed: params[0] };
        } catch (error) {
          db.exec("ROLLBACK");
          throw error;
        }
      } else if (mode === "batch") {
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
