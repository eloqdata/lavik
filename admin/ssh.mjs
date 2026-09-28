// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { isIP } from "node:net";
import { homedir } from "node:os";
import { AdminError } from "./meta.mjs";

/** Normalize SSH settings; cluster placement additionally requires a numeric peer IP. */
export function sshHost(input, { requireAddress = true } = {}) {
  if (!input || typeof input !== "object")
    throw new AdminError("Supply an SSH host");
  const host = String(input.host || "");
  const user = String(input.user || "");
  if (
    !/^[a-zA-Z0-9][a-zA-Z0-9.:-]{0,252}$/.test(host) ||
    !/^[a-z_][a-z0-9_-]{0,63}$/i.test(user)
  )
    throw new AdminError("Supply a valid SSH hostname and user");
  const port = Number(input.port || 22);
  if (!Number.isInteger(port) || port < 1 || port > 65535)
    throw new AdminError("Invalid SSH port");
  let address = String(input.address || (isIP(host) ? host : ""));
  if (!requireAddress && !isIP(address)) address = "";
  if (requireAddress && !isIP(address))
    throw new AdminError(
      "Advertised address must be a numeric IP reachable by the other cluster hosts",
    );
  const result = { host, user, port, address };
  for (const key of ["identityFile", "knownHostsFile"]) {
    if (!input[key]) continue;
    let path = String(input[key]);
    if (path.startsWith("~/")) path = homedir() + path.slice(1);
    if (!path.startsWith("/") || /[\0\r\n]/.test(path) || path.length > 4096)
      throw new AdminError(`Invalid ${key} path on the Admin host`);
    result[key] = path;
  }
  return result;
}

/** Require established trust and noninteractive authentication; never forward credentials. */
export function sshArguments(host) {
  const args = [
    "-T",
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=yes",
    "-o",
    "ConnectTimeout=8",
    "-o",
    "ServerAliveInterval=10",
    "-o",
    "ServerAliveCountMax=2",
    "-o",
    "ForwardAgent=no",
    "-o",
    "ClearAllForwardings=yes",
    "-p",
    String(host.port),
  ];
  if (host.identityFile)
    args.push("-o", "IdentitiesOnly=yes", "-i", host.identityFile);
  if (host.knownHostsFile)
    args.push("-o", `UserKnownHostsFile=${host.knownHostsFile}`);
  return [...args, `${host.user}@${host.host}`];
}

/** Send a fixed helper plus JSON on stdin; no user text becomes remote shell code. */
export class SSH {
  constructor() {
    this.active = 0;
  }
  /** SFTP upload targets only an owned directory returned by the fixed helper. */
  async upload(host, source, destination) {
    if (!/^\/[a-zA-Z0-9_./-]+$/.test(destination))
      throw new AdminError(
        "Remote cache path must not contain shell or whitespace characters",
      );
    const args = sshArguments(host).slice(1, -1);
    args[args.indexOf("-p")] = "-P";
    const target = `${host.user}@${
      host.host.includes(":") ? `[${host.host}]` : host.host
    }:${destination}`;
    await new Promise((resolve, reject) =>
      execFile(
        "scp",
        [...args, source, target],
        { timeout: 300000, maxBuffer: 1024 * 1024 },
        (error, _out, stderr) =>
          error
            ? reject(
                new AdminError(
                  `Release transfer failed: ${stderr || error.message}`,
                  502,
                ),
              )
            : resolve(),
      ),
    );
  }
  async call(host, request, timeout = 20000) {
    if (this.active >= 24)
      throw new AdminError("SSH workers are busy; retry shortly", 503);
    this.active++;
    try {
      const source = await readFile(new URL("./remote.py", import.meta.url));
      const code = `import base64;exec(base64.b64decode("${source.toString(
        "base64",
      )}"))`;
      const result = await new Promise((resolve, reject) => {
        const child = execFile(
          "ssh",
          [...sshArguments(host), `python3 -c '${code}'`],
          { timeout, maxBuffer: 12 * 1024 * 1024 },
          (error, stdout, stderr) => {
            if (error)
              return reject(
                new AdminError(
                  `SSH ${host.user}@${host.host}: ${(
                    stderr ||
                    stdout ||
                    error.message
                  ).slice(
                    -3000,
                  )}. Check passwordless login, trusted host key, and Python 3.`,
                  502,
                ),
              );
            try {
              resolve(JSON.parse(stdout));
            } catch {
              reject(new AdminError("SSH helper returned invalid JSON", 502));
            }
          },
        );
        child.stdin.on("error", () => {});
        child.stdin.end(JSON.stringify(request));
      });
      if (result.error) throw new AdminError(result.error, 502);
      return result;
    } finally {
      this.active--;
    }
  }
}
