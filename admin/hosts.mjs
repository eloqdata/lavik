// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { AdminError } from "./meta.mjs";
import { sshArguments, sshHost } from "./ssh.mjs";

const quote = (value) => "'" + value.replaceAll("'", "'\\''") + "'";
const installKey = `set -eu
umask 077
IFS= read -r key
case "$key" in 'ssh-ed25519 '*) ;; *) exit 1 ;; esac
cd "$HOME"
# Never follow a link out of the account's SSH directory.
[ ! -L .ssh ] && [ ! -L .ssh/authorized_keys ]
mkdir -p .ssh
chmod 700 .ssh
touch .ssh/authorized_keys
chmod 600 .ssh/authorized_keys
if ! grep -Fqx -- "$key" .ssh/authorized_keys; then
  printf '\\n%s\\n' "$key" >> .ssh/authorized_keys
fi
`;

function run(
  binary,
  args,
  { input = "", env = process.env, secret = "" } = {},
) {
  return new Promise((resolve, reject) => {
    const child = execFile(
      binary,
      args,
      { env, timeout: 45000, maxBuffer: 256 * 1024 },
      (error, stdout, stderr) => {
        if (error) {
          // Some SSH servers echo arbitrary banners; never return the supplied secret.
          const detail = secret
            ? stderr.replaceAll(secret, "[redacted]")
            : stderr;
          reject(
            new AdminError(
              `SSH preparation failed: ${
                detail.slice(-2000) || "connection or authentication failed"
              }`,
              502,
            ),
          );
        } else resolve(stdout);
      },
    );
    child.stdin.on("error", () => {});
    child.stdin.end(input);
  });
}

async function withSecret(secret, call) {
  if (!secret) return call(process.env);
  // A short /tmp path also respects macOS's Unix-socket path length limit.
  const directory = await mkdtemp(join(tmpdir(), "lv-ask-"));
  const path = join(directory, "s");
  const clients = new Set();
  let attempts = 0;
  const server = net.createServer((socket) => {
    clients.add(socket);
    socket.on("close", () => clients.delete(socket));
    socket.on("error", () => {});
    socket.setTimeout(5000, () => socket.destroy());
    let prompt = "";
    socket.on("data", (bytes) => {
      prompt += bytes.toString();
      if (prompt.length > 4096) return socket.destroy();
      if (!prompt.includes("\n")) return;
      if (++attempts > 3 || !/(password|passphrase)/i.test(prompt))
        return socket.destroy();
      socket.end(secret + "\n");
    });
  });
  try {
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(path, resolve);
    });
    await chmod(path, 0o600);
    const helper = join(directory, "askpass");
    await writeFile(
      helper,
      `#!/bin/sh\nexec ${quote(process.execPath)} ${quote(
        fileURLToPath(new URL("./askpass.mjs", import.meta.url)),
      )} "$@"\n`,
      { mode: 0o700 },
    );
    return await call({
      ...process.env,
      DISPLAY: "lavik-admin:0",
      SSH_ASKPASS: helper,
      SSH_ASKPASS_REQUIRE: "force",
      LAVIK_ASKPASS_SOCKET: path,
    });
  } finally {
    secret = "";
    for (const client of clients) client.destroy();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
}

/** Reusable host inventory and a workspace-owned identity, independent of node placement. */
export class Hosts {
  constructor(store, directory) {
    this.store = store;
    this.directory = join(directory, "ssh");
    this.active = new Set();
  }
  /** Public connection metadata only; bootstrap credentials never enter the catalog. */
  async list() {
    return (
      await this.store.query(
        "SELECT connection, verified_at FROM hosts ORDER BY verified_at DESC, id",
      )
    ).map((row) => ({
      ...JSON.parse(row.connection),
      verifiedAt: row.verified_at,
    }));
  }
  async identity() {
    // Coalesce generation; publish both key files together and never rotate on restart.
    if (!this.key)
      this.key = this.createIdentity().catch((error) => {
        this.key = undefined;
        throw error;
      });
    return this.key;
  }
  async createIdentity() {
    await mkdir(this.directory, { recursive: true, mode: 0o700 });
    const directory = join(this.directory, "identity");
    try {
      await stat(directory);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      const staging = await mkdtemp(join(this.directory, ".key-"));
      try {
        await run("ssh-keygen", [
          "-q",
          "-t",
          "ed25519",
          "-N",
          "",
          "-C",
          "lavik-admin",
          "-f",
          join(staging, "id_ed25519"),
        ]);
        await rename(staging, directory);
      } finally {
        await rm(staging, { recursive: true, force: true });
      }
    }
    const identityFile = join(directory, "id_ed25519");
    await chmod(identityFile, 0o600);
    const publicKey = (await readFile(identityFile + ".pub", "utf8")).trim();
    if (!/^ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\r\n]*)?$/.test(publicKey))
      throw new AdminError(
        "Admin SSH identity is invalid; restore the workspace backup",
        500,
      );
    const knownHostsFile = join(this.directory, "known_hosts");
    await writeFile(knownHostsFile, "", { flag: "a", mode: 0o600 });
    await chmod(knownHostsFile, 0o600);
    return { identityFile, knownHostsFile, publicKey };
  }
  /** Install Admin's key with an initial login, then prove a fresh key-only login works. */
  async prepare(input) {
    const host = sshHost(input, { requireAddress: false });
    const id = createHash("sha256")
      .update(JSON.stringify([host.host, host.port, host.user]))
      .digest("hex")
      .slice(0, 32);
    if (this.active.has(id))
      throw new AdminError("This host is already being prepared", 409);
    if (this.active.size >= 4)
      throw new AdminError("Host preparation is busy; retry shortly", 503);
    const method = input.method || "key";
    if (!["password", "key"].includes(method))
      throw new AdminError("Choose password or private key authentication");
    let secret = method === "password" ? input.password : input.passphrase;
    if (
      secret !== undefined &&
      (typeof secret !== "string" ||
        secret.length > 4096 ||
        /[\0\r\n]/.test(secret))
    )
      throw new AdminError("Invalid SSH credential");
    if (method === "password" && !secret)
      throw new AdminError("Enter the SSH login password");
    this.active.add(id);
    try {
      const managed = await this.identity();
      const bootstrap = { ...host, knownHostsFile: managed.knownHostsFile };
      const args = sshArguments(bootstrap);
      args[args.indexOf("BatchMode=yes")] = `BatchMode=${
        secret ? "no" : "yes"
      }`;
      // Trust on first use is explicit in the form. OpenSSH still rejects changed keys.
      args[args.indexOf("StrictHostKeyChecking=yes")] =
        "StrictHostKeyChecking=accept-new";
      args.unshift(
        "-F",
        "/dev/null",
        "-o",
        "ControlPath=none",
        "-o",
        "ControlMaster=no",
        "-o",
        "NumberOfPasswordPrompts=1",
      );
      if (method === "password")
        args.unshift(
          "-o",
          "PubkeyAuthentication=no",
          "-o",
          "PreferredAuthentications=password,keyboard-interactive",
        );
      else
        args.unshift(
          "-o",
          "PasswordAuthentication=no",
          "-o",
          "KbdInteractiveAuthentication=no",
        );
      await withSecret(secret, (env) =>
        run("ssh", [...args, `sh -c ${quote(installKey)}`], {
          env,
          input: managed.publicKey + "\n",
          secret,
        }),
      );
      const connection = {
        id,
        host: host.host,
        port: host.port,
        user: host.user,
        ...(host.address ? { address: host.address } : {}),
        identityFile: managed.identityFile,
        knownHostsFile: managed.knownHostsFile,
      };
      await this.verifyConnection(connection);
      return await this.save(connection);
    } finally {
      secret = "";
      this.active.delete(id);
    }
  }
  /** Resolve inventory references for both browser and fleet CLI deployment plans. */
  async resolve(input) {
    if (!input?.hostId) return input;
    const row = await this.store.query(
      "SELECT connection FROM hosts WHERE id=?",
      [input.hostId],
      "get",
    );
    if (!row) throw new AdminError("Prepared host not found", 404);
    return {
      ...JSON.parse(row.connection),
      ...(input.address ? { address: input.address } : {}),
    };
  }
  async verifyConnection(host) {
    // Configured extra identities, a multiplexed connection, or an agent could
    // otherwise hide a broken installed key. Prepared hosts use direct SSH.
    await run("ssh", [
      "-F",
      "/dev/null",
      "-o",
      "ControlPath=none",
      "-o",
      "ControlMaster=no",
      "-o",
      "IdentityAgent=none",
      "-o",
      "PasswordAuthentication=no",
      "-o",
      "KbdInteractiveAuthentication=no",
      ...sshArguments(host),
      "true",
    ]);
  }
  async save(connection) {
    const verifiedAt = new Date().toISOString();
    await this.store.query(
      "INSERT INTO hosts(id,connection,verified_at) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET connection=excluded.connection, verified_at=excluded.verified_at",
      [connection.id, JSON.stringify(connection), verifiedAt],
      "run",
    );
    return { ...connection, verifiedAt };
  }
  /** Recheck a saved host before allowing it into a new placement draft. */
  async verify(id) {
    const row = await this.store.query(
      "SELECT connection FROM hosts WHERE id=?",
      [id],
      "get",
    );
    if (!row) throw new AdminError("Prepared host not found", 404);
    if (this.active.size >= 4 || this.active.has(id))
      throw new AdminError("Host preparation is busy; retry shortly", 503);
    this.active.add(id);
    try {
      const connection = JSON.parse(row.connection);
      await this.verifyConnection(connection);
      return await this.save(connection);
    } finally {
      this.active.delete(id);
    }
  }
}
