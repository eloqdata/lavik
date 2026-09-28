// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import net from "node:net";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

const args = process.argv.slice(2);
if (args[0] === "fleet-plan" && args.length === 2)
  args[1] = Buffer.from(await readFile(args[1])).toString("base64url");
if (args[0] === "fleet-follower-plan" && args.length === 3)
  args[2] = Buffer.from(await readFile(args[2])).toString("base64url");
if (!args[0]?.startsWith("fleet-") || args.some((a) => /[\s\0]/.test(a)))
  throw new Error(
    "Supply a fleet command; use ./lavik-admin --help for examples",
  );
const socket = net.connect(join(process.env.LAVIK_ADMIN_DATA, "admin.sock"));
socket.setTimeout(600000, () =>
  socket.destroy(
    new Error(
      "Fleet request timed out; inspect operation status before retrying",
    ),
  ),
);
let reply = "";
socket.on("connect", () => socket.write(args.join(" ") + "\n"));
socket.on("data", (chunk) => {
  reply += chunk;
  if (reply.length > 16 * 1024 * 1024)
    socket.destroy(new Error("Fleet reply exceeds 16 MiB"));
});
socket.on("end", () => {
  process.stdout.write(reply);
  if (!reply.startsWith("OK ")) process.exitCode = 1;
});
socket.on("error", (error) => {
  console.error(error.message);
  process.exitCode = 1;
});
