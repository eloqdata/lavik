// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
// Executed only by OpenSSH: the private socket carries one transient credential.
// No credential is placed in argv, environment variables, or temporary files.
import net from "node:net";
const socket = net.connect(process.env.LAVIK_ASKPASS_SOCKET);
socket.setTimeout(5000, () => socket.destroy(new Error("Askpass timed out")));
socket.on("error", () => process.exit(1));
socket.on("data", (bytes) => process.stdout.write(bytes));
socket.on("connect", () =>
  socket.write(JSON.stringify(process.argv[2] || "") + "\n"),
);
