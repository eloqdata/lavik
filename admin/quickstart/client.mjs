// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
// Run inside a demo node so a Mac-hosted Admin can reach private Docker endpoints.
import { command } from "./resp.mjs";
let input = "";
for await (const chunk of process.stdin) {
  input += chunk;
  if (Buffer.byteLength(input) > 1024 * 1024)
    throw new Error("Demo request exceeds 1 MiB");
}
const pack = (value) =>
  Buffer.isBuffer(value)
    ? { base64: value.toString("base64") }
    : Array.isArray(value)
    ? value.map(pack)
    : value;
try {
  const { address, args } = JSON.parse(input);
  console.log(JSON.stringify({ value: pack(await command(address, args)) }));
} catch (error) {
  console.log(JSON.stringify({ error: error.message }));
}
