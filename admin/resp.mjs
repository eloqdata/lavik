// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import net from "node:net";
import tls from "node:tls";
import { readFile } from "node:fs/promises";
import { AdminError, endpoint } from "./meta.mjs";

const MAX_REPLY = 8 * 1024 * 1024;
export class RedisError extends Error {}

export function encode(args) {
  return Buffer.concat([
    Buffer.from(`*${args.length}\r\n`),
    ...args.flatMap((arg) => {
      const bytes = Buffer.isBuffer(arg) ? arg : Buffer.from(String(arg));
      return [Buffer.from(`$${bytes.length}\r\n`), bytes, Buffer.from("\r\n")];
    }),
  ]);
}

/** Incremental RESP2 parsing preserves binary keys/values and limits nesting/allocation. */
export function decode(buffer, offset = 0, depth = 0) {
  if (depth > 32) throw new Error("RESP nesting limit exceeded");
  const end = buffer.indexOf("\r\n", offset);
  if (end < 0) return null;
  const kind = String.fromCharCode(buffer[offset]);
  const value = buffer.toString("utf8", offset + 1, end);
  let next = end + 2;
  if (kind === "+") return { value, next };
  if (kind === "-") return { value: new RedisError(value), next };
  if (kind === ":") return { value, next }; // Preserve 64-bit integers.
  if (!["$", "*"].includes(kind) || !/^-?\d+$/.test(value))
    throw new Error("Invalid RESP reply");
  const length = Number(value);
  if (!Number.isSafeInteger(length) || length < -1 || length > MAX_REPLY)
    throw new Error("RESP size limit exceeded");
  if (length === -1) return { value: null, next };
  if (kind === "$") {
    if (buffer.length < next + length + 2) return null;
    if (buffer[next + length] !== 13 || buffer[next + length + 1] !== 10)
      throw new Error("Invalid RESP bulk terminator");
    return {
      value: buffer.subarray(next, next + length),
      next: next + length + 2,
    };
  }
  if (length > 100000) throw new Error("RESP array limit exceeded");
  const values = [];
  for (let i = 0; i < length; i++) {
    const result = decode(buffer, next, depth + 1);
    if (!result) return null;
    values.push(result.value);
    next = result.next;
  }
  return { value: values, next };
}

/** One bounded connection per request; credentials and protocol state cannot leak across clusters. */
export async function command(address, args, profile = {}) {
  const secure = address.startsWith("tls://");
  if (
    !secure &&
    (profile.dataCa ||
      profile.dataCert ||
      profile.dataKey ||
      profile.dataTlsRequired)
  )
    throw new AdminError(
      "This connection profile requires a TLS Data endpoint",
    );
  const target = endpoint(address.replace(/^(tcp|tls):\/\//, ""));
  const tlsOptions = secure
    ? {
        ca: profile.dataCa ? await readFile(profile.dataCa) : undefined,
        cert: profile.dataCert ? await readFile(profile.dataCert) : undefined,
        key: profile.dataKey ? await readFile(profile.dataKey) : undefined,
        checkServerIdentity: (_, certificate) =>
          tls.checkServerIdentity(target.host, certificate),
      }
    : {};
  return new Promise((resolve, reject) => {
    const socket = secure
      ? tls.connect({ ...target, ...tlsOptions })
      : net.connect(target);
    let buffer = Buffer.alloc(0);
    let replies = 0;
    const commands = profile.password
      ? [["AUTH", profile.password], args]
      : [args];
    const timer = setTimeout(
      () =>
        finish(new Error("Data request timed out; a write may have committed")),
      5000,
    );
    const finish = (error, result) => {
      clearTimeout(timer);
      socket.destroy();
      error ? reject(error) : resolve(result);
    };
    socket.once("error", (error) => finish(error));
    socket.once("end", () =>
      finish(new Error("Data connection closed before replying")),
    );
    socket.once(secure ? "secureConnect" : "connect", () =>
      socket.write(Buffer.concat(commands.map(encode))),
    );
    socket.on("data", (chunk) => {
      try {
        if (buffer.length + chunk.length > MAX_REPLY)
          throw new Error("Reply exceeds 8 MiB; narrow the query");
        buffer = Buffer.concat([buffer, chunk]);
        let parsed;
        while ((parsed = decode(buffer))) {
          buffer = buffer.subarray(parsed.next);
          if (parsed.value instanceof RedisError) return finish(parsed.value);
          if (++replies === commands.length) return finish(null, parsed.value);
        }
      } catch (error) {
        finish(error);
      }
    });
  });
}

export function text(value) {
  return Buffer.isBuffer(value) ? value.toString("utf8") : value;
}

export function jsonReply(value) {
  if (Buffer.isBuffer(value)) {
    const decoded = value.toString("utf8");
    return Buffer.from(decoded).equals(value)
      ? decoded
      : { base64: value.toString("base64"), bytes: value.length };
  }
  if (Array.isArray(value)) return value.map(jsonReply);
  if (value instanceof Error) return { error: value.message };
  return value;
}

export function keyBytes(value) {
  if (
    typeof value !== "string" ||
    value.length > 65536 ||
    !/^[A-Za-z0-9+/]*={0,2}$/.test(value)
  )
    throw new AdminError("Invalid encoded key");
  return Buffer.from(value, "base64");
}

export function slot(key) {
  const open = key.indexOf(123);
  const close = open >= 0 ? key.indexOf(125, open + 1) : -1;
  const bytes = close > open + 1 ? key.subarray(open + 1, close) : key;
  let crc = 0;
  for (const byte of bytes) {
    crc ^= byte << 8;
    for (let i = 0; i < 8; i++)
      crc = (crc & 0x8000 ? (crc << 1) ^ 0x1021 : crc << 1) & 0xffff;
  }
  return crc % 16384;
}
