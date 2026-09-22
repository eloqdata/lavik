// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import test from "node:test";
import assert from "node:assert/strict";
import {
  encode,
  decode,
  jsonReply,
  slot,
  RedisError,
  command,
} from "../resp.mjs";
import { endpoint, identifier, Meta } from "../meta.mjs";

test("RESP parser tolerates every split and preserves binary data", () => {
  const bytes = encode(["value", Buffer.from([0, 255, 13, 10]), ""]);
  for (let i = 0; i < bytes.length; i++)
    assert.equal(decode(bytes.subarray(0, i)), null);
  const parsed = decode(bytes);
  assert.equal(parsed.next, bytes.length);
  assert.deepEqual(parsed.value, [
    Buffer.from("value"),
    Buffer.from([0, 255, 13, 10]),
    Buffer.alloc(0),
  ]);
  assert.deepEqual(jsonReply(parsed.value), [
    "value",
    { base64: "AP8NCg==", bytes: 4 },
    "",
  ]);
});

test("RESP limits reject malformed and excessive allocations", () => {
  for (const value of [
    "$-2\r\n",
    "$999999999\r\n",
    "*9999999\r\n",
    "$1\r\naXX",
    "?no\r\n",
  ])
    assert.throws(() => decode(Buffer.from(value)));
  assert.equal(decode(Buffer.from("$-1\r\n")).value, null);
  assert.equal(
    decode(Buffer.from(":9223372036854775807\r\n")).value,
    "9223372036854775807",
  );
  assert.ok(
    decode(Buffer.from("-MOVED 1 127.0.0.1:6379\r\n")).value instanceof
      RedisError,
  );
});

test("slot routing matches the Redis hash-tag contract", () => {
  assert.equal(slot(Buffer.from("123456789")), 12739);
  assert.equal(
    slot(Buffer.from("a{customer}x")),
    slot(Buffer.from("b{customer}y")),
  );
  assert.notEqual(slot(Buffer.from("a{}x")), slot(Buffer.from("b{}y")));
});

test("endpoints and command identifiers cannot inject options or wire tokens", () => {
  assert.deepEqual(endpoint("[::1]:6379"), { host: "::1", port: 6379 });
  for (const value of [
    "localhost:6379",
    "127.0.0.1:0",
    "127.0.0.1:65536",
    "127.0.0.1:123\nstatus",
  ])
    assert.throws(() => endpoint(value));
  for (const value of ["--socket", "group\nstatus", "", "../cluster"])
    assert.throws(() => identifier(value));
});

test("TLS profiles cannot fall back to plaintext", async () => {
  assert.throws(
    () =>
      new Meta("lavik-ctl", { default: { ca: "/ca", allowPlaintext: true } }),
    /all three Meta TLS files/,
  );
  assert.throws(
    () => new Meta("lavik-ctl", { default: { dataCert: "/cert" } }),
    /both Data client/,
  );
  const meta = new Meta("lavik-ctl", {
    default: { ca: "/ca", cert: "/cert", key: "/key", allowPlaintext: true },
  });
  const options = meta.options({ profile: "default" }, "127.0.0.1:7200", true);
  assert.ok(options.includes("--tls-ca"));
  assert.ok(!options.includes("--allow-plaintext-admin"));
  await assert.rejects(
    command("tcp://127.0.0.1:6379", ["PING"], {
      dataCa: "/ca",
      password: "secret",
    }),
    /requires a TLS Data endpoint/,
  );
});
