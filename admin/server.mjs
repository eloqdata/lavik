// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import http from 'node:http';
import net from 'node:net';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { mkdir, readFile, writeFile, chmod, lstat, unlink } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Store } from './store.mjs';
import { Meta, AdminError } from './meta.mjs';
import { Fleet } from './fleet.mjs';

const root = fileURLToPath(new URL('.', import.meta.url));
const equals = (a, b) => {
  const x = Buffer.from(a || ''), y = Buffer.from(b || '');
  return x.length === y.length && timingSafeEqual(x, y);
};

async function body(request) {
  const chunks = [];
  let length = 0;
  for await (const chunk of request) {
    if ((length += chunk.length) > 1024 * 1024) throw new AdminError('Request exceeds 1 MiB', 413);
    chunks.push(chunk);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString() || '{}'); }
  catch { throw new AdminError('Invalid JSON request'); }
}

/** Start HTTP and the mode-0600 lavik-ctl socket over the same Fleet instance. */
export async function start(options = {}) {
  process.umask(0o077);
  const directory = resolve(options.directory || process.env.LAVIK_ADMIN_DATA || '.lavik-admin');
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const stat = await lstat(directory);
  if (!stat.isDirectory() || (stat.mode & 0o077)) throw new Error('Admin data directory must be a private directory (mode 0700)');
  const tokenPath = options.tokenFile || process.env.LAVIK_ADMIN_TOKEN_FILE || join(directory, 'token');
  let token;
  try { token = (await readFile(tokenPath, 'utf8')).trim(); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    token = randomBytes(32).toString('base64url');
    await writeFile(tokenPath, token + '\n', { mode: 0o600, flag: 'wx' });
  }
  if (token.length < 20) throw new Error('Admin access token must contain at least 20 characters');
  const port = options.port ?? Number(process.env.LAVIK_ADMIN_PORT || 4173);
  const host = options.host || process.env.LAVIK_ADMIN_BIND || '127.0.0.1';
  const origins = new Set([process.env.LAVIK_ADMIN_ORIGIN || `http://localhost:${port}`, `http://127.0.0.1:${port}`]);
  const profilesPath = options.profilesFile || process.env.LAVIK_ADMIN_PROFILES;
  const profiles = profilesPath ? JSON.parse(await readFile(profilesPath, 'utf8')) : undefined;
  const socketPath = join(directory, 'admin.sock');
  // Binding this private socket also enforces one process per fleet database.
  // A stale socket is removed only after proving that no owner is listening.
  try {
    if (!(await lstat(socketPath)).isSocket()) throw new Error('Admin socket path is occupied by a non-socket');
    await new Promise((resolve, reject) => {
      const probe = net.connect(socketPath);
      probe.once('connect', () => { probe.destroy(); reject(new Error('Another Lavik Admin owns this data directory')); });
      probe.once('error', error => error.code === 'ECONNREFUSED' ? resolve() : reject(error));
    });
    await unlink(socketPath);
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  const meta = options.meta || new Meta(process.env.LAVIK_CTL || 'lavik-ctl', profiles);
  const store = options.store || new Store(join(directory, 'fleet.sqlite'));
  const fleet = new Fleet(store, meta);
  const sessions = new Map();
  const loginAttempts = new Map();
  const cli = net.createServer(socket => {
    socket.setTimeout(30000, () => socket.destroy());
    let line = '', received = false;
    socket.on('error', () => {});
    socket.on('data', async bytes => {
      if (received) return;
      line += bytes.toString();
      if (Buffer.byteLength(line) > 65536) { socket.destroy(); return; }
      if (!line.includes('\n')) return;
      received = true;
      try {
        const [verb, id, a, b, c, ...extra] = line.trim().split(/\s+/);
        if (extra.length) throw new AdminError('Too many arguments');
        let value;
        if (verb === 'fleet-list' && !id) value = await fleet.clusters();
        else if (verb === 'fleet-add' && id && a && !c) value = await fleet.add({ id, seeds: a.split(','), profile: b || 'default' });
        else if (verb === 'fleet-status' && id && !a) value = await fleet.view(id, true);
        else if (verb === 'fleet-operations' && id && !b) value = await fleet.operations(id, a || '0');
        else if (verb === 'fleet-resume' && id && a && !b) value = await fleet.resume(id, a);
        else if (verb === 'fleet-abandon' && id && a && !b) value = await fleet.abandon(id, a);
        else if (verb === 'fleet-failover' && id && a && !b) value = await fleet.enqueue(id, 'failover', { group: a });
        else if (verb === 'fleet-replica-add' && id && a && b && c) value = await fleet.enqueue(id, 'replica-add', { group: a, node: b, endpoint: c });
        else if (verb === 'fleet-replica-remove' && id && a && b && !c) value = await fleet.enqueue(id, 'replica-remove', { group: a, node: b });
        else throw new AdminError('Use fleet-list, fleet-add NAME SEEDS [PROFILE], fleet-status NAME, fleet-operations NAME [CURSOR], fleet-failover NAME GROUP, fleet-replica-add NAME GROUP NODE ENDPOINT, or fleet-replica-remove NAME GROUP NODE');
        socket.end(`OK ${JSON.stringify(value)}\n`);
      } catch (error) { socket.end(`ERR ${JSON.stringify({ error: error.message })}\n`); }
    });
  });
  await new Promise((resolve, reject) => { cli.once('error', reject); cli.listen(socketPath, resolve); })
    .catch(async error => { await store.close(); throw error; });
  await chmod(socketPath, 0o600);
  const server = http.createServer(async (request, response) => {
    const send = (status, value) => {
      response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      response.end(JSON.stringify(value));
    };
    response.setHeader('X-Content-Type-Options', 'nosniff');
    response.setHeader('Referrer-Policy', 'no-referrer');
    response.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
    try {
      const url = new URL(request.url, 'http://localhost');
      if (!url.pathname.startsWith('/api/')) {
        if (request.method !== 'GET') throw new AdminError('Method not allowed', 405);
        const file = { '/': 'index.html', '/app.js': 'app.js', '/style.css': 'style.css' }[url.pathname];
        if (!file) throw new AdminError('Not found', 404);
        const data = await readFile(join(root, 'public', file));
        response.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html', 'Cache-Control': 'no-cache' });
        response.end(data);
        return;
      }
      if (!['GET', 'POST', 'DELETE'].includes(request.method)) throw new AdminError('Method not allowed', 405);
      if (request.method !== 'GET') {
        if (request.headers['x-lavik-admin'] !== '1' || (request.headers.origin && !origins.has(request.headers.origin)))
          throw new AdminError('Request origin is not allowed', 403);
        if (!request.headers['content-type']?.startsWith('application/json')) throw new AdminError('JSON content type is required', 415);
      }
      const sessionId = /(?:^|;\s*)lavik_session=([^;]+)/.exec(request.headers.cookie || '')?.[1];
      const authenticated = (sessions.get(sessionId) || 0) > Date.now();
      if (url.pathname === '/api/login' && request.method === 'POST') {
        const ip = request.socket.remoteAddress;
        const recent = loginAttempts.get(ip) || { count: 0, since: Date.now() };
        if (Date.now() - recent.since > 300000) { recent.count = 0; recent.since = Date.now(); }
        if (++recent.count > 20) throw new AdminError('Too many login attempts; retry in five minutes', 429);
        loginAttempts.set(ip, recent);
        if (loginAttempts.size > 4096) loginAttempts.delete(loginAttempts.keys().next().value);
        if (!equals((await body(request)).token, token)) throw new AdminError('Invalid access token', 401);
        loginAttempts.delete(ip);
        for (const [key, expiry] of sessions) if (expiry <= Date.now()) sessions.delete(key);
        if (sessions.size >= 1000) throw new AdminError('Too many active sessions', 503);
        const session = randomBytes(32).toString('base64url');
        sessions.set(session, Date.now() + 8 * 3600000);
        response.setHeader('Set-Cookie', `lavik_session=${session}; HttpOnly; SameSite=Strict; Path=/; Max-Age=28800${process.env.LAVIK_ADMIN_ORIGIN?.startsWith('https:') ? '; Secure' : ''}`);
        send(200, { authenticated: true });
        return;
      }
      if (!authenticated) throw new AdminError('Sign in to Lavik Admin', 401);
      if (url.pathname === '/api/session') { send(200, { authenticated: true, profiles: Object.keys(meta.profiles) }); return; }
      if (url.pathname === '/api/logout' && request.method === 'POST') {
        sessions.delete(sessionId);
        response.setHeader('Set-Cookie', 'lavik_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0');
        send(200, { authenticated: false }); return;
      }
      const parts = url.pathname.split('/').filter(Boolean);
      const [_, resource, id, subresource] = parts;
      if (resource !== 'clusters') throw new AdminError('Not found', 404);
      let value;
      if (!id && request.method === 'GET') value = await fleet.clusters();
      else if (!id && request.method === 'POST') value = await fleet.add(await body(request));
      else if (id && !subresource && request.method === 'GET') value = await fleet.view(id, url.searchParams.has('fresh'));
      else if (id && !subresource && request.method === 'DELETE') value = await fleet.forget(id);
      else if (subresource === 'metrics' && request.method === 'GET') value = await fleet.metrics(id);
      else if (subresource === 'operations' && request.method === 'GET') value = await fleet.operations(id, url.searchParams.get('after') || '0');
      else if (subresource === 'operations' && request.method === 'POST') {
        const input = await body(request);
        value = await fleet.enqueue(id, input.kind, input.input || {}, input.requestId);
      } else if (subresource === 'keys' && request.method === 'GET') value = await fleet.keys(id, Object.fromEntries(url.searchParams));
      else if (subresource === 'resume' && request.method === 'POST') value = await fleet.resume(id, (await body(request)).id);
      else if (subresource === 'abandon' && request.method === 'POST') value = await fleet.abandon(id, (await body(request)).id);
      else if (subresource === 'key' && request.method === 'GET') value = await fleet.key(id, url.searchParams.get('id'));
      else if (subresource === 'command' && request.method === 'POST') value = await fleet.send(id, await body(request));
      else if (subresource === 'activity' && request.method === 'GET') value = await fleet.activity(id);
      else throw new AdminError('Not found', 404);
      send(200, value);
    } catch (error) { send(error.status || 500, { error: error.message }); }
  });
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  server.maxHeadersCount = 50;
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, resolve); })
    .catch(async error => {
      await new Promise(resolve => cli.close(resolve));
      await store.close();
      throw error;
    });
  try { await fleet.start(); }
  catch (error) {
    await fleet.stop();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => cli.close(resolve))]);
    await store.close().catch(() => {});
    throw error;
  }
  return { server, fleet, store, tokenPath, socketPath,
    async close() {
      await fleet.stop();
      server.closeIdleConnections();
      await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => cli.close(resolve))]);
      while (fleet.busy.size) await new Promise(resolve => setTimeout(resolve, 50));
      await store.close();
    }
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const app = await start();
  console.log(`Lavik Admin listening on port ${app.server.address().port}\nAccess token: ${app.tokenPath}\nlavik-ctl socket: ${app.socketPath}`);
  let stopping = false;
  for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, async () => {
    if (stopping) return;
    stopping = true;
    await app.close();
  });
}
