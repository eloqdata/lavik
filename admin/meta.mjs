// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { execFile } from 'node:child_process';
import { isIP } from 'node:net';

export class AdminError extends Error {
  constructor(message, status = 400) { super(message); this.status = status; }
}

/** Keep identifiers separate from command-line flags and the one-line wire grammar. */
export function identifier(value, label = 'identifier') {
  if (typeof value !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(value))
    throw new AdminError(`Invalid ${label}`);
  return value;
}

export function endpoint(value) {
  if (typeof value !== 'string') throw new AdminError('A numeric IP:port is required');
  const match = /^(?:\[([^\]]+)\]|([^:]+)):(\d+)$/.exec(value);
  if (!match || !isIP(match[1] || match[2]) || +match[3] < 1 || +match[3] > 65535)
    throw new AdminError('Use a numeric IPv4:port or [IPv6]:port endpoint');
  return { host: match[1] || match[2], port: +match[3] };
}

/** Reuse lavik-ctl's TLS verification, leader discovery and bounded transport. */
export class Meta {
  constructor(binary, profiles = { default: { allowPlaintext: true } }) {
    for (const [name, profile] of Object.entries(profiles)) {
      identifier(name, 'profile name');
      if (!profile || typeof profile !== 'object' || Array.isArray(profile))
        throw new AdminError(`Invalid connection profile: ${name}`);
      const metaTls = [profile.ca, profile.cert, profile.key].filter(Boolean).length;
      if (metaTls !== 0 && metaTls !== 3)
        throw new AdminError(`Profile ${name} requires all three Meta TLS files`);
      if (!!profile.dataCert !== !!profile.dataKey)
        throw new AdminError(`Profile ${name} requires both Data client certificate and key`);
    }
    this.binary = binary;
    this.profiles = profiles;
    this.inflight = 0;
  }
  profile(cluster) {
    const profile = this.profiles[cluster.profile];
    if (!profile) throw new AdminError(`Unknown connection profile: ${cluster.profile}`);
    return profile;
  }
  options(cluster, seed, discovery = false) {
    const p = this.profile(cluster);
    endpoint(seed);
    const args = ['--addr', seed, '--timeout-ms', '8000'];
    if (p.ca && p.cert && p.key) args.push('--tls-ca', p.ca, '--tls-cert', p.cert, '--tls-key', p.key);
    else if (p.allowPlaintext !== true) throw new AdminError('Connection profile requires Meta mTLS');
    else if (discovery) args.push('--allow-plaintext-admin');
    return args;
  }
  async exec(args) {
    if (this.inflight >= 32) throw new AdminError('Management service is busy; retry shortly', 503);
    this.inflight++;
    try {
      return await new Promise(resolve => {
        // No shell: values from a DBA can never become process arguments outside
        // this explicit argv. A killed mutation remains uncertain, never retried.
        execFile(this.binary, args, { timeout: 12000, maxBuffer: 8 * 1024 * 1024 },
          (error, stdout, stderr) => resolve({ code: error ? error.code : 0, stdout: stdout.trim(), stderr: stderr.trim() }));
      });
    } finally { this.inflight--; }
  }
  async status(cluster) {
    let last;
    for (const seed of JSON.parse(cluster.seeds)) {
      const result = await this.exec(['cluster-status', ...this.options(cluster, seed, true), '--json']);
      try {
        const status = JSON.parse(result.stdout);
        if (status.capture) return status;
        last = status.status_explanation;
      } catch { last = result.stderr || 'Meta returned an invalid status response'; }
    }
    throw new AdminError(last || 'No Meta seed is reachable', 503);
  }
  async leader(cluster) {
    const status = await this.status(cluster);
    const leader = status.meta_members.find(member => member.leader)?.ctl_endpoint;
    if (!leader) throw new AdminError('Meta has no available leader', 503);
    return leader.replace(/^(tcp|tls):\/\//, '');
  }
  async command(cluster, args, leader) {
    const result = await this.exec([...this.options(cluster, leader || await this.leader(cluster)), ...args]);
    if (result.code !== 0 || !result.stdout.startsWith('OK')) {
      const error = new AdminError(result.stdout || result.stderr || 'Meta connection failed', 502);
      error.uncertain = !result.stdout.startsWith('ERR');
      throw error;
    }
    return result.stdout;
  }
  async nodes(cluster, status) {
    const leader = await this.leader(cluster);
    const nodes = [];
    // Bound fan-out for large fleets; do not spawn a process per node at once.
    for (let i = 0; i < status.data_nodes.length; i += 8) {
      nodes.push(...await Promise.all(status.data_nodes.slice(i, i + 8).map(async node => {
        try {
          const reply = await this.command(cluster, ['getnode', node.node_id], leader);
          const address = /(?:^| )endpoints=([^ ]+)/.exec(reply)?.[1]?.split(',')[0];
          return { ...node, endpoint: address || null };
        } catch (error) { return { ...node, endpoint: null, error: error.message }; }
      })));
    }
    return nodes;
  }
}
