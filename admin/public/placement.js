// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.

/** One placement model drives the live form and the server's reviewed plan. */
export function nodeLayout({
  hosts,
  groups = 1,
  followers = 2,
  metaCount = 3,
  placement,
}) {
  const size = hosts.length;
  if (
    !Number.isInteger(size) ||
    size < 1 ||
    size > 64 ||
    ![1, 3, 5].includes(metaCount) ||
    !Number.isInteger(groups) ||
    groups < 1 ||
    groups > 16 ||
    !Number.isInteger(followers) ||
    followers < 0 ||
    followers > 8 ||
    groups * (followers + 1) > 64
  )
    throw new Error(
      "Choose 1–64 hosts, 1/3/5 Meta voters, and at most 64 Data nodes.",
    );
  const nodes = [];
  for (let index = 0; index < metaCount; index++)
    nodes.push({
      name: `meta-${index + 1}`,
      index,
      kind: "meta",
      host: index % size,
    });
  for (let index = 0; index < groups * (followers + 1); index++)
    nodes.push({
      name: `data-${index + 1}`,
      index,
      kind: "data",
      host: index % size,
      group: `group-${Math.floor(index / (followers + 1)) + 1}`,
      role: index % (followers + 1) ? "replica" : "primary",
    });
  if (placement !== undefined) {
    if (
      !placement ||
      typeof placement !== "object" ||
      Array.isArray(placement) ||
      Object.keys(placement).length !== nodes.length ||
      Object.keys(placement).some((name) => !nodes.some((n) => n.name === name))
    )
      throw new Error("Assign a host to every node in the placement table.");
    for (const node of nodes) {
      const host = placement[node.name];
      if (!Number.isInteger(host) || host < 0 || host >= size)
        throw new Error(`Choose a valid host for ${node.name}.`);
      node.host = host;
    }
  }
  return nodes;
}

/** Paste one SSH address and optional cluster IP per line, with shared login settings. */
export function parseHostList(text, defaults = {}) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length || lines.length > 64)
    throw new Error("Paste 1–64 hosts, one per line.");
  return lines.map((line, index) => {
    const [ssh, address, extra] = line.split(/\s+/);
    // Brackets make the SSH port unambiguous for IPv6 addresses.
    const match =
      /^(?:\[([a-fA-F0-9:]+)\]|([a-zA-Z0-9][a-zA-Z0-9.-]*))(?::(\d+))?$/.exec(
        ssh,
      );
    if (!match || extra)
      throw new Error(
        `Line ${
          index + 1
        }: use SSH_HOST[:PORT] followed by an optional cluster IP.`,
      );
    const host = match[1] || match[2];
    const port = Number(match[3] || 22);
    if (port < 1 || port > 65535)
      throw new Error(`Line ${index + 1}: invalid SSH port.`);
    return { ...defaults, host, port, address: address || host };
  });
}
