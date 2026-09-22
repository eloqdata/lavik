// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
const $ = (selector) => document.querySelector(selector);
const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const short = (value) =>
  value ? `${value.slice(0, 8)}…${value.slice(-4)}` : "—";
const number = (value) =>
  value === null || value === undefined
    ? "—"
    : new Intl.NumberFormat("en", { maximumFractionDigits: 1 }).format(value);
const bytes = (value) =>
  value < 1024
    ? `${number(value)} B`
    : value < 1048576
    ? `${number(value / 1024)} KiB`
    : value < 1073741824
    ? `${number(value / 1048576)} MiB`
    : `${number(value / 1073741824)} GiB`;
const state = {
  clusters: [],
  cluster: null,
  page: "fleet",
  views: {},
  errors: {},
  history: [],
  samples: [],
  profiles: [],
  keys: [],
  cursor: null,
  keyPattern: "*",
  metrics: null,
  generation: 0,
};
const navigation = [
  ["dashboard", "◫", "Dashboard"],
  ["topology", "◇", "Topology"],
  ["keys", "⌕", "Key browser"],
  ["console", "›_", "Send command"],
  ["activity", "≋", "Activity"],
  ["operations", "◷", "Operations"],
];

async function api(path, method = "GET", data) {
  const response = await fetch(`/api${path}`, {
    method,
    headers:
      method === "GET"
        ? {}
        : { "Content-Type": "application/json", "X-Lavik-Admin": "1" },
    body: data === undefined ? undefined : JSON.stringify(data),
  });
  const value = await response.json();
  if (!response.ok) {
    if (response.status === 401 && path !== "/login") login();
    throw new Error(value.error || "Request failed");
  }
  return value;
}
function toast(message) {
  $("#toast").textContent = message;
  $("#toast").classList.add("visible");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => $("#toast").classList.remove("visible"), 6000);
}
const badge = (label, kind = "") =>
  `<span class="badge ${kind}">${escape(label)}</span>`;
const health = (view) =>
  !view
    ? badge("Connecting", "neutral")
    : view.status.cluster_ready
    ? badge("● Healthy")
    : badge(
        view.status.cluster_state === "uninitialized"
          ? "Not initialized"
          : "Needs attention",
        "warn",
      );
const empty = (title, text, action = "") =>
  `<div class="empty"><div class="cluster-icon">◇</div><h2>${escape(
    title,
  )}</h2><p>${escape(text)}</p>${action}</div>`;
const table = (heads, rows) =>
  `<div class="table-wrap"><table><thead><tr>${heads
    .map((h) => `<th>${escape(h)}</th>`)
    .join("")}</tr></thead><tbody>${rows.join("")}</tbody></table></div>`;
const stat = (label, value, note) =>
  `<div class="stat"><div class="stat-label">${escape(
    label,
  )}</div><div class="stat-value number">${escape(
    value,
  )}</div><div class="stat-note">${escape(note)}</div></div>`;

function login() {
  state.generation++;
  state.page = "login";
  $(
    "#app",
  ).innerHTML = `<div class="login"><div class="login-intro"><div class="brand"><div class="mark">L</div>lavik <span>ADMIN</span></div><div class="eyebrow">THE HOME FOR YOUR CLUSTERS</div><h1>A familiar view.<br>A faster database.</h1><p>Understand your fleet, explore your data, and manage Lavik with confidence.</p></div><div class="login-form"><form id="login-form"><h2>Welcome to Lavik Admin</h2><p class="muted">Sign in to your cluster workspace.</p><label for="access-token">Access token</label><input id="access-token" name="token" type="password" autocomplete="current-password" required placeholder="Enter your Admin access token"><button class="primary" type="submit">Open workspace →</button><p id="login-error" class="error" role="alert"></p><small>Your token is in the Admin data directory’s <code>token</code> file. The same workspace is available from <code>lavik-ctl</code>.</small></form></div></div>`;
  $("#login-form").onsubmit = async (event) => {
    event.preventDefault();
    try {
      await api("/login", "POST", { token: $("#access-token").value });
      await initialize();
    } catch (error) {
      $("#login-error").textContent = error.message;
    }
  };
}
async function initialize() {
  const session = await api("/session");
  state.profiles = session.profiles;
  state.clusters = await api("/clusters");
  navigate("fleet");
}
function shell() {
  const cluster = state.clusters.find((c) => c.id === state.cluster);
  $(
    "#app",
  ).innerHTML = `<div class="layout"><aside class="sidebar"><div class="brand"><div class="mark">L</div>lavik <span>ADMIN</span></div><div class="nav"><button data-nav="fleet" class="${
    state.page === "fleet" ? "active" : ""
  }"><span class="symbol" aria-hidden="true">▦</span>All clusters</button></div><label for="cluster-select">Workspace</label><select id="cluster-select" aria-label="Select cluster"><option value="">Choose a cluster</option>${state.clusters
    .map(
      (c) =>
        `<option value="${escape(c.id)}" ${
          state.cluster === c.id ? "selected" : ""
        }>${escape(c.name)}</option>`,
    )
    .join(
      "",
    )}</select><label>Manage cluster</label><nav class="nav" aria-label="Cluster navigation">${navigation
    .map(
      ([page, icon, label]) =>
        `<button data-nav="${page}" class="${
          state.page === page ? "active" : ""
        }" ${
          !state.cluster ? "disabled" : ""
        }><span class="symbol" aria-hidden="true">${icon}</span>${label}</button>`,
    )
    .join(
      "",
    )}</nav><div class="sidefooter">One workspace.<br>Browser and command line.<br><button class="small" id="signout">Sign out</button></div></aside><main><header class="topbar"><span>Workspace <span class="muted"> / </span> <strong>${escape(
    state.page === "fleet" ? "All clusters" : cluster?.name || "Cluster",
  )}</strong></span><span class="live"><i class="dot"></i> <span id="updated">Live · every 5 seconds</span></span></header><div class="content" id="content"></div></main></div>`;
  document
    .querySelectorAll("[data-nav]")
    .forEach((button) => (button.onclick = () => navigate(button.dataset.nav)));
  $("#cluster-select").onchange = (event) => {
    state.cluster = event.target.value || null;
    state.samples = [];
    state.metrics = null;
    navigate(state.cluster ? "dashboard" : "fleet");
  };
  $("#signout").onclick = async () => {
    await api("/logout", "POST", {});
    login();
  };
}
function navigate(page, cluster) {
  if (cluster) {
    state.cluster = cluster;
    state.samples = [];
    state.metrics = null;
  }
  state.page = page;
  state.generation++;
  state.forceRefresh = true;
  shell();
  $("#content").innerHTML =
    '<div class="spinner">Connecting to your cluster…</div>';
  refresh(true).catch((error) => showError(error));
}
function showError(error) {
  if ($("#content"))
    $("#content").innerHTML = `${heading(
      "Connection unavailable",
      "Your saved connection is retained.",
    )}<div class="banner error" role="alert">${escape(
      error.message,
    )}</div><button id="retry">Try again</button>`;
  if ($("#retry")) $("#retry").onclick = () => refresh(true).catch(showError);
}
function heading(title, subtitle, actions = "") {
  return `<div class="heading"><div><div class="eyebrow">LAVIK ADMIN</div><h1>${escape(
    title,
  )}</h1><p>${escape(
    subtitle,
  )}</p></div><div class="actions">${actions}</div></div>`;
}
async function refresh(force = false) {
  if (state.page === "login" || refresh.busy) return;
  force ||= state.forceRefresh;
  state.forceRefresh = false;
  refresh.busy = true;
  const generation = state.generation;
  try {
    if (state.page === "fleet") {
      state.clusters = await api("/clusters");
      // Connection failures remain visible independently; one unreachable
      // cluster does not suppress healthy clusters in the fleet.
      await Promise.all(
        state.clusters.map(async (cluster) => {
          try {
            state.views[cluster.id] = await api(`/clusters/${cluster.id}`);
            delete state.errors[cluster.id];
          } catch (error) {
            state.errors[cluster.id] = error.message;
            delete state.views[cluster.id];
          }
        }),
      );
      if (generation === state.generation) renderFleet();
      return;
    }
    const clusterId = state.cluster;
    const view = await api(`/clusters/${clusterId}${force ? "?fresh=1" : ""}`);
    if (generation !== state.generation) return;
    state.views[clusterId] = view;
    if (state.page === "dashboard") {
      state.metrics = await api(`/clusters/${clusterId}/metrics`);
      if (generation !== state.generation) return;
      const valid = state.metrics.nodes.filter((n) => !n.error);
      state.samples.push(
        valid.some((n) => n.ops !== null)
          ? valid.reduce((sum, n) => sum + (n.ops || 0), 0)
          : null,
      );
      state.samples = state.samples.slice(-60);
      renderDashboard(view);
    } else if (state.page === "topology") renderTopology(view);
    else if (state.page === "operations") {
      const operations = await api(`/clusters/${clusterId}/operations`);
      if (generation === state.generation) renderOperations(operations);
    } else if (force && state.page === "keys") renderKeys();
    else if (force && state.page === "console") renderConsole(view);
    else if (force && state.page === "activity") {
      const activity = await api(`/clusters/${clusterId}/activity`);
      if (generation === state.generation) renderActivity(activity);
    }
    if ($("#updated"))
      $("#updated").textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } finally {
    refresh.busy = false;
    if (state.forceRefresh)
      queueMicrotask(() => refresh(true).catch(showError));
  }
}
function renderFleet() {
  const healthy = state.clusters.filter(
    (c) => state.views[c.id]?.status.cluster_ready,
  ).length;
  const nodes = state.clusters.reduce(
    (count, c) =>
      count + (state.views[c.id]?.nodes.filter((n) => n.group_id).length || 0),
    0,
  );
  $("#content").innerHTML = `${heading(
    "Your clusters",
    "One place to monitor, explore, and manage your Lavik fleet.",
    '<button class="primary" id="connect">＋ Connect cluster</button>',
  )}<div class="stats">${stat(
    "Connected clusters",
    state.clusters.length,
    "Shared with lavik-ctl",
  )}${stat("Healthy clusters", healthy, "Observed by Meta")}${stat(
    "Data nodes",
    nodes,
    "Across connected clusters",
  )}${stat(
    "Needs attention",
    state.clusters.length - healthy,
    "Unavailable or not ready",
  )}</div>${
    state.clusters.length
      ? `<div class="cards">${state.clusters
          .map((c) => {
            const view = state.views[c.id];
            return `<article class="cluster-card"><div class="row"><div class="cluster-icon">◇</div>${
              state.errors[c.id] ? badge("Unavailable", "bad") : health(view)
            }</div><h2><button class="link" data-open="${escape(
              c.id,
            )}">${escape(
              c.name,
            )}</button></h2><div class="endpoint mono">${escape(
              JSON.parse(c.seeds)[0],
            )}</div><div class="cluster-size"><div><strong>${
              view?.status.groups.length ?? "—"
            }</strong>Primary groups</div><div><strong>${
              view?.nodes.filter((n) => n.group_id).length ?? "—"
            }</strong>Data nodes</div><div><strong>${
              view?.status.meta_members.length ?? "—"
            }</strong>Meta members</div></div>${
              state.errors[c.id]
                ? `<p class="sub wrap">${escape(state.errors[c.id])}</p>`
                : ""
            }<footer><small>${escape(
              c.profile,
            )} profile</small><button class="link" data-open="${escape(
              c.id,
            )}">Open cluster →</button></footer></article>`;
          })
          .join("")}</div>`
      : `<div class="panel">${empty(
          "Your workspace is ready",
          "Connect a Lavik Meta endpoint to see cluster health, topology, data, and operations.",
          '<button class="primary" id="connect-empty">Connect your first cluster</button>',
        )}</div>`
  }<div class="banner">Connections registered from <code>lavik-ctl</code> appear here automatically. Cluster changes use the same Meta state and operation IDs.</div>`;
  $("#connect").onclick = connectDialog;
  if ($("#connect-empty")) $("#connect-empty").onclick = connectDialog;
  document
    .querySelectorAll("[data-open]")
    .forEach(
      (button) =>
        (button.onclick = () => navigate("dashboard", button.dataset.open)),
    );
}
function readiness(view) {
  if (view.status.cluster_ready) return "";
  return `<div class="banner warn"><strong>${escape(
    view.status.status_explanation,
  )}</strong><br>${escape(view.status.next_action)}${view.status.blockers
    .slice(0, 5)
    .map((b) => `<div>${escape(b.scope)} · ${escape(b.detail || b.code)}</div>`)
    .join("")}</div>`;
}
function chart(samples) {
  const valid = samples.filter((n) => n !== null);
  if (valid.length < 2)
    return empty(
      "Collecting throughput",
      "The chart starts after two samples.",
    );
  const max = Math.max(1, ...valid);
  const points = valid
    .map(
      (n, i) =>
        `${(i / Math.max(valid.length - 1, 1)) * 700},${155 - (n / max) * 135}`,
    )
    .join(" ");
  return `<svg class="chart" viewBox="0 0 700 175" preserveAspectRatio="none" role="img" aria-label="Command throughput over recent samples"><path class="chart-grid" d="M0 20H700 M0 65H700 M0 110H700 M0 155H700"/><polygon class="chart-area" points="0,175 ${points} 700,175"/><polyline class="chart-line" points="${points}"/></svg><div class="chart-foot"><span>${
    valid.length
  } live samples</span><span>Peak ${number(
    max,
  )} commands/s</span><span>Now</span></div>`;
}
function renderDashboard(view) {
  const valid = state.metrics.nodes.filter((n) => !n.error);
  const sum = (field) => valid.reduce((n, item) => n + (item[field] || 0), 0);
  const owners = new Set(view.status.groups.map((g) => g.owner_node_id));
  const keys = valid
    .filter((n) => owners.has(n.node_id))
    .reduce((n, item) => n + item.keys, 0);
  $("#content").innerHTML = `${heading(
    view.name,
    "Cluster health and performance, at a glance.",
    `${health(view)}<button id="open-topology">Manage topology →</button>`,
  )}${readiness(view)}${
    view.status.cluster_state === "uninitialized"
      ? '<div class="banner">This Meta cluster is ready for initialization. <button class="link" id="initialize">Create its data cluster →</button></div>'
      : ""
  }<div class="stats">${stat(
    "Command throughput",
    valid.some((n) => n.ops !== null) ? `${number(sum("ops"))}/s` : "—",
    "Across available nodes",
  )}${stat(
    "Memory used",
    bytes(sum("memory")),
    "Sum of reported node memory",
  )}${stat("Keys", number(keys), "Primary nodes only")}${stat(
    "Connected clients",
    number(sum("clients")),
    "Across available nodes",
  )}</div><div class="split"><section class="panel"><div class="row"><h2>Command throughput</h2><small>Live · 5-second samples</small></div>${chart(
    state.samples,
  )}</section><section class="panel"><h2>Cluster at a glance</h2>${table(
    ["Component", "Status"],
    [
      `<tr><td>Meta control plane</td><td>${badge(
        view.status.meta_available ? "Available" : "Unavailable",
        view.status.meta_available ? "" : "bad",
      )}</td></tr>`,
      `<tr><td>Topology</td><td>${badge(
        view.status.topology_converged ? "Converged" : "Converging",
        view.status.topology_converged ? "" : "warn",
      )}</td></tr>`,
      `<tr><td>Primary groups</td><td>${view.status.groups.length}</td></tr>`,
      `<tr><td>Data nodes</td><td>${
        view.nodes.filter((n) => n.group_id).length
      }</td></tr>`,
    ],
  )}</section></div><section class="panel"><div class="row"><h2>Node overview</h2><small>${
    state.metrics.nodes.filter((n) => n.error).length
      ? "Some node metrics are unavailable"
      : "Latest reported values"
  }</small></div>${table(
    ["Node", "Group / role", "Memory", "Clients", "Commands/s", "Population"],
    view.nodes
      .filter((n) => n.group_id)
      .map((n) => {
        const metric = state.metrics.nodes.find((m) => m.node_id === n.node_id);
        return `<tr><td><span class="mono">${escape(
          n.endpoint || short(n.node_id),
        )}</span><span class="sub mono">${escape(
          short(n.node_id),
        )}</span></td><td>${escape(n.group_id)}<span class="sub">${
          owners.has(n.node_id) ? "Primary" : "Replica"
        }</span></td><td>${
          metric?.error ? "Unavailable" : bytes(metric?.memory || 0)
        }</td><td>${number(metric?.clients)}</td><td>${number(
          metric?.ops,
        )}</td><td>${badge(
          n.population_current ? "Current" : "Synchronizing",
          n.population_current ? "" : "warn",
        )}</td></tr>`;
      }),
  )}</section>`;
  $("#open-topology").onclick = () => navigate("topology");
  if ($("#initialize")) $("#initialize").onclick = createDialog;
}
function nodeBox(node, owner, group) {
  return `<div class="node-box ${
    owner ? "primary-node" : ""
  }"><div class="row">${badge(
    owner ? "PRIMARY" : "REPLICA",
    owner ? "" : "neutral",
  )}${badge(
    node.health_fresh ? "Connected" : "Waiting",
    node.health_fresh ? "" : "warn",
  )}</div><h3 class="mono">${escape(
    node.endpoint || "Endpoint unavailable",
  )}</h3><div class="node-id mono">${escape(
    node.node_id,
  )}</div><footer><small>${
    node.population_current ? "Population current" : "Synchronizing population"
  }</small>${
    owner
      ? ""
      : `<button class="small danger" data-remove="${escape(
          node.node_id,
        )}" data-group="${escape(group)}">Remove</button>`
  }</footer></div>`;
}
function renderTopology(view) {
  $("#content").innerHTML = `${heading(
    "Cluster topology",
    "Primary groups, replicas, and slot ownership.",
    '<button id="refresh-topology">↻ Refresh</button>',
  )}${readiness(
    view,
  )}<div class="banner">Add already-running nodes as replicas or remove replicas to adjust redundancy. Adding primary groups and redistributing key slots requires a data-migration workflow and is not available yet.</div>${
    view.status.groups.length
      ? view.status.groups
          .map((g) => {
            const nodes = view.nodes.filter((n) => n.group_id === g.group_id);
            const primary = nodes.find((n) => n.node_id === g.owner_node_id);
            const replicas = nodes.filter((n) => n.node_id !== g.owner_node_id);
            const slots = view.status.slot_ranges
              .filter((r) => r.group_id === g.group_id)
              .map((r) => `${r.first}–${r.last}`)
              .join(", ");
            return `<section class="group-card"><div class="group-header"><div><h2>${escape(
              g.group_id,
            )}</h2><small>Slots ${escape(slots)} · Term ${escape(g.term)} · ${
              replicas.length
            } replica${
              replicas.length === 1 ? "" : "s"
            }</small></div><div class="actions">${badge(
              g.serving_ready ? "Serving" : "Not serving",
              g.serving_ready ? "" : "warn",
            )}<button class="small" data-failover="${escape(g.group_id)}" ${
              !replicas.length ? "disabled" : ""
            }>Switch primary</button><button class="small primary" data-add="${escape(
              g.group_id,
            )}">＋ Add replica</button></div></div><div class="node-tree">${
              primary
                ? nodeBox(primary, true, g.group_id)
                : '<div class="node-box">No current primary</div>'
            }<div class="replicas">${
              replicas.length
                ? replicas.map((n) => nodeBox(n, false, g.group_id)).join("")
                : '<div class="node-box muted">No replicas. Add a replica to improve availability.</div>'
            }</div></div></section>`;
          })
          .join("")
      : `<div class="panel">${empty(
          "No data groups yet",
          "Initialize the data cluster with the nodes you have already started.",
          '<button class="primary" id="initialize">Initialize cluster</button>',
        )}</div>`
  }<section class="panel"><h2>Meta members</h2>${table(
    ["Member", "Admin endpoint", "Role"],
    view.status.meta_members.map(
      (m) =>
        `<tr><td>Meta ${escape(m.server_id)}</td><td class="mono">${escape(
          m.ctl_endpoint,
        )}</td><td>${badge(
          m.leader ? "Leader" : "Follower",
          m.leader ? "" : "neutral",
        )}</td></tr>`,
    ),
  )}</section>`;
  $("#refresh-topology").onclick = () => refresh(true).catch(showError);
  if ($("#initialize")) $("#initialize").onclick = createDialog;
  document
    .querySelectorAll("[data-add]")
    .forEach((b) => (b.onclick = () => replicaDialog(b.dataset.add)));
  document
    .querySelectorAll("[data-remove]")
    .forEach(
      (b) =>
        (b.onclick = () =>
          operationDialog(
            "replica-remove",
            { group: b.dataset.group, node: b.dataset.remove },
            "Remove replica",
            "This node will leave the group. Its data files remain on the host. The primary continues serving; redundancy decreases.",
          )),
    );
  document
    .querySelectorAll("[data-failover]")
    .forEach(
      (b) =>
        (b.onclick = () =>
          operationDialog(
            "failover",
            { group: b.dataset.failover },
            "Switch primary",
            "Meta selects an eligible replica, drains writes, and coordinates a controlled failover. Follow the shared operation until cutover completes.",
          )),
    );
}
function renderOperations(value) {
  const localById = new Map(
    value.jobs.map((j) => [j.meta_operation_id || j.id, j]),
  );
  const rows = [
    ...value.meta.map((op) => ({ ...op, local: localById.get(op.id) })),
    ...value.jobs
      .filter(
        (j) => !value.meta.some((o) => o.id === (j.meta_operation_id || j.id)),
      )
      .map((j) => ({
        id: j.id,
        kind: j.kind,
        state: j.state,
        phase: j.step,
        detail: j.detail,
        local: j,
      })),
  ];
  $("#content").innerHTML = `${heading(
    "Operations",
    "Follow cluster changes initiated from the browser or command line.",
    '<button id="refresh-ops">↻ Refresh</button>',
  )}${
    value.error
      ? `<div class="banner warn">Meta operation history is unavailable: ${escape(
          value.error,
        )}. Saved Admin requests are shown below.</div>`
      : ""
  }<div class="banner">The Meta journal is shared with <code>lavik-ctl getop</code>. “Submitted” confirms acceptance; completion and current cluster readiness are separate checks.</div><section class="panel">${
    rows.length
      ? table(
          ["Operation", "Kind", "Status", "Progress / result"],
          rows
            .reverse()
            .map(
              (op) =>
                `<tr><td class="mono">${escape(op.id)}${
                  op.local
                    ? '<span class="sub">Lavik Admin / fleet CLI</span>'
                    : '<span class="sub">Meta journal</span>'
                }</td><td>${escape(op.kind)}</td><td>${badge(
                  op.local?.state === "uncertain" ? "Uncertain" : op.state,
                  ["completed"].includes(op.state)
                    ? ""
                    : ["failed", "aborted"].includes(op.state)
                    ? "bad"
                    : "warn",
                )}</td><td class="wrap">${escape(
                  op.local?.detail || op.detail || op.phase || "Accepted",
                )}<span class="sub">${escape(
                  op.local?.step || op.phase || "",
                )}</span>${
                  op.local?.state === "uncertain" && op.local.kind !== "create"
                    ? `<div class="actions"><button class="small" data-resume="${escape(
                        op.local.id,
                      )}">Retry original request</button>${
                        op.local.kind.startsWith("replica-")
                          ? `<button class="small danger" data-abandon="${escape(
                              op.local.id,
                            )}">Abandon request</button>`
                          : ""
                      }</div>`
                    : ""
                }</td></tr>`,
            ),
        )
      : empty(
          "No operations to show",
          "Cluster creation, failover, and replica changes appear here.",
        )
  } ${
    value.next ? `<button id="older-ops">Next journal page</button>` : ""
  }</section>`;
  $("#refresh-ops").onclick = () => refresh(true).catch(showError);
  document.querySelectorAll("[data-resume], [data-abandon]").forEach(
    (button) =>
      (button.onclick = () => {
        const abandon = !!button.dataset.abandon;
        const id = button.dataset.abandon || button.dataset.resume;
        dialog(
          abandon ? "Abandon replica request" : "Retry original request",
          abandon
            ? "Committed membership changes remain in place. This stops this request without undoing them."
            : "The original operation ID, deadline, and reviewed membership revision are retained.",
          `<p class="mono">${escape(id)}</p>`,
          async () => {
            await api(
              `/clusters/${state.cluster}/${abandon ? "abandon" : "resume"}`,
              "POST",
              { id },
            );
            toast(abandon ? "Request abandoned" : "Original request queued");
            state.forceRefresh = true;
          },
          abandon ? "Abandon request" : "Retry request",
        );
      }),
  );
  if ($("#older-ops"))
    $("#older-ops").onclick = async () =>
      renderOperations(
        await api(`/clusters/${state.cluster}/operations?after=${value.next}`),
      );
}
function renderKeys() {
  state.keys = [];
  state.cursor = null;
  $("#content").innerHTML = `${heading(
    "Key browser",
    "Search and inspect keys across every primary group.",
  )}<form id="search-keys" class="search-row"><input aria-label="Key pattern" id="key-pattern" placeholder="Search with a pattern, e.g. user:*" value="${escape(
    state.keyPattern,
  )}"><button class="primary">Search keys</button><button type="button" id="new-key">＋ New string</button></form><div class="key-layout"><section class="panel"><div class="row"><h2>Keys</h2><small id="key-count">Ready to scan</small></div><div id="key-list">${empty(
    "Explore your data",
    "Enter a key pattern and start a cursor-based scan. Scans are incremental, not a consistent snapshot.",
  )}</div><button id="more-keys" hidden>Load more</button></section><section class="panel" id="key-detail">${empty(
    "Select a key",
    "View its type, value, time to live, and owning node.",
  )}</section></div>`;
  $("#search-keys").onsubmit = async (event) => {
    event.preventDefault();
    state.keys = [];
    state.cursor = null;
    state.keyPattern = $("#key-pattern").value;
    await scanKeys();
  };
  $("#more-keys").onclick = scanKeys;
  $("#new-key").onclick = () => stringDialog();
}
async function scanKeys() {
  const generation = state.generation;
  try {
    const result = await api(
      `/clusters/${state.cluster}/keys?pattern=${encodeURIComponent(
        state.keyPattern,
      )}${state.cursor ? `&cursor=${encodeURIComponent(state.cursor)}` : ""}`,
    );
    if (generation !== state.generation) return;
    state.keys = [
      ...new Map(
        [...state.keys, ...result.keys].map((k) => [k.id, k]),
      ).values(),
    ];
    state.cursor = result.cursor;
    $("#key-count").textContent = `${state.keys.length} found${
      result.cursor ? " · more to scan" : " · scan finished"
    }`;
    $("#key-list").innerHTML = state.keys.length
      ? table(
          ["Key", "Primary node"],
          state.keys.map(
            (k) =>
              `<tr class="clickable" data-key="${escape(
                k.id,
              )}"><td><button class="link mono" data-key-button="${escape(
                k.id,
              )}">${escape(
                typeof k.name === "string"
                  ? k.name
                  : `[binary · ${k.name.bytes} bytes]`,
              )}</button></td><td class="mono">${escape(
                short(k.node),
              )}</td></tr>`,
          ),
        )
      : empty(
          "No matching keys in this page",
          result.cursor
            ? "Continue scanning to search the remaining keyspace."
            : "Try a different pattern or create a string key.",
        );
    $("#more-keys").hidden = !result.cursor;
    document
      .querySelectorAll("[data-key]")
      .forEach((row) => (row.onclick = () => inspectKey(row.dataset.key)));
  } catch (error) {
    toast(error.message);
  }
}
async function inspectKey(id) {
  const generation = state.generation;
  try {
    const key = await api(
      `/clusters/${state.cluster}/key?id=${encodeURIComponent(id)}`,
    );
    if (generation !== state.generation) return;
    const listed = state.keys.find((k) => k.id === id);
    $("#key-detail").innerHTML = `<div class="row"><h2>Key details</h2>${badge(
      key.type,
      "neutral",
    )}</div><h3 class="mono wrap">${escape(
      typeof listed?.name === "string" ? listed.name : "Binary key",
    )}</h3><p class="muted">Slot ${key.slot} · ${
      key.ttl === "-1"
        ? "No expiry"
        : key.ttl === "-2"
        ? "Key no longer exists"
        : `${number(+key.ttl / 1000)} seconds to live`
    }</p><pre class="key-value">${escape(
      JSON.stringify(key.value, null, 2),
    )}</pre>${
      key.bounded
        ? '<p class="muted">Collection preview is limited to the first page / 100 entries. Use Send command for another cursor or range.</p>'
        : ""
    }<div class="actions">${
      key.type === "string" &&
      typeof key.value === "string" &&
      typeof listed?.name === "string"
        ? '<button id="edit-key">Edit value</button>'
        : ""
    }${
      typeof listed?.name === "string" && key.type !== "none"
        ? '<button class="danger" id="delete-key">Delete key</button>'
        : ""
    }</div>`;
    if ($("#edit-key"))
      $("#edit-key").onclick = () => stringDialog(listed.name, key.value);
    if ($("#delete-key"))
      $("#delete-key").onclick = () =>
        dialog(
          "Delete key",
          "This deletes the selected key from the cluster.",
          `<p class="mono wrap">${escape(listed.name)}</p>`,
          async () => {
            await api(`/clusters/${state.cluster}/command`, "POST", {
              args: ["DEL", listed.name],
              node: key.node,
              confirm: true,
            });
            await inspectKey(id);
            toast("Key deleted");
          },
          "Delete key",
        );
  } catch (error) {
    toast(error.message);
  }
}
function stringDialog(name = "", value = "") {
  const editing = !!name;
  dialog(
    editing ? "Edit string value" : "Create a string key",
    editing
      ? "The existing expiry is preserved."
      : "The key will be stored without an expiry.",
    `<label for="string-key">Key</label><input id="string-key" value="${escape(
      name,
    )}" ${
      editing ? "readonly" : ""
    } required><label for="string-value">Value</label><textarea id="string-value" rows="6">${escape(
      value,
    )}</textarea>`,
    async () => {
      const args = ["SET", $("#string-key").value, $("#string-value").value];
      if (editing) args.push("KEEPTTL");
      else args.push("NX");
      const result = await api(`/clusters/${state.cluster}/command`, "POST", {
        args,
        confirm: true,
      });
      if (result.reply === null)
        throw new Error("That key already exists; inspect it before editing");
      toast("String saved");
    },
    "Save string",
  );
}
function parseCommand(line) {
  const args = [];
  let token = "",
    quote = null,
    started = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === "\\") {
      if (++i >= line.length) throw new Error("Incomplete escape");
      token += { n: "\n", r: "\r", t: "\t" }[line[i]] ?? line[i];
      started = true;
    } else if (quote) {
      if (c === quote) quote = null;
      else token += c;
    } else if (c === '"' || c === "'") {
      quote = c;
      started = true;
    } else if (/\s/.test(c)) {
      if (started) {
        args.push(token);
        token = "";
        started = false;
      }
    } else {
      token += c;
      started = true;
    }
  }
  if (quote) throw new Error("Close the quoted argument");
  if (started) args.push(token);
  return args;
}
function renderConsole(view) {
  $("#content").innerHTML = `${heading(
    "Send command",
    "A familiar command line, connected to your Lavik cluster.",
  )}<section class="panel"><form id="command-form"><div class="row"><h2>Command console</h2><select id="command-node" aria-label="Target node">${view.nodes
    .filter((n) => n.group_id)
    .map(
      (n) =>
        `<option value="${escape(n.node_id)}">${escape(
          n.endpoint || short(n.node_id),
        )} · ${escape(n.group_id)}</option>`,
    )
    .join(
      "",
    )}</select></div><div class="search-row"><input id="command-input" class="console-input" aria-label="Command" list="commands" placeholder='GET "user:1001"' autocomplete="off"><datalist id="commands">${[
    "PING",
    "INFO",
    "GET",
    "SET",
    "SCAN",
    "HGETALL",
    "LRANGE",
    "ZRANGE",
    "SLOWLOG GET",
    "CLUSTER INFO",
    "DBSIZE",
    "TYPE",
    "TTL",
    "DEL",
  ]
    .map((c) => `<option value="${c}">`)
    .join(
      "",
    )}</datalist><button class="primary">Run command →</button></div><p class="toolbar-note">Quote arguments containing spaces. Data writes require confirmation. Cluster management is available in Topology.</p></form><div class="history" id="command-history"></div><pre class="terminal" id="command-output">Connected workspace: ${escape(
    view.name,
  )}\nReady for a command.</pre></section>`;
  const run = async (args, line) => {
    try {
      const result = await api(`/clusters/${state.cluster}/command`, "POST", {
        args,
        node: $("#command-node").value,
        confirm: true,
      });
      $("#command-output").textContent = `> ${line}\n\n${JSON.stringify(
        result.reply,
        null,
        2,
      )}`;
      state.history = [line, ...state.history.filter((h) => h !== line)].slice(
        0,
        10,
      );
      $("#command-history").innerHTML = state.history
        .map(
          (h, i) =>
            `<button class="small" data-history="${i}">${escape(h)}</button>`,
        )
        .join("");
      document.querySelectorAll("[data-history]").forEach(
        (b) =>
          (b.onclick = () => {
            $("#command-input").value = state.history[+b.dataset.history];
            $("#command-input").focus();
          }),
      );
    } catch (error) {
      $("#command-output").textContent = `> ${line}\n\nError: ${error.message}`;
    }
  };
  $("#command-form").onsubmit = (event) => {
    event.preventDefault();
    try {
      const line = $("#command-input").value;
      const args = parseCommand(line);
      if (!args.length) return;
      const write =
        /^(SET|MSET|DEL|UNLINK|EXPIRE|PEXPIRE|PERSIST|HSET|HDEL|LPUSH|RPUSH|LPOP|RPOP|SADD|SREM|ZADD|ZREM|XADD|XDEL|INCR|DECR|INCRBY|DECRBY|APPEND)$/i.test(
          args[0],
        );
      if (write)
        dialog(
          "Execute data write",
          "Review this command before changing data.",
          `<pre class="key-value">${escape(line)}</pre>`,
          () => run(args, line),
          "Execute write",
        );
      else void run(args, line);
    } catch (error) {
      toast(error.message);
    }
  };
}
function renderActivity(value) {
  $("#content").innerHTML = `${heading(
    "Activity",
    "Investigate slow commands across your cluster.",
    '<button id="refresh-activity">↻ Refresh</button>',
  )}<div class="banner">Slow logs use Lavik’s <code>SLOWLOG</code> interface. Valkey-specific hot-key tracking and large-request / large-reply command logs are not available in Lavik.</div><section class="panel"><h2>Slow commands</h2>${
    value.entries.length
      ? table(
          ["Time", "Node", "Duration", "Command"],
          value.entries.map(
            (e) =>
              `<tr><td>${
                e.time
                  ? escape(new Date(+e.time * 1000).toLocaleTimeString())
                  : "—"
              }</td><td class="mono">${escape(short(e.node))}</td><td>${
                e.duration ? `${number(+e.duration / 1000)} ms` : "—"
              }</td><td class="mono wrap">${escape(
                e.error || JSON.stringify(e.args),
              )}</td></tr>`,
          ),
        )
      : empty(
          "No slow commands recorded",
          "Commands appear when their execution time exceeds the node’s slow-log threshold.",
        )
  }</section>`;
  $("#refresh-activity").onclick = () => refresh(true).catch(showError);
}
function dialog(title, description, content, submit, label = "Confirm") {
  const element = $("#dialog");
  element.className = "";
  element.innerHTML = `<form id="dialog-form"><div class="title-row"><h2>${escape(
    title,
  )}</h2><button type="button" id="close-dialog" aria-label="Close dialog">×</button></div><p>${escape(
    description,
  )}</p>${content}<div class="error" id="dialog-error" role="alert"></div><div class="actions"><button type="button" id="cancel-dialog">Cancel</button><button class="primary" type="submit">${escape(
    label,
  )}</button></div></form>`;
  $("#close-dialog").onclick = $("#cancel-dialog").onclick = () =>
    element.close();
  $("#dialog-form").onsubmit = async (event) => {
    event.preventDefault();
    const button = element.querySelector("button[type=submit]");
    button.disabled = true;
    try {
      await submit();
      element.close();
    } catch (error) {
      $("#dialog-error").textContent = error.message;
    } finally {
      button.disabled = false;
    }
  };
  element.showModal();
}
function connectDialog() {
  dialog(
    "Connect a Lavik cluster",
    "Use the Meta Admin endpoint. You can connect a running cluster or an uninitialized Meta deployment.",
    `<label for="cluster-name">Cluster name</label><input id="cluster-name" placeholder="production-eu" required pattern="[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}"><label for="meta-seeds">Meta seed addresses</label><input id="meta-seeds" placeholder="10.0.0.11:7200, 10.0.0.12:7200" required><small>Numeric IP addresses, separated by commas.</small><label for="profile">Connection profile</label><select id="profile">${state.profiles
      .map((p) => `<option>${escape(p)}</option>`)
      .join("")}</select>`,
    async () => {
      const id = $("#cluster-name").value;
      await api("/clusters", "POST", {
        id,
        seeds: $("#meta-seeds")
          .value.split(",")
          .map((s) => s.trim()),
        profile: $("#profile").value,
      });
      state.clusters = await api("/clusters");
      navigate("dashboard", id);
    },
    "Connect cluster",
  );
}
function requestId() {
  return [...crypto.getRandomValues(new Uint8Array(16))]
    .map((n) => n.toString(16).padStart(2, "0"))
    .join("");
}
function operationDialog(kind, input, title, description) {
  const id = requestId();
  dialog(
    title,
    description,
    `<div class="banner">Cluster <strong>${escape(
      state.cluster,
    )}</strong> · Group <strong>${escape(input.group)}</strong>${
      input.node ? `<br>Node <code>${escape(input.node)}</code>` : ""
    }</div>`,
    async () => {
      await api(`/clusters/${state.cluster}/operations`, "POST", {
        kind,
        input,
        requestId: id,
      });
      navigate("operations");
      toast("Operation accepted. Follow its progress here.");
    },
    title,
  );
}
function replicaDialog(group) {
  let retained = null;
  dialog(
    "Add a replica",
    "Start the node with this cluster’s Meta seeds and a fresh data file first. Lavik will register it, assign it to the group, and synchronize it from the primary.",
    `<div class="banner">Cluster ${escape(state.cluster)} · Group ${escape(
      group,
    )}</div><label for="node-id">Node ID</label><input id="node-id" required pattern="[0-9a-f]{40}" placeholder="40-character lowercase hex node ID"><label for="node-endpoint">Client endpoint</label><input id="node-endpoint" required placeholder="tcp://10.0.1.15:6379">`,
    async () => {
      const input = {
        group,
        node: $("#node-id").value,
        endpoint: $("#node-endpoint").value,
      };
      const key = JSON.stringify(input);
      if (!retained || retained.key !== key)
        retained = { key, id: requestId() };
      await api(`/clusters/${state.cluster}/operations`, "POST", {
        kind: "replica-add",
        input,
        requestId: retained.id,
      });
      navigate("operations");
      toast("Replica addition accepted. Waiting for synchronization.");
    },
    "Add replica",
  );
}
function createDialog() {
  let retained = null;
  dialog(
    "Initialize a data cluster",
    "Paste the manifest used to start the Meta members. Every listed Data node must be running with a fresh, disposable population. Initialization replaces those populations.",
    `<label for="manifest">Cluster manifest (TOML)</label><textarea id="manifest" class="mono" rows="14" required placeholder='schema_version = 1\nslot_strategy = "contiguous-even"\n\n[[meta_members]]\n...'></textarea><label for="confirm-cluster">Type ${escape(
      state.cluster,
    )} to confirm initialization</label><input id="confirm-cluster" required autocomplete="off">`,
    async () => {
      if ($("#confirm-cluster").value !== state.cluster)
        throw new Error("The cluster name does not match");
      const input = { manifest: $("#manifest").value };
      if (!retained || retained.manifest !== input.manifest)
        retained = { manifest: input.manifest, id: requestId() };
      await api(`/clusters/${state.cluster}/operations`, "POST", {
        kind: "create",
        input,
        requestId: retained.id,
      });
      navigate("operations");
      toast("Initialization accepted. Waiting for cluster readiness.");
    },
    "Initialize cluster",
  );
  $("#dialog").classList.add("wide");
}

initialize().catch(() => login());
setInterval(() => {
  if (!$("#dialog").open)
    refresh(false).catch((error) => {
      if (state.page !== "login") showError(error);
    });
}, 5000);
