// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { nodeLayout } from "./placement.js";
import { prepareHosts } from "./hosts.js";
const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );

/** A setup form owns its inputs; background fleet polling cannot erase a draft. */
export async function setup(root, api, created, options = {}) {
  const follower = options.cluster;
  const hosts = await prepareHosts(root, api, {
    single: !!follower,
    selected: options.hosts || [],
  });
  let draft = {
    id: "",
    storage: "file",
    hugepageMiB: 8192,
    metricsPort: 9100,
    grafanaPort: 3000,
    prometheusPort: 9090,
    release: "nightly",
    followers: 2,
    groups: 1,
    metaCount: 3,
    threads: 2,
    dataGiB: 4,
    dataPort: 6379,
    metaPort: 7100,
    ctlPort: 7200,
    controlPort: 7300,
    supervisor: "systemd",
    clientMode: "single",
    baseDir: ".local/share/lavik/clusters",
    ...options.draft,
  };
  if (
    JSON.stringify(hosts.map((h) => h.id)) !==
    JSON.stringify((options.hosts || []).map((h) => h.id))
  )
    draft.placement = undefined;
  const field = (name, label, type = "text", value = draft[name]) =>
    `<label>${escape(
      label,
    )}<input name="${name}" type="${type}" value="${escape(
      value,
    )}" required></label>`;
  root.innerHTML = `<div class="heading"><div><div class="eyebrow">${
    follower ? "EXPAND YOUR CLUSTER" : "SET UP A LAVIK CLUSTER"
  }</div><h1>${follower ? "Add a follower" : "Create a cluster"}</h1><p>${
    follower
      ? "Install the cluster’s exact release on a host and synchronize a new follower."
      : "SSH is ready. Choose a release and map your prepared hosts to Meta voters and Data nodes."
  }</p></div></div><ol class="setup-steps"><li>1 · Prepare hosts</li><li class="active">2 · Node placement</li><li>3 · Review & deploy</li></ol><form id="setup-form" class="panel setup-form">
  ${
    follower
      ? `<div class="banner">${escape(follower)} · ${escape(
          options.group,
        )}</div>`
      : `<div class="setup-grid">${field(
          "id",
          "Cluster name",
        )}<label>Lavik release<input name="release" value="${escape(
          draft.release,
        )}" list="lavik-releases" required><datalist id="lavik-releases"><option value="nightly"></datalist><small>Nightly or any published GitHub release tag. <a href="https://github.com/eloqdata/lavik/releases" target="_blank" rel="noopener noreferrer">View releases on GitHub ↗</a></small></label></div><p id="release-note" class="muted">Loading releases…</p><button type="button" class="small" id="more-releases" hidden>Load older releases</button>`
  }
  <h2>Prepared hosts</h2><p class="muted">Admin can connect using its workspace SSH key. Set each host’s cluster IP below: this is the private address other cluster hosts use, which may differ from the SSH address.</p><div id="setup-hosts"></div><button type="button" id="change-hosts" class="small">Back to host preparation</button>
  ${
    follower
      ? ""
      : `<h2>Cluster size</h2><div class="setup-grid">${field(
          "groups",
          "Primary groups",
          "number",
        )}${field(
          "followers",
          "Followers per primary",
          "number",
        )}</div><p class="muted">Defaults: one primary, two followers, and three Meta voters. Nodes are placed across your hosts.</p><label>Service lifecycle<select name="supervisor"><option value="systemd">Start on boot with systemd</option><option value="process">Development processes (no automatic restart)</option></select><small>For Docker lab hosts without systemd, select Development processes.</small></label><details><summary>Tune advanced settings</summary><div class="setup-grid">${field(
          "metaCount",
          "Meta voters (1, 3, or 5)",
          "number",
        )}${field("threads", "Workers per Data node", "number")}${field(
          "dataGiB",
          "Storage GiB per Data node",
          "number",
        )}${field("baseDir", "Storage directory on each host")}${field(
          "dataPort",
          "Data base port",
          "number",
        )}${field("metaPort", "Raft base port", "number")}${field(
          "ctlPort",
          "Admin base port",
          "number",
        )}${field(
          "controlPort",
          "Control base port",
          "number",
        )}<label>Client mode<select name="clientMode"><option value="cluster">Redis Cluster</option><option value="single">Single endpoint (one primary group)</option></select></label></div></details>`
  }
  ${
    follower
      ? `<label>Data port<input name="dataPort" type="number" value="${escape(
          draft.dataPort,
        )}" required></label>`
      : ""
  }
  ${
    follower
      ? ""
      : '<h2>Node placement</h2><p class="muted">A host can run a Meta voter and a Data node. The default spreads both across your hosts; choose a different host for any node below.</p><div id="node-placement"></div><button type="button" class="small" id="reset-placement">Spread evenly across hosts</button>'
  }
  ${
    follower
      ? ""
      : `<h2>Storage</h2><label>Storage engine<select name="storage"><option value="file">io_uring · managed data files</option><option value="spdk">SPDK · dedicated NVMe controllers</option></select></label><div id="spdk-settings" hidden><p>Admin will configure VFIO, hugepages, and persistent boot services on every Data host, using the full SPDK release. Select fresh dedicated media: binding removes the controller from Linux block access.</p><p class="muted">Requires root SSH, IOMMU isolation, one namespace per controller, and one Data node per physical host. Mounted, partitioned, in-use, and nonempty devices are rejected. Unsafe no-IOMMU mode is unsupported.</p>${field(
          "hugepageMiB",
          "Hugepage memory MiB per Data host",
          "number",
        )}<div id="spdk-devices"></div></div>
  <h2>Monitoring</h2><p>Choose hosts for Prometheus and Grafana. Each selected host collects every Data node’s metrics. Docker Engine and Compose v2 must already be available to its SSH account; monitoring hosts need registry access or cached images.</p><div id="monitor-hosts">${hosts
    .map(
      (h, i) =>
        `<label class="prepared-host"><input type="checkbox" name="monitorHosts" value="${i}" ${
          (draft.monitorHosts || []).map(Number).includes(i) ? "checked" : ""
        }><span>Host ${i + 1} · ${escape(
          h.host,
        )}<small>Grafana: private IP · Prometheus: localhost</small></span></label>`,
    )
    .join(
      "",
    )}</div><p class="muted">Leave unchecked to use your own monitoring. Metrics listeners are enabled when a monitoring host is selected. Protect metrics ports with your private-network firewall.</p><details><summary>Monitoring ports</summary><div class="setup-grid">${field(
    "metricsPort",
    "Data metrics base port",
    "number",
  )}${field("grafanaPort", "Grafana port", "number")}${field(
    "prometheusPort",
    "Prometheus port",
    "number",
  )}</div></details>`
  }
  <div class="error" id="setup-error" role="alert"></div><div class="actions"><button class="primary" type="submit">Check hosts & review</button></div></form><div id="setup-review"></div>`;
  const form = root.querySelector("#setup-form");
  let revision = 0;
  const step = (index) =>
    root
      .querySelectorAll(".setup-steps li")
      .forEach((item, i) => item.classList.toggle("active", i === index));
  const invalidate = () => {
    revision++;
    form.hidden = false;
    root.querySelector("#setup-review").innerHTML = "";
    step(1);
  };
  form.addEventListener("input", invalidate);
  form.addEventListener("change", invalidate);
  const readHosts = () =>
    hosts.map((host, index) => ({
      ...host,
      address: root.querySelector(`[data-address="${index}"]`).value.trim(),
    }));
  root.querySelector("#change-hosts").onclick = () => {
    invalidate();
    void setup(root, api, created, {
      ...options,
      hosts: readHosts(),
      draft: {
        ...Object.fromEntries(new FormData(form)),
        placement,
        monitorHosts: [
          ...form.querySelectorAll('[name="monitorHosts"]:checked'),
        ].map((e) => Number(e.value)),
        spdkDevices: Object.fromEntries(
          [...form.querySelectorAll("[data-spdk-host]")]
            .filter((e) => e.value.trim())
            .map((e) => [e.dataset.spdkHost, e.value.trim()]),
        ),
      },
    });
  };
  let placement = draft.placement;
  let shape = "";
  const renderPlacement = (reset = false) => {
    if (follower) return;
    const target = root.querySelector("#node-placement");
    try {
      const currentHosts = readHosts();
      const counts = Object.fromEntries(
        ["groups", "followers", "metaCount"].map((name) => [
          name,
          Number(form.elements[name].value),
        ]),
      );
      const nextShape = JSON.stringify([currentHosts.length, counts]);
      if (reset || (shape && shape !== nextShape)) placement = undefined;
      const nodes = nodeLayout({ hosts: currentHosts, ...counts, placement });
      shape = nextShape;
      const devices = root.querySelector("#spdk-devices");
      const previous = {
        ...draft.spdkDevices,
        ...Object.fromEntries(
          [...devices.querySelectorAll("input")].map((input) => [
            input.dataset.spdkHost,
            input.value,
          ]),
        ),
      };
      devices.innerHTML = [
        ...new Set(nodes.filter((n) => n.kind === "data").map((n) => n.host)),
      ]
        .map(
          (index) =>
            `<label>Host ${
              index + 1
            } dedicated NVMe namespace<input data-spdk-host="${index}" placeholder="spdk://0000:01:00.0/1" value="${escape(
              previous[index] || "",
            )}"></label>`,
        )
        .join("");
      placement = Object.fromEntries(nodes.map((n) => [n.name, n.host]));
      target.innerHTML = `<div class="table-wrap"><table><thead><tr><th>Node</th><th>Role</th><th>Host</th></tr></thead><tbody>${nodes
        .map(
          (n) =>
            `<tr><td>${n.name}</td><td>${
              n.kind === "meta"
                ? "Meta voter"
                : `${n.group} · ${
                    n.role === "primary" ? "Primary" : "Follower"
                  }`
            }</td><td><select data-placement="${n.name}" aria-label="Host for ${
              n.name
            }">${currentHosts
              .map(
                (h, i) =>
                  `<option value="${i}" ${
                    i === n.host ? "selected" : ""
                  }>Host ${i + 1} · ${escape(h.host || "not entered")}:${escape(
                    h.port || 22,
                  )}${h.address ? ` → ${escape(h.address)}` : ""}</option>`,
              )
              .join("")}</select></td></tr>`,
        )
        .join("")}</tbody></table></div>`;
      target.querySelectorAll("[data-placement]").forEach(
        (select) =>
          (select.onchange = () => {
            placement[select.dataset.placement] = Number(select.value);
            invalidate();
            renderPlacement();
          }),
      );
    } catch (error) {
      target.innerHTML = `<p class="muted">${escape(error.message)}</p>`;
    }
  };
  root.querySelector("#setup-hosts").innerHTML = hosts
    .map(
      (h, i) =>
        `<div class="setup-grid"><p><strong>Host ${i + 1} · ${escape(
          h.user,
        )}@${escape(h.host)}:${
          h.port
        }</strong><br><small>Admin SSH access verified</small></p><label>Host ${
          i + 1
        } cluster IP<input data-address="${i}" value="${escape(
          h.address || "",
        )}" placeholder="Private IP reachable between hosts" required></label></div>`,
    )
    .join("");
  root
    .querySelectorAll("[data-address]")
    .forEach((input) =>
      input.addEventListener("input", () => renderPlacement()),
    );
  if (!follower) {
    for (const name of ["clientMode", "supervisor", "storage"])
      form.elements[name].value = draft[name];
    for (const name of ["groups", "followers", "metaCount"])
      form.elements[name].addEventListener("input", () => renderPlacement());
    root.querySelector("#reset-placement").onclick = () => {
      invalidate();
      renderPlacement(true);
    };
    const storageChanged = () => {
      root.querySelector("#spdk-settings").hidden =
        form.elements.storage.value !== "spdk";
    };
    form.elements.storage.addEventListener("change", storageChanged);
    storageChanged();
    renderPlacement();
  }
  let page = 0;
  const loadReleases = async () => {
    try {
      const result = await api(`/releases?page=${page + 1}`);
      page++;
      if (!root.querySelector("#lavik-releases")) return;
      root.querySelector("#lavik-releases").insertAdjacentHTML(
        "beforeend",
        result.items
          .filter((r) => r.tag !== "nightly")
          .map(
            (r) =>
              `<option value="${escape(r.tag)}">${escape(r.name)}</option>`,
          )
          .join(""),
      );
      root.querySelector("#release-note").textContent =
        "Releases from eloqdata/lavik on GitHub. Exact package checksums are pinned when you review.";
      root.querySelector("#more-releases").hidden = !result.more;
    } catch (error) {
      if (root.querySelector("#release-note"))
        root.querySelector(
          "#release-note",
        ).textContent = `${error.message}. You can still enter a release tag and retry host checks.`;
    }
  };
  if (!follower) {
    root.querySelector("#more-releases").onclick = loadReleases;
    void loadReleases();
  }
  form.onsubmit = async (event) => {
    event.preventDefault();
    const checkedRevision = revision;
    const button = form.querySelector("button[type=submit]");
    button.disabled = true;
    button.textContent = "Checking SSH, storage, and ports…";
    root.querySelector("#setup-error").textContent = "";
    root.querySelector("#setup-review").innerHTML = "";
    try {
      draft = {
        ...Object.fromEntries(new FormData(form)),
        hosts: readHosts().map((host) => ({
          hostId: host.id,
          address: host.address,
        })),
        ...(!follower
          ? {
              placement,
              monitorHosts: [
                ...form.querySelectorAll('[name="monitorHosts"]:checked'),
              ].map((e) => Number(e.value)),
              spdkDevices: Object.fromEntries(
                [...form.querySelectorAll("[data-spdk-host]")]
                  .filter((e) => e.value.trim())
                  .map((e) => [e.dataset.spdkHost, e.value.trim()]),
              ),
            }
          : {}),
      };
      const preview = await api(
        follower ? `/clusters/${follower}/follower-preview` : "/setup/preview",
        "POST",
        follower
          ? { ...draft, group: options.group, host: draft.hosts[0] }
          : draft,
      );
      if (!form.isConnected) return;
      if (revision !== checkedRevision)
        throw new Error(
          "Settings changed during host checks. Check the updated settings again.",
        );
      step(2);
      form.hidden = true;
      const canUseProcesses =
        !follower &&
        preview.supervisor === "systemd" &&
        preview.storage !== "spdk" &&
        preview.checks.some((check) => check.systemdAvailable === false);
      const review = root.querySelector("#setup-review");
      review.innerHTML = `<section class="panel"><div class="title-row"><h2>${
        preview.ready ? "Ready to deploy" : "Host checks need attention"
      }</h2><button type="button" id="edit-setup">Edit settings</button></div><div class="setup-checks">${preview.checks
        .map(
          (c) =>
            `<div class="banner ${c.ok ? "" : "warn"}"><strong>${escape(
              `${preview.hosts[c.host].host}:${preview.hosts[c.host].port}`,
            )} · ${c.ok ? "Ready" : "Needs attention"}</strong>${
              c.ok
                ? `<p>${escape(c.arch)} · ${c.cpus} CPUs · ${Math.floor(
                    c.freeBytes / 1024 ** 3,
                  )} GiB free</p>`
                : `<ul>${c.errors
                    .map((error) => `<li>${escape(error)}</li>`)
                    .join("")}</ul>`
            }</div>`,
        )
        .join("")}</div>${(preview.warnings || [])
        .map((w) => `<p class="muted">${escape(w)}</p>`)
        .join("")}
      ${
        canUseProcesses
          ? '<div class="banner warn"><p>These hosts cannot use systemd. For this lab, run all cluster nodes as development processes. Nodes will not restart automatically after a crash or host reboot.</p><button type="button" id="use-process-supervisor">Use development processes & recheck</button></div>'
          : ""
      }
      ${
        preview.storage === "spdk"
          ? `<div class="banner warn"><strong>SPDK host configuration</strong><p>${
              preview.hugepageMiB
            } MiB hugepages per Data host. The selected controllers will be bound to VFIO:</p><ul>${preview.nodes
              .filter((n) => n.spdk)
              .map(
                (n) =>
                  `<li>${escape(preview.hosts[n.host].address)} · ${escape(
                    n.spdk,
                  )} · serial ${escape(
                    preview.checks.find((c) => c.host === n.host)?.spdk
                      ?.serial || "unavailable",
                  )}</li>`,
              )
              .join("")}</ul></div>`
          : ""
      }
      ${
        preview.monitorHosts?.length
          ? `<div class="banner"><strong>Monitoring hosts</strong><p>${preview.monitorHosts
              .map((i) => escape(preview.hosts[i].address))
              .join(", ")} · Grafana :${escape(
              preview.grafanaPort || 3000,
            )}</p><p>Grafana user: admin. A unique password is generated on each monitoring host and saved under the deployment’s monitoring/grafana-password file.</p></div>`
          : ""
      }
      <p>Service lifecycle: <strong>${
        preview.supervisor === "process"
          ? "Development processes (no automatic restart)"
          : "Start on boot with systemd"
      }</strong></p>
      <p>Release <strong>${escape(preview.release.tag)}</strong> · ${
        preview.nodes.filter((n) => n.kind === "data").length
      } Data nodes · ${
        preview.nodes.filter((n) => n.kind === "meta").length
      } Meta voters</p><div class="table-wrap"><table><thead><tr><th>Node</th><th>Host</th><th>Ports</th></tr></thead><tbody>${preview.nodes
        .map(
          (n) =>
            `<tr><td>${escape(n.name)}${
              n.role ? ` · ${escape(n.role)}` : ""
            }</td><td>${escape(preview.hosts[n.host].address)}</td><td>${[
              n.port,
              n.ctl,
              n.control,
              n.metrics,
            ]
              .filter(Boolean)
              .join(", ")}</td></tr>`,
        )
        .join("")}</tbody></table></div>${
        preview.ready
          ? `<form id="deploy-confirm"><p>Admin will install binaries and create fresh storage under <code>${escape(
              preview.baseDir,
            )}/${escape(
              preview.id,
            )}</code>. Existing databases are never overwritten.</p>${
              preview.storage === "spdk"
                ? '<label class="prepared-host"><input name="spdkConfirm" type="checkbox" required><span>I confirm these are dedicated controllers for this cluster and authorize VFIO binding and hugepage allocation.</span></label>'
                : ""
            }<label>Type ${escape(
              preview.id,
            )} to confirm<input name="confirm" autocomplete="off" required></label><div class="error" id="deploy-error" role="alert"></div><button class="primary" type="submit">${
              follower ? "Deploy follower" : "Deploy cluster"
            }</button></form>`
          : "<p>Resolve the reported prerequisite or adjust settings, then run the checks again.</p>"
      }</section>`;
      review.querySelector("#edit-setup").onclick = () => {
        invalidate();
        form.scrollIntoView({ behavior: "smooth" });
      };
      const useProcesses = review.querySelector("#use-process-supervisor");
      if (useProcesses)
        useProcesses.onclick = () => {
          // Switching lifecycle is an explicit operator choice and needs a fresh
          // server review before a deployment can be admitted.
          form.elements.supervisor.value = "process";
          invalidate();
          form.requestSubmit();
        };
      review.scrollIntoView({ behavior: "smooth", block: "start" });
      const confirm = review.querySelector("#deploy-confirm");
      if (confirm)
        confirm.onsubmit = async (event) => {
          event.preventDefault();
          const submit = confirm.querySelector("button");
          submit.disabled = true;
          try {
            await api(
              follower
                ? `/clusters/${follower}/follower-deploy`
                : "/setup/deploy",
              "POST",
              {
                token: preview.token,
                confirm: new FormData(confirm).get("confirm"),
                spdkConfirm: new FormData(confirm).get("spdkConfirm") === "on",
              },
            );
            step(2);
            created(preview.id);
          } catch (error) {
            review.querySelector("#deploy-error").textContent = error.message;
            submit.disabled = false;
          }
        };
    } catch (error) {
      if (form.isConnected)
        root.querySelector("#setup-error").textContent = error.message;
    } finally {
      button.disabled = false;
      button.textContent = "Check hosts & review";
    }
  };
}
