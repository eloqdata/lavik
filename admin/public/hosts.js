// Copyright (C) 2026 EloqData Inc. Licensed under the Apache License, Version 2.0.
import { parseHostList } from "./placement.js";
const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );

/** Host onboarding completes before any node is assigned; credentials stay in this form. */
export function prepareHosts(
  root,
  api,
  { single = false, selected = [] } = {},
) {
  return new Promise((resolve) => {
    root.innerHTML = `<div class="heading"><div><div class="eyebrow">${
      single ? "ADD A FOLLOWER" : "CREATE A CLUSTER"
    }</div><h1>Prepare hosts</h1><p>First connect your Linux hosts. In the next step, choose which nodes run on each host.</p></div></div>
    <ol class="setup-steps"><li class="active">1 · Prepare hosts</li><li>2 · Node placement</li><li>3 · Review & deploy</li></ol>
    <section class="panel"><h2>Available hosts</h2><p class="muted">Prepared hosts can be shared by multiple clusters. Select ${
      single ? "one host" : "the hosts for this cluster"
    }; Admin checks a fresh passwordless connection before continuing.</p><div id="prepared-hosts">Loading hosts…</div></section>
    <form id="prepare-host-form" class="panel setup-form"><h2>Prepare new hosts</h2><p>Paste ${
      single ? "a host" : "a host list"
    } and provide the login you use today. Admin installs its own SSH public key and verifies that it can connect without a password. Your terminal SSH command must select this key explicitly with ssh -i.</p>
    <label>Host list<textarea id="host-list" aria-label="Host list" rows="4" required placeholder="server-1.example.com&#10;server-2.example.com&#10;127.0.0.1:2213 172.30.95.13"></textarea><small>One SSH_HOST[:PORT] per line. Optionally append the cluster’s private IP; you can set that in Node placement.</small></label>
    <div class="setup-grid"><label>SSH user<input id="host-user" value="ubuntu" required autocomplete="username"></label><label>Connect using<select id="host-auth"><option value="password">Password</option><option value="key">Private key / SSH agent</option></select></label></div>
    <div id="password-login"><label>SSH password<input id="host-password" type="password" autocomplete="off"></label></div>
    <div id="key-login" hidden><div class="setup-grid"><label>Private key path on the Admin computer<input id="host-key" placeholder="~/.ssh/id_ed25519 (empty uses SSH agent)"></label><label>Key passphrase (if encrypted)<input id="host-passphrase" type="password" autocomplete="off"></label></div></div>
    <p class="muted">The login applies to this batch. Use separate batches for different logins. Passwords and passphrases are used only for preparation and are not saved. On first connection, Admin trusts and saves the host’s SSH key; a changed host key stops the connection. Existing authorized keys are preserved.</p>
    <div id="host-prepare-error" class="error" role="alert"></div><button type="submit" class="primary">Prepare hosts</button><div id="host-progress" aria-live="polite"></div></form>
    <section class="panel"><p class="muted">This step sets up SSH access. The next step checks Linux, Python 3, storage, ports, and service settings before installing Lavik.</p><div id="host-next-error" class="error" role="alert"></div><button id="hosts-next" class="primary" disabled>Continue to node placement</button></section>`;
    const area = root.querySelector("#prepared-hosts");
    const form = root.querySelector("#prepare-host-form");
    const next = root.querySelector("#hosts-next");
    const records = new Map();
    const chosen = new Set(selected.map((host) => host.id).filter(Boolean));
    const verified = new Set();
    let busy = false;
    function render() {
      if (!area.isConnected) return;
      area.innerHTML = records.size
        ? [...records.values()]
            .map(
              (h) =>
                `<label class="prepared-host"><input type="${
                  single ? "radio" : "checkbox"
                }" name="selected-host" data-host-id="${h.id}" ${
                  chosen.has(h.id) ? "checked" : ""
                }><span><strong>${escape(h.user)}@${escape(h.host)}:${
                  h.port
                }</strong><small>${
                  verified.has(h.id)
                    ? "Admin SSH access verified"
                    : "Saved host · will verify before continuing"
                }${
                  h.address ? ` · ${escape(h.address)}` : ""
                }</small></span></label>`,
            )
            .join("")
        : '<p class="muted">No prepared hosts yet. Add your first hosts below.</p>';
      area.querySelectorAll("[data-host-id]").forEach((input) => {
        input.disabled = busy;
        input.onchange = () => {
          if (single) chosen.clear();
          input.checked
            ? chosen.add(input.dataset.hostId)
            : chosen.delete(input.dataset.hostId);
          render();
        };
      });
      next.disabled = busy || !chosen.size;
    }
    function setBusy(value) {
      busy = value;
      form
        .querySelectorAll("input, textarea, select, button")
        .forEach((input) => (input.disabled = value));
      render();
    }
    root.querySelector("#host-auth").onchange = (event) => {
      const key = event.target.value === "key";
      root.querySelector("#password-login").hidden = key;
      root.querySelector("#key-login").hidden = !key;
      root.querySelector("#host-password").value = "";
      root.querySelector("#host-passphrase").value = "";
    };
    void api("/hosts")
      .then((hosts) => {
        for (const h of hosts) if (!records.has(h.id)) records.set(h.id, h);
        render();
      })
      .catch((error) => {
        if (area.isConnected) area.textContent = error.message;
      });
    form.onsubmit = async (event) => {
      event.preventDefault();
      const error = root.querySelector("#host-prepare-error");
      const progress = root.querySelector("#host-progress");
      error.textContent = "";
      let credentials;
      try {
        const hosts = parseHostList(root.querySelector("#host-list").value, {
          user: root.querySelector("#host-user").value,
        });
        if (single && hosts.length !== 1)
          throw new Error("Choose one host for the new follower.");
        credentials = {
          method: root.querySelector("#host-auth").value,
          password: root.querySelector("#host-password").value,
          identityFile: root.querySelector("#host-key").value,
          passphrase: root.querySelector("#host-passphrase").value,
        };
        if (credentials.method === "password" && !credentials.password)
          throw new Error("Enter the SSH login password.");
        root.querySelector("#host-password").value = "";
        root.querySelector("#host-passphrase").value = "";
        setBusy(true);
        progress.innerHTML = hosts
          .map(
            (h, i) =>
              `<p data-progress="${i}">${escape(h.host)}:${
                h.port
              } · Waiting</p>`,
          )
          .join("");
        // Sequential batches give clear per-host results and keep SSH work bounded.
        for (const [index, h] of hosts.entries()) {
          if (!area.isConnected) break;
          const row = progress.querySelector(`[data-progress="${index}"]`);
          row.textContent = `${h.host}:${h.port} · Preparing SSH…`;
          try {
            const prepared = await api("/hosts/prepare", "POST", {
              ...h,
              ...credentials,
            });
            records.set(prepared.id, prepared);
            if (single) chosen.clear();
            chosen.add(prepared.id);
            verified.add(prepared.id);
            row.textContent = `${h.host}:${h.port} · Admin SSH access verified`;
            render();
          } catch (failure) {
            row.textContent = `${h.host}:${h.port} · ${failure.message}`;
            row.className = "error";
          }
        }
      } catch (failure) {
        error.textContent = failure.message;
      } finally {
        if (credentials) {
          credentials.password = "";
          credentials.passphrase = "";
        }
        setBusy(false);
      }
    };
    next.onclick = async () => {
      const error = root.querySelector("#host-next-error");
      error.textContent = "";
      setBusy(true);
      next.textContent = "Verifying Admin SSH access…";
      try {
        if (!chosen.size || chosen.size > 64)
          throw new Error("Select 1–64 prepared hosts.");
        const hosts = [];
        for (const id of chosen) {
          const h = records.get(id);
          try {
            const current = await api(`/hosts/${id}/verify`, "POST", {});
            verified.add(id);
            hosts.push({
              ...current,
              address:
                selected.find((item) => item.id === id)?.address ||
                current.address ||
                "",
            });
          } catch (failure) {
            verified.delete(id);
            throw new Error(
              `${h.user}@${h.host}:${h.port}: ${failure.message}`,
            );
          }
        }
        if (area.isConnected) resolve(hosts);
      } catch (failure) {
        error.textContent = failure.message;
      } finally {
        setBusy(false);
        next.textContent = "Continue to node placement";
      }
    };
  });
}
