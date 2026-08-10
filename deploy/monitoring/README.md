# Keylane monitoring stack

This directory runs Prometheus and Grafana on any Docker host that can reach
the Keylane metrics endpoints. Keylane itself does not run in this Compose
project.

## Prerequisites

- Docker Engine with Docker Compose v2 (`docker compose version`).
- TCP connectivity from the monitoring host to every Keylane metrics port.
- Keylane started with a nonzero metrics port, for example:

  ```sh
  ./keylane --bind 10.0.0.11 --port 7379 --metrics-port 9100 \
    --data-file test2.bin
  ```

Before deploying, run this on the monitoring host and confirm that it returns
Prometheus text metrics:

```sh
curl http://10.0.0.11:9100/metrics
```

## Deploy

Copy the example configuration and edit it:

```sh
cd deploy/monitoring
cp .env.example .env
${EDITOR:-vi} .env
```

At minimum, set a reachable target. The default Grafana login is
`admin`/`admin`; replace the password for production deployments:

```dotenv
KEYLANE_TARGETS=10.0.0.11:9100
GRAFANA_ADMIN_PASSWORD=admin
```

Validate and start the stack:

```sh
docker compose config
docker compose up -d
docker compose ps
```

Open the following URL in a browser, replacing the host and port if needed:

```text
http://MONITORING_HOST:3000/d/keylane-overview/keylane-overview
```

Log in with `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` from `.env`
(`admin`/`admin` by default). The Prometheus datasource and the
`Keylane Overview` dashboard are provisioned automatically.

Prometheus is bound to `127.0.0.1:9090` on the monitoring host by default. Its
target status is available locally at:

```text
http://127.0.0.1:9090/targets
```

## Add or remove Keylane nodes

Put every endpoint in the comma-separated `KEYLANE_TARGETS` value:

```dotenv
KEYLANE_TARGETS=10.0.0.11:9100,10.0.0.12:9100,10.0.0.13:9100
```

Apply the change:

```sh
docker compose up -d --force-recreate target-config
```

Prometheus file discovery notices the new target file within 30 seconds; it
does not need to restart. The Grafana dashboard has a multi-select
`Keylane instance` variable. It shows all nodes by default and can filter to
one or more nodes.

Use an address reachable from inside the Prometheus container. When Keylane is
on the same Linux Docker host, `host.docker.internal:9100` is supported by this
Compose file. Do not use `127.0.0.1:9100`, because that address would refer to
the Prometheus container itself.

## Operations

```sh
# Follow logs.
docker compose logs -f prometheus grafana

# Restart after a configuration change.
docker compose up -d

# Stop containers while retaining all stored data.
docker compose down
```

Prometheus and Grafana data are stored in named volumes. `docker compose down
-v` permanently deletes the collected metrics, Grafana database, and generated
target file.

The metrics endpoint has no authentication or TLS. Restrict every Keylane
metrics port so only monitoring hosts can connect. Grafana listens on all host
interfaces by default; protect its host port with a firewall and a strong admin
password. Set `GRAFANA_BIND_ADDRESS` to a specific private address when
appropriate.
