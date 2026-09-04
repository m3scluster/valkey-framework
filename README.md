# Valkey Mesos Control Plane

Dual implementation reference:

- `backend/app.go`: Process start, HTTP server and lifecycle.
- `backend/init.go`: Central initialization of all environment variables via `init()`.
- `backend/main.go`: API, domain models and Mesos abstraction.
- `frontend/`: React/Vite administration frontend with live polling.

## Start

```bash
make run
```

This starts the backend on `http://localhost:8080` and frontend on `http://localhost:5173`. For a secure local test, the backend scheduler runs by default in `DRY_RUN=true`; nodes and scaling are then deterministically visible, but not actually started on Mesos.

## Dashboard

The React dashboard provides a live cluster overview, node resource details, scheduler events, and explicit start/stop and scaling controls.

For the test cluster, credentials can only be set via shell:

```bash
MESOS_MASTER=mesos.example.test:5050 MESOS_USERNAME=example-user MESOS_PASSWORD="$MESOS_PASSWORD" DRY_RUN=true make run
```

`DRY_RUN=false` activates the explicit production seam. The Mesos-v1 framework registration/streaming integration is intentionally not yet activated; the backend startup will fail with an explanatory error message on scaling instead of pretending to have a successful deployment status.

## API

- `GET /api/health`
- `GET /api/cluster`
- `PUT /api/cluster/scale` with `{ "nodes": 3..100 }`
- `GET /api/metrics`

The metrics explicitly report `unknown_no_valkey_endpoint` in dry-run mode because no real Valkey processes are running. Once Mesos task status and endpoints are integrated, the backend metrics collector will query Valkey `INFO` via TCP.

## Foundation

The scheduler structure is based on [m3scluster/compose](https://github.com/m3scluster/compose): Framework/Mesos client behind an interface, reconciliation as source for desired task count, and offer-/task-oriented scheduling. The external integration remains separate from the local dry-run implementation.
