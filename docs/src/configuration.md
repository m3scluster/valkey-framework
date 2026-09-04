# Configuration

The backend scheduler is configured through environment variables. Values are read during startup. Unless stated otherwise, an empty value is treated as unset and the documented default is used. Boolean values are enabled only when the value is exactly `true`.

## Mesos and framework settings

| Variable | Default | Description |
|---|---|---|
| `MESOS_MASTER` | `127.0.0.1:5050` | Mesos master endpoint. If no scheme is included, `http://` is used, or `https://` when `MESOS_SSL=true`. |
| `MESOS_SSL` | `false` | Selects HTTPS for a master address without an explicit scheme. |
| `MESOS_TLS_INSECURE` | `false` | Disables TLS certificate verification for Mesos connections. Use only for trusted development or self-signed certificates. |
| `MESOS_USERNAME` | empty | Username for Mesos HTTP Basic Authentication. Authentication is enabled only when this and `MESOS_PASSWORD` are set. |
| `MESOS_PASSWORD` | empty | Mesos HTTP Basic Authentication password. Keep it in the deployment environment or secret store; do not commit it. |
| `MESOS_ROLE` | `*` | Mesos role used by the framework. |
| `MESOS_CNI` | `weave` | CNI network name passed to Mesos tasks. An explicitly set empty value remains empty. |
| `MESOS_DOMAIN` | `mesos` | DNS domain used for task hostnames and the default Valkey master hostname. Leading and trailing dots are removed. |
| `MESOS_DRY_RUN` | `false` | When `true`, runs the scheduler without deploying real Mesos tasks. |
| `MESOS_CHECKPOINT` | `true` | Enables Mesos framework checkpointing when `true`. |
| `FRAMEWORK_NAME` | `valkey-framework` | Framework name and prefix used for persisted Redis state. |
| `FRAMEWORK_USER` | `$USER`, then `root` | Mesos framework user. If `FRAMEWORK_USER` is unset, the backend uses the `USER` environment variable and finally `root`. |
| `START_ON_BOOT` | `true` | Starts the scheduler automatically during backend initialization when `true`. |
| `LISTEN_ADDR` | `0.0.0.0:10001` | Address on which the backend HTTP API listens. |
| `FRONTEND_URL` | `http://localhost:5173` | URL published as the Mesos framework Web UI URL. |
| `SSL_KEY_BASE64` | empty | Base64-encoded private key for serving the backend over HTTPS. Must be provided together with `SSL_CRT_BASE64`. |
| `SSL_CRT_BASE64` | empty | Base64-encoded certificate for serving the backend over HTTPS. Must be provided together with `SSL_KEY_BASE64`. |

## Valkey settings

| Variable | Default | Description |
|---|---:|---|
| `VALKEY_IMAGE` | `valkey/valkey:8-alpine` | Container image used for Valkey tasks. |
| `VALKEY_MASTERS` | `1` | Desired number of masters. Values below 1 are clamped to 1. |
| `VALKEY_SLAVES` | `2` | Desired number of replicas. Values below 1 are clamped to 1. |
| `VALKEY_CPU` | `0.2` | CPU resources requested per Valkey task. Parsed as a floating-point number. Invalid values use the default. |
| `VALKEY_MEMORY_MB` | `256` | Memory requested per task in megabytes. Parsed as a floating-point number. Invalid values use the default. |
| `VALKEY_DISK_MB` | `0` | Disk requested per task in megabytes. Parsed as a floating-point number. Invalid values use the default. |
| `VALKEY_PORT` | `6379` | Valkey service port. Parsed as an integer; invalid values use the default. |
| `VALKEY_METRICS_ADDR` | `master.<MESOS_DOMAIN>:<VALKEY_PORT>` | Address used to query Valkey metrics. The computed default uses the configured domain and port. |
| `VALKEY_MASTER_HOST` | `master.<MESOS_DOMAIN>` | Hostname used when configuring replication. The computed default uses the configured domain. |
| `VALKEY_PASSWORD` | empty | Password clients use when connecting to Valkey. Keep it out of source control. |
| `VALKEY_REPLICATION_PASSWORD` | empty | Password used for master-to-replica authentication. Keep it out of source control. |
| `VALKEY_VOLUME_DRIVER` | empty | Optional Docker volume driver for Valkey persistence. |
| `VALKEY_VOLUME_NAME` | empty | Optional Docker volume name for Valkey persistence. |
| `VALKEY_VOLUME_PATH` | `/data` | Container path used for the persistent Valkey volume. |

## Redis state storage

| Variable | Default | Description |
|---|---|---|
| `REDIS_SERVER` | `redis.weave.local:6379` | Redis endpoint used for scheduler state. |
| `REDIS_PASSWORD` | empty | Password for the Redis state store. Keep it out of source control. |
| `REDIS_DB` | `10` | Redis database number. Parsed as an integer; invalid values use the default. |
| `REDIS_POOLSIZE` | `0` | Redis client pool size. `0` lets the Redis client choose its default. Parsed as an integer; invalid values use the default. |

## Duration and parsing notes

| Variable | Default | Description |
|---|---|---|
| `RECONCILE_WAIT` | `30m` | Go duration between reconciliation cycles, for example `10m` or `30s`. |

An invalid `RECONCILE_WAIT` currently results in Go's zero duration because the parse error is ignored; use a valid Go duration. Integer and floating-point parsing errors fall back to their documented defaults. The scheduler persists its runtime state in Redis under `<FRAMEWORK_NAME>:state`; Redis is therefore required for state recovery.

## Example

```bash
MESOS_MASTER=https://mesos.example.test:5050 \
MESOS_USERNAME=mesos \
MESOS_PASSWORD="$MESOS_PASSWORD" \
REDIS_SERVER=redis.example.test:6379 \
REDIS_DB=10 \
VALKEY_PASSWORD="$VALKEY_PASSWORD" \
VALKEY_REPLICATION_PASSWORD="$VALKEY_REPLICATION_PASSWORD" \
make run
```

Never put real passwords or private keys in shell scripts, documentation, or Git. Supply secrets through the deployment system's secret mechanism.