#!/bin/bash
set -euo pipefail

export MESOS_MASTER="${MESOS_MASTER:-devtest.lab.internal:5050}"
export MESOS_SSL="${MESOS_SSL:-true}"
export MESOS_TLS_INSECURE="${MESOS_TLS_INSECURE:-true}"
export MESOS_USERNAME="${MESOS_USERNAME:-mesos}"
export MESOS_PASSWORD="${MESOS_PASSWORD:-test}"
export LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:8080}"
export STATE_FILE="${STATE_FILE:-/tmp/valkey-devtest-test.json}"
export START_ON_BOOT="${START_ON_BOOT:-true}"
export MESOS_DRY_RUN="${MESOS_DRY_RUN:-false}"

(
	cd "$(dirname "$0")/backend"
	go build -o valkey-mesos .
)

exec backend/valkey-mesos
