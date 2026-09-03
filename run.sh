#!/bin/bash
set -euo pipefail

export MESOS_MASTER="devtest.lab.internal:5050"
export MESOS_SSL="true"
export MESOS_TLS_INSECURE="true"
export MESOS_USERNAME="mesos"
export MESOS_PASSWORD="test"
export LISTEN_ADDR="0.0.0.0:8080"
export START_ON_BOOT="true"

make run
make build
cd backend
./valkey-mesos

