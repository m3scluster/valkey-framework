.PHONY: run build test clean

run:
	cd frontend && exec npm run dev -- --host 0.0.0.0 &

stop:
	@curl -fsS -X POST "http://127.0.0.1:$${STOP_PORT:-8080}/api/stop" >/dev/null 2>&1 || true
	@if test -f /tmp/valkey-mesos-framework.pids; then while read -r pid; do test -z "$$pid" || kill "$$pid" 2>/dev/null || true; done < /tmp/valkey-mesos-framework.pids; rm -f /tmp/valkey-mesos-framework.pids; fi

build:
	(cd backend && go build -o valkey-mesos .)
	(cd frontend && npm install --include=dev && npm run build)

test:
	(cd backend && go test ./... && go vet ./...)
	(cd frontend && npm run build)

clean:
	rm -f backend/valkey-mesos
