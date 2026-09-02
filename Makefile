.PHONY: run build test clean

run:
	@set -e; $(MAKE) build; (cd backend && MESOS_MASTER=$${MESOS_MASTER} MESOS_USERNAME=$${MESOS_USERNAME:-} MESOS_PASSWORD=$${MESOS_PASSWORD:-} HTTP_ADDR=$${HTTP_ADDR:-:8080} ./valkey-mesos) & backend_pid=$$!; (cd frontend && npm run dev -- --host 0.0.0.0) & frontend_pid=$$!; trap 'kill $$backend_pid $$frontend_pid 2>/dev/null || true' INT TERM EXIT; wait $$backend_pid $$frontend_pid

build:
	(cd backend && go build -o valkey-mesos .)
	(cd frontend && npm install --include=dev && npm run build)

test:
	(cd backend && go test ./... && go vet ./...)
	(cd frontend && npm run build)

clean:
	rm -f backend/valkey-mesos
