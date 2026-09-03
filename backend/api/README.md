# API

HTTP endpoint registration and handlers live in `../api.go`. Keeping the transport layer separate from Mesos and scheduler logic makes the endpoint contract explicit while preserving the backend's single executable package.
