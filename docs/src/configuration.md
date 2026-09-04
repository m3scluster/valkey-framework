# Configuration

The Valkey Mesos Control Plane framework uses environment variables for all configuration settings. These are centralized in the backend's `init.go` file to ensure consistent initialization across components.

## Environment Variables

### Required Variables

- `MESOS_MASTER`: The address of the Mesos master (for example, `mesos.example.test:5050`)
- `MESOS_USERNAME`: Username for Mesos authentication  
- `MESOS_PASSWORD`: Password for Mesos authentication
- `DRY_RUN`: Set to `true` or `false` to control run mode

### Example Configuration

```bash
MESOS_MASTER=mesos.example.test:5050 \
MESOS_USERNAME=example-user \
MESOS_PASSWORD="$MESOS_PASSWORD" \
DRY_RUN=true \
make run
```

In dry-run mode (`DRY_RUN=true`):
- The backend scheduler runs in deterministic test mode
- Nodes and scaling operations are visible but not actually deployed on Mesos
- This enables safe local testing without cluster interaction

In production mode (`DRY_RUN=false`):
- Actual Mesos framework registration and streaming integration is enabled
- Scale operations result in real task deployment on the Mesos cluster
- Note: Framework registration/streaming integration is currently intentionally disabled

## Configuration Behavior

Environment variables are initialized during application startup through Go's `init()` functions, ensuring all components have access to the configuration at runtime. This provides consistent behavior across both backend and frontend components.

The framework is designed to be robust against missing or malformed configuration by providing clear error messages when critical values aren't provided.