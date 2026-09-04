# Operation & API

The Valkey Mesos Control Plane exposes a comprehensive RESTful API for managing and monitoring Valkey instances within a Mesos environment.

## API Endpoints

### Health Check
- `GET /api/health` - Returns system health status

### Cluster Management  
- `GET /api/cluster` - Retrieves cluster information and status
- `PUT /api/cluster/scale` - Scales the cluster to the specified number of nodes
  - Request body: `{ "nodes": 3..100 }`
  - Response: Acknowledges scaling request (in dry-run mode, will show error)

### Metrics
- `GET /api/metrics` - Exposes system metrics 
  - In dry-run mode: Reports `unknown_no_valkey_endpoint` since no real Valkey processes are running
  - In production: Collects Valkey `INFO` via TCP connection

## Startup Process

The framework can be started with:
```bash
make run
```

This will start:
- Backend service on `http://localhost:8080`
- Frontend interface on `http://localhost:5173`

## Deployment Mode

### Dry Run Mode (`DRY_RUN=true`)
- Operations are visible but not actually performed on Mesos
- Enables safe local testing
- Provides deterministic behavior for validation

### Production Mode (`DRY_RUN=false`) 
- Activates the explicit production seam
- The Mesos-v1 framework registration/streaming integration is intentionally not yet activated
- Backend startup will fail with an explanatory error message on scaling instead of pretending to have a successful deployment status

## Error Handling

The framework provides clear error messages:
- Scaling operations in dry-run mode will fail with explanatory messages
- This prevents false success reporting when the actual Mesos integration isn't available

## Monitoring Integration  

The metrics system collects information from Valkey instances:
- In dry-run: Shows `unknown_no_valkey_endpoint` 
- In production: Queries Valkey `INFO` via TCP for real-time data