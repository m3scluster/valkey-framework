# Architecture

The Valkey Mesos Control Plane follows a dual-implementation approach with clear separation between backend and frontend components.

## Backend Architecture

The backend is implemented in Go and focuses on:

- **Process Management**: Starting, stopping, and managing the lifecycle of Valkey processes
- **HTTP Server**: Exposing RESTful API endpoints for external communication
- **Mesos Integration**: Communicating with Mesos master for task scheduling and management
- **Configuration**: Centralized environment variable handling via `init()` functions

### Key Components

1. `backend/app.go`: Handles process initialization, HTTP server lifecycle, and application startup logic
2. `backend/init.go`: Central configuration initialization for all environment variables 
3. `backend/main.go`: Contains main API logic, domain models, and Mesos abstraction layer
4. `backend/scheduler/`: Scheduler components with reconciliation mechanisms and task-oriented scheduling

## Frontend Architecture

The frontend is built with React and Vite offering:

- **Live Polling Interface**: Real-time monitoring of system status  
- **Admin Dashboard**: User-friendly interface for managing Valkey instances
- **Component-Based Design**: Modular architecture following React best practices

## Integration Pattern

The framework maintains separation between:
- Local dry-run implementation for safe testing 
- Production Mesos integration for actual deployment

This design allows for validation of configurations and behavior without requiring access to a live Mesos cluster during development.

## External Dependencies

The framework integrates with:
- Mesos master for task scheduling
- Valkey instances for actual database operations  
- Environment variables for configuration
- HTTP endpoints for external API interaction

## Data Flow

1. User initiates commands via frontend or API
2. Backend processes requests and manages Valkey lifecycle
3. Mesos scheduler coordinates task deployment on cluster nodes
4. Frontend polls backend for real-time updates and status information