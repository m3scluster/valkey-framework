# Development

This section covers development practices and guidelines for working with the Valkey Mesos Control Plane framework.

## Prerequisites

- Go 1.19 or higher
- Node.js and npm 
- Mesos cluster access (for production deployment)
- Basic knowledge of Docker and containerization

## Project Structure

The codebase is organized as follows:
```
backend/          # Go backend implementation
├── app.go        # Process start, HTTP server, lifecycle management  
├── init.go       # Environment variable initialization
├── main.go       # API, domain models, Mesos abstraction
├── scheduler/    # Scheduler components with reconciliation
├── mesos/        # Mesos client implementations
└── utils/        # Utility functions

frontend/         # React/Vite admin interface 
├── src/          # Source files
├── public/       # Static assets  
└── package.json  # Dependencies and scripts

docs/             # Documentation site
├── book.toml     # mdBook configuration
├── SUMMARY.md    # Table of contents
└── *.md          # Documentation chapters
```

## Build Process

The framework uses a Makefile for managing build operations:

### Available Make Targets
- `make run`: Start the development servers (frontend + backend)
- `make build`: Compile both backend and frontend components
- `make test`: Run all tests for backend and frontend
- `make clean`: Remove build artifacts

### Backend Build
```bash
cd backend && go build -o valkey-mesos .
```

### Frontend Build  
```bash
cd frontend && npm install --include=dev && npm run build
```

## Testing

The framework includes comprehensive tests across both backend and frontend components:
- Backend: Unit tests, integration tests, lifecycle tests 
- Frontend: Component testing and build validation

Running tests:
```bash
make test
```

## Development Workflow

1. Make changes to backend or frontend code
2. Test locally with `make run`  
3. Validate changes with `make test`
4. Commit and push to version control

## Code Quality

All contributions should follow:
- Go coding standards
- React best practices
- Clear documentation in code comments