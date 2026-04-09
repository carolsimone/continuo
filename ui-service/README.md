# UI Service

React dashboard + Express API server for the Continuo orchestration system.

## Purpose

The UI service provides a web-based dashboard for monitoring and managing Continuo workflows. It serves as the primary user interface for the orchestration system.

## Features

- **React frontend** — displays scheduler runs and their task statuses in real time
- **Express API server** — thin HTTP wrapper over the `state` service gRPC API
- **No own database** — all data is read from the state service via gRPC
- **Real-time updates** — WebSocket connections for live status monitoring
- **Task rerun functionality** — UI controls for retrying failed tasks
- **Responsive design** — Mobile-friendly interface for monitoring workflows

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/schedulers` | List scheduler runs (sorted newest first) |
| `GET` | `/api/schedulers/:id/tasks` | List tasks for a scheduler run |

Both endpoints normalise gRPC enum values (e.g. `SCHEDULER_STATUS_SUCCEEDED → succeeded`) and convert proto timestamps to ISO-8601.

## Technology Stack

### Frontend
- **Framework**: React 18 with TypeScript
- **Build Tool**: Vite for fast development
- **State Management**: React Context API
- **Styling**: CSS Modules for scoped styles
- **UI Components**: Custom components with responsive design

### Backend
- **Runtime**: Node.js
- **Framework**: Express.js
- **gRPC Client**: `@grpc/grpc-js` for state service communication
- **WebSockets**: Native WebSocket implementation

### Testing
- **Unit Tests**: Vitest for frontend components
- **Integration Tests**: Supertest for API endpoints
- **E2E Tests**: Playwright for browser automation

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STATE_GRPC_ADDR` | `localhost:50051` | Address of the state service gRPC server |
| `PORT` | `8090` | HTTP port to listen on |
| `NODE_ENV` | — | Set to `production` to serve the built React app |
| `WEBSOCKET_PATH` | `/ws` | Base path for WebSocket connections |
| `API_PREFIX` | `/api` | Base path for REST API endpoints |

### Configuration Files

- `vite.config.ts` — Vite configuration for frontend development
- `tsconfig.json` — TypeScript configuration
- `package.json` — Project dependencies and scripts

## Development

```bash
npm install
npm run dev        # starts Vite (port 5173) + tsx server (port 8090) concurrently
```

## Testing

```bash
npm test           # runs vitest unit tests
```

## Production build

```bash
npm run build      # builds client → dist/ and server → dist-server/
npm start          # serves built app on PORT (default 8090)
```

Or via Docker:

```bash
docker build -t continuo-ui .
docker run -e STATE_GRPC_ADDR=state:50051 -p 8090:8090 continuo-ui
```

With docker-compose (from the repo root) the `ui` service is already configured:

```bash
docker compose up -d ui
open http://localhost:8090
```
