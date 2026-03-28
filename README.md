---
title: Unified DB MCP Server
emoji: 🗄️
colorFrom: blue
colorTo: indigo
sdk: docker
app_port: 7860
pinned: false
---

# Unified DB MCP Server

Unified DB MCP Server is a production-focused MCP service for cross-database schema extraction and schema migration.
It exposes MCP tools over a single streamable HTTP endpoint.

## What This Project Does

- Connects to a source database using connector-specific credentials.
- Extracts schema metadata into a normalized internal representation.
- Converts schema types between database engines.
- Applies converted schema to a target database.
- Supports dry-run and table-filtered migrations.

## Supported Databases

- Supabase
- PostgreSQL
- MySQL
- MariaDB
- SQLite
- MongoDB
- SQL Server
- Cassandra

## Core Features

- MCP tool interface for client integrations (Cursor, MCP-compatible agents, custom clients).
- Streamable HTTP MCP endpoint: `/unified-db/mcp`.
- Header-based credentials support for API calls.
- Dockerized runtime with container healthcheck.

## Project Structure

```text
unified-db-mcp-server/
├── unified_db_mcp/
│   ├── server.py                  # MCP server entrypoint and routes
│   ├── config.py                  # App host/port/path and supported DB list
│   ├── schema_migrate.py          # Migration orchestration + config + conversion logic
│   ├── tools/
│   │   ├── migrate_schema_tool.py # High-level migration tool wrappers
│   │   └── schema_connector_tools.py
│   ├── database_connectors/       # Per-database connector implementations
│   └── helpers/                   # Shared schema and type-conversion utilities
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
└── README.md
```

## MCP Tools Exposed

- `connect_database`
- `extract_schema`
- `apply_schema`
- `migrate_schema`

## Runtime Configuration

Core server environment variables:

- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `7860`)

Credential/environment defaults are defined in `unified_db_mcp/schema_migrate.py`.
At runtime, request-level credentials can override defaults via tool arguments or request headers.

## API Endpoints

- `GET /unified-db/mcp`
  - MCP discovery and streamable transport endpoint.
  - This is the only exposed application endpoint.
  - Requires appropriate `Accept` header for streamable behavior.

## Production Deployment (Docker)

### Build

```bash
docker build -t unified-db-mcp .
```

### Run

```bash
docker run --rm -d \
  --name unified-db-mcp \
  -p 7860:7860 \
  -e HOST=0.0.0.0 \
  -e PORT=7860 \
  unified-db-mcp
```

### Verify

```bash
docker ps --filter "name=unified-db-mcp"
docker logs unified-db-mcp
```

### Stop

```bash
docker stop unified-db-mcp
```

## Local Development

```bash
python -m venv venv
source venv/bin/activate  # Windows: .\venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
python -m unified_db_mcp.server
```

## MCP Endpoint Validation

A plain request may return `406 Not Acceptable` if the streamable header is missing.

Use:

```bash
curl -i -H "Accept: text/event-stream" http://localhost:7860/unified-db/mcp
```

Expected response includes:

- `HTTP/1.1 200 OK`
- `content-type: text/event-stream`

## Operational Notes

- Run dry-run migrations before applying changes on production targets.
- Keep credentials in environment variables or secure secret stores.
- Avoid embedding credentials in source files or committed artifacts.
- Use container logs first when diagnosing migration or connectivity failures.
