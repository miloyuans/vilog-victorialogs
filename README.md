# vilog-victorialogs

`vilog-victorialogs` is a monolithic Go backend for VictoriaLogs. It provides datasource management, multi-datasource concurrent search, automatic service/tag discovery, Mongo-backed caching, Telegram startup notifications, datasource retention proxy tasks, IP/CIDR whitelist protection, and JSON APIs for the embedded web UI.

## Features

- VictoriaLogs datasource CRUD and connectivity test
- Concurrent multi-datasource log search with partial success
- Gateway-side normalization for `timestamp`, `message`, `service`, `pod`, `datasource`, `labels`, and `raw`
- Automatic field discovery for service, pod, message, time, and common tags
- Mongo-backed cache for search results, service lists, and tag values
- Retention template and binding management
- VictoriaLogs delete task creation, polling, listing, and stop control
- Startup discovery plus Telegram notification fallback
- IP/CIDR whitelist and in-memory rate limiting
- Audit logging for config mutations and delete actions

## Linux Quick Start

### 1. Prepare config

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` with your MongoDB, VictoriaLogs, whitelist, and Telegram settings.

### 2. Run with local Go

Requirements:

- Go 1.22+
- MongoDB 7+

```bash
make run
```

### 3. Run with Docker Compose

```bash
make docker-up
```

The service listens on `:8080` by default.

## Linux Operations

Build:

```bash
make build
```

Run:

```bash
./bin/vilog-victorialogs -config config.yaml
```

Stop compose environment:

```bash
make docker-down
```

## Configuration

The config file is `config.yaml`. Environment overrides use the `VILOG_` prefix.

Common environment overrides:

- `VILOG_HTTP_ADDR`
- `VILOG_MONGO_URI`
- `VILOG_MONGO_DATABASE`
- `VILOG_LOG_LEVEL`
- `VILOG_CACHE_QUERY_TTL`
- `VILOG_SECURITY_WHITELIST`
- `VILOG_SECURITY_TRUST_PROXY_HEADERS`
- `VILOG_SECURITY_RATE_LIMIT_RPS`
- `VILOG_TELEGRAM_ENABLED`
- `VILOG_VL_REQUEST_RETRIES`

See [config.example.yaml](config.example.yaml) for the full config surface.

## API Summary

- `GET /api/datasources`
- `POST /api/datasources`
- `PUT /api/datasources/:id`
- `POST /api/datasources/:id/test`
- `POST /api/datasources/:id/discover`
- `POST /api/query/search`
- `GET /api/query/services`
- `GET /api/query/tags`
- `GET /api/query/tag-values`
- `GET /api/tags`
- `POST /api/tags`
- `PUT /api/tags/:id`
- `DELETE /api/tags/:id`
- `GET /api/retention/templates`
- `POST /api/retention/templates`
- `PUT /api/retention/templates/:id`
- `GET /api/retention/bindings`
- `POST /api/retention/bindings`
- `PUT /api/retention/bindings/:id`
- `POST /api/retention/run/:datasource_id`
- `GET /api/retention/tasks`
- `POST /api/retention/tasks/:id/stop`
- `GET /healthz`
- `GET /readyz`

See [openapi.yaml](openapi.yaml) for the API contract.

## Project Layout

```text
cmd/vilog-victorialogs      application entrypoint
internal/client             outbound VictoriaLogs client
internal/config             config loading and validation
internal/httpserver         Gin server and handlers
internal/middleware         request middleware
internal/model              API and persistence models
internal/scheduler          cron-backed retention scheduler
internal/service            datasource, query, discovery, cache, retention, telegram
internal/store/mongo        Mongo repositories and indexes
internal/util              shared helpers
```

## Notes

- Search sorting and pagination are controlled at the gateway.
- Datasource delete operations stay disabled unless `supports_delete=true`.
- Startup discovery and Telegram notification run asynchronously.
- This repository caches snapshots and query payloads only. It does not persist a second copy of logs.
