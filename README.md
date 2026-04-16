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
- Embedded web console at `GET /` for datasource, search, tag, and retention debugging

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

Embedded console:

```bash
curl -I http://127.0.0.1:8080/
```

Then open `http://127.0.0.1:8080/` in a browser.

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

## 数据源如何配置

当前代码里，`config.yaml` 不负责声明具体的 VictoriaLogs 数据源列表。代码结构上：

- 服务级配置由 [config.example.yaml](/C:/Users/mylo/Documents/milo2025/go/vilog-victorialogs/config.example.yaml:1) 和 [internal/config/config.go](/C:/Users/mylo/Documents/milo2025/go/vilog-victorialogs/internal/config/config.go:1) 加载
- 数据源本身通过 [internal/store/mongo/datasources.go](/C:/Users/mylo/Documents/milo2025/go/vilog-victorialogs/internal/store/mongo/datasources.go:1) 持久化到 MongoDB
- 管理入口是 `GET /` 内嵌控制台，或者 `POST /api/datasources`

也就是说，正确的使用方式是：

1. 先在 `config.yaml` 里配置服务监听、Mongo、白名单、Telegram、discover、retention 等运行参数
2. 启动服务
3. 打开 `http://127.0.0.1:8080/`
4. 在 `Datasources` 页面新增一个或多个 VictoriaLogs 数据源
5. 数据源保存后会写入 Mongo，后续查询、discover、retention 都从 Mongo 读取

如果你想直接走 API，也可以调用：

```bash
curl -X POST http://127.0.0.1:8080/api/datasources \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "prod-vl-01",
    "base_url": "http://127.0.0.1:9428",
    "enabled": true,
    "timeout_seconds": 15,
    "headers": {
      "AccountID": "0",
      "ProjectID": "0",
      "Authorization": ""
    },
    "query_paths": {
      "query": "/select/logsql/query",
      "field_names": "/select/logsql/field_names",
      "field_values": "/select/logsql/field_values",
      "stream_field_names": "/select/logsql/stream_field_names",
      "stream_field_values": "/select/logsql/stream_field_values",
      "facets": "/select/logsql/facets",
      "delete_run_task": "/delete/run_task",
      "delete_active_tasks": "/delete/active_tasks",
      "delete_stop_task": "/delete/stop_task"
    },
    "field_mapping": {
      "service_field": "",
      "pod_field": "",
      "message_field": "",
      "time_field": "_time"
    },
    "supports_delete": false
  }'
```

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
- `GET /api/datasources/:id/snapshot`
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
