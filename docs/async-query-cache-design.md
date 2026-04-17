# Async Query Cache Design

## Goals

- Keep gateway-side log queries fast and complete even when the source platform limits each request.
- Normalize source logs into a stable local format before UI filtering.
- Start rendering results as soon as the first normalized chunk is ready.
- Retain normalized query data for a configurable lifetime, default `24h`.

## Proposed Config

```yaml
cache:
  query_ttl: 5m
  service_list_ttl: 30m
  tag_values_ttl: 30m
  local_query_dir: ./data/query-cache
  local_query_retention: 24h
  source_chunk_size: 1000
  source_request_limit: 10000
  max_query_window: 100000
```

## Storage Model

### Mongo collections

- `query_jobs`
  - Job metadata and progress.
  - Fields: `job_id`, `request_hash`, `status`, `started_at`, `updated_at`, `expire_at`, `progress`, `totals`, `source_states`.
- `query_segments`
  - Segment index only, not raw payload.
  - Fields: `job_id`, `segment_id`, `datasource_id`, `service`, `start_ts`, `end_ts`, `row_count`, `file_path`, `created_at`, `expire_at`.
- `query_tags`
  - Optional secondary index for fast UI tag aggregation on cached results.
  - Fields: `job_id`, `field`, `value`, `count`, `expire_at`.

### Local files

- Directory layout:

```text
data/query-cache/
  2026-04-17/
    job_<hash>/
      meta.json
      segment_00001.ndjson.gz
      segment_00002.ndjson.gz
      ...
```

- Each segment stores normalized rows in time-desc order.
- Each row should already be merged and cleaned for UI use.

## Query Flow

1. UI submits `datasources + services + time range`.
2. Backend creates a `query_job`.
3. Worker splits source fetch by datasource and by offset/time chunks.
4. Each source request stays under `source_request_limit`.
5. Raw rows are normalized immediately:
   - map datasource fields into unified schema
   - merge continuation rows when the current row has no timestamp
   - preserve original raw payload inside `raw`
6. Normalized rows are appended to compressed local segment files.
7. Mongo receives only job state, segment index, and optional tag counters.
8. UI polls or receives stream updates and renders rows incrementally.
9. Expired jobs are deleted by TTL cleanup plus local file sweeper.

## Normalized Row Shape

```json
{
  "timestamp": "2026-04-17T10:46:21Z",
  "datasource": "vilogs-us",
  "service": "international-finance",
  "pod": "pod-1",
  "labels": {
    "namespace": "prod",
    "container": "app"
  },
  "message": "full merged message with continuation lines",
  "raw": {
    "...": "original source fields",
    "_merged_continuations": []
  }
}
```

## JVM / Continuation Merge Rule

- If a source row has no timestamp, append it to the latest normalized row from the same stream context.
- Preserve the appended raw rows in `raw._merged_continuations`.
- Never delete raw content.
- Sort final display rows by:
  1. timestamp descending
  2. message lowercase ascending
  3. datasource ascending
  4. service ascending

## UI Model

- Page and row count become virtual slicing over cached normalized results.
- Filtering is local on normalized content:
  - keyword contains / AND / OR
  - LogsQL-like local matcher
  - level filter
  - tag filter
- Export reads from cached segments instead of re-querying the source.

## Incremental Rendering

- Recommended API additions:
  - `POST /api/query/jobs`
  - `GET /api/query/jobs/:id`
  - `GET /api/query/jobs/:id/results?page=1&page_size=500`
  - `GET /api/query/jobs/:id/stream`
- The stream endpoint can be SSE or websocket.
- UI should refresh summary, histogram, source states, and current page as segments land.

## Cleanup

- Mongo TTL on `expire_at`.
- Background local sweeper removes expired `job_<hash>` directories.
- If a job is re-submitted within retention and the request hash matches, reuse cached segments.

## Why This Model

- Source-platform limits are handled by chunking.
- UI speed improves because filtering and pagination no longer depend on repeated source queries.
- Export becomes reliable for large result sets.
- Mongo stays small because large payloads live on disk, not inside documents.
