# Streaming Load Raw Mode Design

## Overview

Add `PUT /v1/streaming_load` as an alternative ingest path for raw mode (`IsJsonTransform=false`).
Replaces the two-step `uploadToStage + copyInto` with a single HTTP multipart request.
All existing code paths remain untouched.

## Config

New field in `config/config.go`:
```go
UseStreamingLoad bool `json:"useStreamingLoad" default:"false"`
```

New CLI flag in `main.go`:
```go
flag.BoolVar(&cfg.UseStreamingLoad, "use-streaming-load", false, "use /v1/streaming_load HTTP endpoint (raw mode only)")
```

Validation rules added to `validateConfig`:
- `UseStreamingLoad && IsJsonTransform` → panic (streaming load is raw mode only)
- `UseStreamingLoad && UseReplaceMode` → panic (mutually exclusive)

## Architecture

### Path selection in `IngestData`

```
IngestData(batch)
  ├── reWriteTheJsonData(batch)          // unchanged
  ├── if UseStreamingLoad && !IsJsonTransform
  │     └── streamingLoad(ctx, data)    // NEW
  └── else (existing path)
        ├── generateNDJsonFile(data)
        ├── uploadToStage(file)
        └── copyInto(stage)
```

### `streamingLoad` implementation

Location: `ingest_databend.go`

Signature:
```go
func (ig *databendIngester) streamingLoad(ctx context.Context, batchJsonData []string) (int, error)
```

Steps:
1. `godatabend.ParseDSN(DatabendDSN)` → extract host, user, password, warehouse, SSLMode
2. Build endpoint: `https://host/v1/streaming_load` (http if SSLMode=="disable")
3. Build multipart body via `io.Pipe` — writer goroutine streams NDJson lines, no temp file
4. Set headers: `Content-Type: multipart/form-data`, `X-Databend-SQL`, `Authorization: Basic`, `X-Databend-Warehouse` (if warehouse present)
5. Execute `PUT` with `http.Client{Timeout: 5*time.Minute}`
6. Parse response: non-2xx → wrap as `ErrStreamingLoadFailed`
7. Log: `"streaming load %s rows=%d bytes=%d cost=%s"`

### SQL template

```sql
INSERT INTO <table> FROM @_databend_load FILE_FORMAT=(type=NDJSON missing_field_as=FIELD_DEFAULT COMPRESSION=AUTO)
```

Note: `DISABLE_VARIANT_CHECK` is a COPY INTO option and does not apply here.
JSON validity is already enforced upstream in `prepareRawData`.

### Error type and retry

```go
var ErrStreamingLoadFailed = errors.New("streaming load failed")
```

Added to `DoRetry` retry condition alongside `ErrUploadStageFailed` and `ErrCopyIntoFailed`.
All failures wrapped as ErrStreamingLoadFailed are retried via DoRetry (consistent with existing ErrUploadStageFailed / ErrCopyIntoFailed behavior). 4xx errors will exhaust retries and surface to the caller — same as existing behavior.

### HTTP client placement

`databendIngester` gains a new field:
```go
httpClient *http.Client
```

Initialized in `NewDatabendIngester` with `Timeout: 5*time.Minute`.
Injected via constructor for testability.

## Endpoint derivation

| DSN field | HTTP request |
|-----------|-------------|
| `cfg.Host` (host:port) | URL host |
| `cfg.SSLMode == "disable"` | `http://` scheme |
| otherwise | `https://` scheme |
| `cfg.User` / `cfg.Password` | Basic Auth |
| `cfg.Warehouse` (if non-empty) | `X-Databend-Warehouse` header |

## Tests

### Unit tests (`ingest_databend_test.go`)

- `TestStreamingLoadBuildRequest` — verify multipart body, headers, SQL from a mock HTTP server
- `TestStreamingLoadRetryOn5xx` — mock server returns 500 twice then 200; verify retry fires
- `TestStreamingLoadNoRetryOn4xx` — mock server returns 400; verify no retry

### E2E test (`ingest_databend_test.go`)

- `TestIngestDataWithStreamingLoad` — uses `TEST_DATABEND_DSN` env var (Databend Cloud DSN)
  - Creates table `default.test_ingest_streaming`
  - Ingests one batch via `UseStreamingLoad=true, IsJsonTransform=false`
  - Queries and asserts row count and field values
  - Defers `DROP TABLE`

## Files changed

| File | Change |
|------|--------|
| `config/config.go` | Add `UseStreamingLoad` field |
| `main.go` | Add CLI flag + validation rules |
| `ingest_databend.go` | Add `httpClient` field, `streamingLoad` method, branch in `IngestData`, `ErrStreamingLoadFailed`, `buildStreamingLoadURL` helper |
| `ingest_databend_test.go` | Add unit + E2E tests |
