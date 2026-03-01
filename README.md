# Local Connector Registration

This project implements only the one-time connector registration/pairing flow with OnlineMainServer.

## First-time registration

```bash
python -m connector register --online-url "https://example.com" --enroll-token "YOUR_ENROLL_TOKEN"
```

You can also provide values through environment variables:

- `ONLINE_URL` (required)
- `ENROLL_TOKEN` (required for first registration if not passed via CLI)
- `CONNECTOR_IDENTITY_PATH` (optional, default `./data/connector_identity.json`)
- `CONNECTOR_LABEL` (optional)
- `CONNECTOR_OS` (optional)
- `CONNECTOR_HOSTNAME` (optional)
- `CONNECTOR_VERSION` (optional)

## Identity file location

By default, identity is stored at `./data/connector_identity.json`.
Use `--identity-path` or `CONNECTOR_IDENTITY_PATH` to change it.

Identity JSON format:

```json
{
  "device_id": "string",
  "device_token": "string",
  "registered_at": "ISO-8601",
  "online_url": "string"
}
```

## Token error meanings

- `TOKEN_INVALID`: enroll token is not recognized
- `TOKEN_EXPIRED`: enroll token is expired
- `TOKEN_USED_UP`: enroll token usage limit has been reached

## Security notes

- The connector never logs raw `enroll_token` or `device_token`.
- Identity writes are atomic (temp file + rename).
- On Unix, identity file permissions are set to `600` when possible.
- On Windows, restrictive file permissions are best-effort due to OS differences.

## Service Mode

The LocalConnector can now run as a local gateway service. It auto-registers on startup if needed, accepts scan results from the Brain via a local HTTP API, and forwards them to the OnlineMainServer asynchronously.

### Running the Service

```bash
python -m connector serve
```

### Endpoints

- `GET /health`: Returns the health status and whether the connector is registered.
- `POST /update`: Accepts a `ScanResult` JSON payload, stores it in a local SQLite outbox, and returns `202 Accepted` immediately.

### `/update` payload example (Brain -> LocalConnector)

```bash
curl -X POST "http://127.0.0.1:8600/update" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id":"s1",
    "image_id":"s1-0001",
    "timestamp":"2026-03-01T23:40:00Z",
    "weight_grams":120.5,
    "fruits":[]
  }'
```

Expected response:

```json
{"status":"accepted","image_id":"s1-0001"}
```

### Forwarded request format (LocalConnector -> OnlineMainServer)

Headers:

- `Authorization: Bearer <device_token>`
- `Idempotency-Key: <image_id>`

Body:

```json
{
  "envelope_version": "v1",
  "sent_at": "2026-03-01T23:40:00Z",
  "image_id": "s1-0001",
  "scan_result": {
    "session_id": "s1",
    "image_id": "s1-0001",
    "timestamp": "2026-03-01T23:40:00Z",
    "weight_grams": 120.5,
    "fruits": []
  }
}
```

### Configuration (Environment Variables)

In addition to the registration variables, the service mode supports:

- `SERVICE_HOST` (default `0.0.0.0`)
- `SERVICE_PORT` (default `8600`)
- `OUTBOX_DB_PATH` (default `./data/localconnector.db`)
- `ONLINE_UPDATE_PATH` (default `/update`)
- `FORWARD_POLL_INTERVAL_MS` (default `500`)
- `FORWARD_TIMEOUT_SECONDS` (default `8`)
- `OUTBOX_MAX_ATTEMPTS` (default `20`)
- `BACKOFF_BASE_SECONDS` (default `1`)
- `BACKOFF_MAX_SECONDS` (default `60`)

## WebSocket command channel

When service mode starts, LocalConnector now also opens a persistent outbound WebSocket connection to OnlineMainServer.

### Connection behavior

- WS URL is derived from identity `online_url`:
  - `http://...` -> `ws://...`
  - `https://...` -> `wss://...`
- WS path defaults to `/connector/v1/ws` and is configurable via `ONLINE_WS_PATH`.
- Auth header is always `Authorization: Bearer <device_token>`.
- On connect/reconnect, LocalConnector sends a `hello` envelope with safe device metadata.
- LocalConnector sends a `heartbeat` envelope every `WS_HEARTBEAT_SECONDS`.
- Disconnects trigger exponential reconnect backoff using:
  - `WS_RECONNECT_BASE_SECONDS` (default `1`)
  - `WS_RECONNECT_MAX_SECONDS` (default `30`)

### WS message envelope

All WS messages use:

```json
{
  "type": "hello|heartbeat|request|ack|response|error",
  "ts": "ISO-8601",
  "message_id": "uuid",
  "device_id": "string",
  "payload": {}
}
```

### Supported request allowlist

Only these `request_type` values are accepted:

- `ping`
- `device.info`
- `connector.stats`
- `camera.capture`

Unknown request types are acknowledged with `accepted=false` and answered with `status="rejected"` + `error.code="rejected_not_allowed"`.

Command execution guarantees:

- At most one command executes at a time.
- Each command has hard timeout via `COMMAND_TIMEOUT_SECONDS` (default `20`).
- Responses are cached by `request_id` briefly to replay idempotent retries.

## camera.capture contract

`camera.capture` captures one image from the local camera service, uploads bytes to OMS blob storage via HTTP, and returns metadata in WS `response.data`.

### Inputs

- Optional `params` fields passed through to camera capture endpoint:
  - `resolution`
  - `format`
  - `quality`

### Internal flow

1. `POST {CAMERA_SERVICE_URL}/capture` with optional JSON params.
2. Read `image_id` and image URL/path from response.
3. Download bytes from camera service.
4. Compute `sha256`, `size_bytes`, and `content_type`.
5. Upload bytes to OMS:
   - `POST {online_url}{OMS_BLOB_UPLOAD_PATH}`
   - multipart `file`
   - auth header `Bearer <device_token>`
   - fields `image_id`, `content_type`, `sha256`
6. Return `blob_id` and metadata over WS response payload (bytes are never sent over WS).

### camera.capture success payload

```json
{
  "image_id": "...",
  "blob_id": "...",
  "sha256": "...",
  "size_bytes": 123,
  "content_type": "image/jpeg"
}
```

### camera.capture error codes

- `camera_unavailable` for capture endpoint failures/invalid capture metadata
- `camera_fetch_failed` when image bytes cannot be downloaded
- `blob_upload_failed` when OMS blob upload fails

### Additional service environment variables

- `ONLINE_WS_PATH` (default `/connector/v1/ws`)
- `WS_HEARTBEAT_SECONDS` (default `15`)
- `WS_RECONNECT_BASE_SECONDS` (default `1`)
- `WS_RECONNECT_MAX_SECONDS` (default `30`)
- `COMMAND_TIMEOUT_SECONDS` (default `20`)
- `CAMERA_SERVICE_URL` (default `http://localhost:8200`)
- `OMS_BLOB_UPLOAD_PATH` (default `/connector/v1/blobs`)

