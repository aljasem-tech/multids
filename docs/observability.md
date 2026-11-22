# Reliability and Observability

Every remote connector (`s3`, `sql`, `mssql`, `athena`, `opensearch`, and their
synchronous counterparts) shares one operational model:

* the same retry/backoff policy for safe transient operations;
* the same backend-exception to `multids.errors` mapping;
* the same structured `start` / `completion` / `retry` / `failure` events.

## Retries and timeouts

Each connector accepts `max_retries`, `backoff_factor`, `max_backoff` and (where
meaningful) `timeout`. These build a `multids.reliability.RetryConfig`.

* Read and statement operations are retried with exponential backoff on
  transient failures (connection errors, server 5xx, timeouts).
* Non-idempotent writes (`bulk_insert`, `write_records`, `execute_many`) are
  bounded by a timeout but **not** retried, so a retry cannot duplicate rows.
* Cancellation (`asyncio.CancelledError`) always propagates immediately.

```python
from multids.connectors.sql import MySQLConnector

conn = MySQLConnector(
    "mysql+asyncmy://user:pass@host/db",
    max_retries=5,
    backoff_factor=0.5,
    max_backoff=30.0,
    timeout=30.0,
)
```

## Error hierarchy

Backend exceptions are normalized so callers never import a driver SDK:

```python
from multids.errors import (
    ConnectorDependencyError,   # optional dependency missing
    ConnectorConnectionError,   # unreachable backend / timeout
    ConnectorSchemaError,       # invalid statement or schema
    ConnectorDataError,         # data-level rejection
    ConnectorOperationError,    # anything else
)

from multids.connectors.s3 import S3Connector

try:
    await S3Connector().read_bytes("bucket", "missing")
except ConnectorOperationError as exc:
    ...  # 404 / rejected request
except ConnectorConnectionError as exc:
    ...  # retry or surface a connectivity problem
```

`multids.error_mapping` holds the per-backend mappers
(`map_sqlalchemy_error`, `map_botocore_error`, `map_pyodbc_error`). A
`multids.errors.ConnectorError` raised by a connector is passed through
unchanged.

## Structured telemetry

Connectors emit structured events through the standard `logging` module at
`DEBUG` level on the `multids.connectors` logger. Each event carries:

| field         | meaning                                              |
| ------------- | ---------------------------------------------------- |
| `event`       | `start`, `completion`, `retry` or `failure`          |
| `backend`     | `s3`, `sql`, `mssql`, `athena`, `opensearch`         |
| `operation`   | dotted operation name, e.g. `sql.execute`            |
| `duration_seconds` | elapsed time for completion/failure events      |
| `resource`    | optional **safe** identifier (bucket/key, table)     |
| `retry_number`| retry attempt for `retry` events                     |
| `error_type`  | exception class name for `retry`/`failure` events    |

### Redaction guarantee

`TelemetryEvent` has a **closed set of fields**. There is no payload, no SQL
text, no query parameters and no credentials. `resource` is an operator-chosen
safe identifier; never pass a secret as `resource`. This is why credentials,
SQL parameters and record payloads cannot appear in telemetry.

### Optional tracing and metrics

Pass `tracer` and/or `metrics` callbacks to `multids.telemetry.Telemetry` and
inject it into any connector. No observability provider is required.

```python
import logging
from multids.telemetry import Telemetry, TelemetryEvent
from multids.connectors.s3 import S3Connector

logging.basicConfig(level=logging.DEBUG)

def tracer(event: TelemetryEvent) -> None:
    ...  # forward to your tracing backend

def metrics(event: TelemetryEvent) -> None:
    ...  # increment counters / histograms

telemetry = Telemetry(tracer=tracer, metrics=metrics)
conn = S3Connector(telemetry=telemetry)
```

Callbacks are isolated: an exception raised by a callback is logged and
swallowed, so telemetry can never break data movement. Every connector exposes a
`telemetry` attribute (a default `Telemetry()` when none is supplied), so the
same instrumentation works uniformly across backends.
