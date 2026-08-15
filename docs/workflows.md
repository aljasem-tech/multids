# Data movement workflows

`multids.workflows` provides the small amount of orchestration needed to make connectors useful together. It does not
create or close connectors; callers retain ownership of their lifecycle and credentials.

## Copy an object without buffering it

`copy_stream` passes the source byte stream straight into the destination writer. `source_args` and `destination_args`
are the positional arguments each connector expects after the stream argument.

```python
from multids.connectors.local import LocalConnector
from multids.connectors.s3 import S3Connector
from multids.workflows import copy_stream

local = LocalConnector(base_path="./exports")
s3 = S3Connector(region_name="eu-central-1")

result = await copy_stream(
    local,
    s3,
    source_args=("daily/events.jsonl",),
    destination_args=("my-bucket", "exports/events.jsonl"),
)
print(result.bytes_copied, result.chunks_copied)
```

The example is equally useful for S3-to-local copies; swap the connectors and arguments. The entire object is never held
in memory by the workflow helper.

## Transform and batch records

Record helpers accept either an `Iterable` or an `AsyncIterable`. Mapping and filtering accept normal or async
functions. `concurrency` bounds active async work and still preserves source order.

```python
from multids.workflows import batch_records, filter_records, map_records

async def enrich(row):
    return {**row, "normalized": row["name"].strip().lower()}

rows = await get_rows_somehow()
enriched = map_records(rows, enrich, concurrency=10)
valid = filter_records(enriched, lambda row: bool(row["normalized"]))

async for batch in batch_records(valid, size=500):
    await write_batch_somewhere(batch)
```

## Practical destinations

Wrap a connector-specific bulk operation in a small batch writer and use
`copy_records` to obtain a success summary.

```python
from multids.workflows import copy_records

# Athena -> SQL
async def insert_rows(batch):
    await mysql.bulk_insert("events", batch)

summary = await copy_records(athena.query_stream("SELECT * FROM events"), insert_rows)

# SQL -> OpenSearch
async def index_rows(batch):
    await opensearch.bulk_index("events", batch)

summary = await copy_records(mysql.fetch_rows("SELECT * FROM events"), index_rows, batch_size=500)
```

### S3 JSONL to OpenSearch

```python
import json

from multids.workflows import copy_records

async def json_lines():
    remainder = b""
    async for chunk in s3.read_stream("my-bucket", "exports/events.jsonl"):
        lines = (remainder + chunk).split(b"\n")
        remainder = lines.pop()
        for line in lines:
            if line:
                yield json.loads(line)
    if remainder:
        yield json.loads(remainder)

async def index_rows(batch):
    await opensearch.bulk_index("events", batch)

summary = await copy_records(json_lines(), index_rows, batch_size=500)
```

## Failures, cancellation, and checkpoints

The helpers propagate exceptions and `asyncio.CancelledError`; they do not silently retry or swallow a failed
destination write. A failed stream may leave a partial destination object, while records in batches submitted before the
failure remain written. Use idempotent destination writes where possible.

For resumable S3 destinations, pass `checkpoint_path` and `resume` through
`destination_kwargs` in `copy_stream`. The S3 connector owns its checkpoint and multipart cleanup behavior.
