"""Opt-in integration coverage for Amazon Athena.

Required environment:

* ``MULTIDS_ATHENA_DATABASE`` - database that the caller may create tables in
* ``MULTIDS_ATHENA_OUTPUT_LOCATION`` - ``s3://bucket/prefix/`` for query results
* ``MULTIDS_ATHENA_REGION`` (or ``AWS_REGION``)
* standard AWS credentials
* ``MULTIDS_ATHENA_WORKGROUP`` - optional workgroup

Scenarios: authentication, DDL + Unicode, streamed results, cleanup and failure
recovery (invalid SQL surfaces a typed error).
"""

from __future__ import annotations

import os
import uuid

import pytest

from multids.connectors.athena import AthenaConnector
from multids.errors import ConnectorError

pytestmark = [pytest.mark.real_integration, pytest.mark.asyncio]


def _connector() -> AthenaConnector | None:
    database = os.environ.get("MULTIDS_ATHENA_DATABASE")
    output = os.environ.get("MULTIDS_ATHENA_OUTPUT_LOCATION")
    if not database or not output:
        return None
    return AthenaConnector(
        region_name=os.environ.get("MULTIDS_ATHENA_REGION") or os.environ.get("AWS_REGION"),
        workgroup=os.environ.get("MULTIDS_ATHENA_WORKGROUP") or None,
        timeout=120.0,
        max_wait=600.0,
    )


def _database() -> str:
    return os.environ["MULTIDS_ATHENA_DATABASE"]


def _qualified(table: str) -> str:
    return f"{_database()}.{table}"


async def test_athena_ping_and_unicode_ctas():
    connector = _connector()
    if connector is None:
        pytest.skip("Set MULTIDS_ATHENA_DATABASE and MULTIDS_ATHENA_OUTPUT_LOCATION to run Athena tests")

    output = os.environ["MULTIDS_ATHENA_OUTPUT_LOCATION"].rstrip("/")
    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        assert await connector.ping() is True

        # CTAS with Unicode content.
        await connector.start_query(
            f"CREATE TABLE {_qualified(table)} "
            f"WITH (format='PARQUET', external_location='{output}/{table}/') AS "
            "SELECT 1 AS id, 'héllo 世界' AS name"
        )
        # Wait for completion by polling the most recent execution is awkward;
        # instead run a query that implicitly waits for the table to exist.
        rows = await connector.fetch_all(f"SELECT id, name FROM {_qualified(table)}")
        assert rows == [{"id": "1", "name": "héllo 世界"}]
    finally:
        try:
            qid = await connector.start_query(f"DROP TABLE IF EXISTS {_qualified(table)}")
            await connector.wait_query(qid)
        except Exception:
            pass
        await connector.close()


async def test_athena_stream_and_failure_recovery():
    connector = _connector()
    if connector is None:
        pytest.skip("Athena integration environment not configured")

    try:
        # Invalid SQL must surface a typed ConnectorError, not hang.
        qid = await connector.start_query("SELECT * FROM definitely_missing_table_xyz")
        exec_resp = await connector.wait_query(qid)
        state = exec_resp.get("QueryExecution", {}).get("Status", {}).get("State")
        assert state == "FAILED"

        with pytest.raises(ConnectorError):
            async for _ in connector.query_stream("SELECT * FROM definitely_missing_table_xyz"):
                pass
    finally:
        await connector.close()
