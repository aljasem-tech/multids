"""Opt-in integration coverage for a real SQL Server instance.

Set ``MULTIDS_MSSQL_DSN`` to a full ODBC connection string, for example
``DRIVER={ODBC Driver 18 for SQL Server};SERVER=host,1433;DATABASE=db;UID=user;PWD=pass``.

Scenarios: authentication, Unicode, pagination, large streams, cleanup and
failure recovery.
"""

from __future__ import annotations

import os
import uuid

import pytest

from multids.connectors.mssql import MSSQLConnector
from multids.errors import ConnectorError

pytestmark = [pytest.mark.real_integration, pytest.mark.asyncio]


def _connector() -> MSSQLConnector | None:
    dsn = os.environ.get("MULTIDS_MSSQL_DSN")
    if not dsn:
        return None
    return MSSQLConnector(dsn=dsn, timeout=60.0)


async def test_mssql_auth_ping_and_unicode():
    connector = _connector()
    if connector is None:
        pytest.skip("Set MULTIDS_MSSQL_DSN to run real SQL Server integration tests")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        assert await connector.ping() is True
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name NVARCHAR(255))")
        await connector.execute(f"INSERT INTO {table} (id, name) VALUES (1, ?)", ["héllo 世界"])
        rows = await connector.fetch_rows(f"SELECT id, name FROM {table}")
        assert rows == [{"id": 1, "name": "héllo 世界"}]
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


async def test_mssql_pagination_and_large_stream():
    connector = _connector()
    if connector is None:
        pytest.skip("MULTIDS_MSSQL_DSN not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    total = 500
    try:
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, grp INT)")
        rows = [[i, i % 10] for i in range(total)]
        await connector.bulk_insert(table, ["id", "grp"], rows, batch_size=100)

        base = f"SELECT id FROM {table} ORDER BY id"
        page1 = await connector.fetch_rows(f"{base} OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")
        page2 = await connector.fetch_rows(f"{base} OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY")
        assert [r["id"] for r in page1] == list(range(10))
        assert [r["id"] for r in page2] == list(range(10, 20))

        streamed = [row async for row in connector.fetch_iter(f"SELECT id FROM {table} ORDER BY id")]
        assert len(streamed) == total
        assert [r["id"] for r in streamed] == list(range(total))
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


async def test_mssql_failure_recovery_raises_typed_error():
    connector = _connector()
    if connector is None:
        pytest.skip("MULTIDS_MSSQL_DSN not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY)")
        with pytest.raises(ConnectorError):
            await connector.execute(f"INSERT INTO {table} (missing_col) VALUES (1)")
        assert await connector.ping() is True
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()
