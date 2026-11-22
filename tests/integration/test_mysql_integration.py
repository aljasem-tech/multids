"""Opt-in integration coverage for a real MySQL-compatible service.

Set ``MULTIDS_MYSQL_URL`` to an async SQLAlchemy URL, for example
``mysql+asyncmy://user:pass@host:3306/dbname``. The test creates and drops its
own uniquely-named table.

Scenarios: authentication, Unicode, pagination, large streams, cleanup and
failure recovery.
"""

from __future__ import annotations

import os
import uuid

import pytest

from multids.connectors.sql import MySQLConnector
from multids.errors import ConnectorError

pytestmark = [pytest.mark.real_integration, pytest.mark.asyncio]


def _url() -> str | None:
    return os.environ.get("MULTIDS_MYSQL_URL")


def _connector() -> MySQLConnector | None:
    url = _url()
    if not url:
        return None
    return MySQLConnector(url, timeout=60.0)


async def test_mysql_auth_ping_and_unicode():
    connector = _connector()
    if connector is None:
        pytest.skip("Set MULTIDS_MYSQL_URL to run real MySQL integration tests")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        assert await connector.ping() is True
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(255))")
        await connector.execute(f"INSERT INTO {table} (id, name) VALUES (1, :name)", name="héllo 世界")
        rows = [row async for row in connector.fetch_rows(f"SELECT id, name FROM {table}")]
        assert rows == [{"id": 1, "name": "héllo 世界"}]
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


async def test_mysql_pagination_and_large_stream():
    connector = _connector()
    if connector is None:
        pytest.skip("MULTIDS_MYSQL_URL not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    total = 500
    try:
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, grp INT)")
        await connector.write_records(
            f"{table}",
            ({"id": i, "grp": i % 10} for i in range(total)),
            batch_size=100,
        )

        # Pagination via LIMIT/OFFSET.
        page1 = [r async for r in connector.fetch_rows(f"SELECT id FROM {table} ORDER BY id LIMIT 10 OFFSET 0")]
        page2 = [r async for r in connector.fetch_rows(f"SELECT id FROM {table} ORDER BY id LIMIT 10 OFFSET 10")]
        assert [r["id"] for r in page1] == list(range(10))
        assert [r["id"] for r in page2] == list(range(10, 20))

        # Large streamed read returns every row exactly once.
        streamed = [r async for r in connector.fetch_rows(f"SELECT id FROM {table} ORDER BY id")]
        assert len(streamed) == total
        assert [r["id"] for r in streamed] == list(range(total))
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


async def test_mysql_failure_recovery_raises_typed_error():
    connector = _connector()
    if connector is None:
        pytest.skip("MULTIDS_MYSQL_URL not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY)")
        with pytest.raises(ConnectorError):
            await connector.execute(f"INSERT INTO {table} (missing_col) VALUES (1)")
        # Connection is still usable after a rejected statement.
        assert await connector.ping() is True
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()
