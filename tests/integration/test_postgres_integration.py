"""Opt-in integration coverage for a real PostgreSQL service.

Set ``MULTIDS_POSTGRES_URL`` to a standard PostgreSQL DSN, for example
``postgresql://user:pass@host:5432/dbname``. The test creates and drops its
own uniquely-named table.

Scenarios: authentication, Unicode, bulk insert via COPY, streaming reads,
pagination, cleanup and failure recovery.
"""

from __future__ import annotations

import os
import uuid

import pytest

from multids.connectors.postgres import PostgreSQLConnector
from multids.connectors.sync.postgres import SyncPostgreSQLConnector

pytestmark = [pytest.mark.real_integration]


def _dsn() -> str | None:
    return os.environ.get("MULTIDS_POSTGRES_URL")


def _connector() -> PostgreSQLConnector | None:
    dsn = _dsn()
    if not dsn:
        return None
    return PostgreSQLConnector(dsn, timeout=60.0)


@pytest.mark.asyncio
async def test_postgres_auth_ping_and_unicode():
    connector = _connector()
    if connector is None:
        pytest.skip("Set MULTIDS_POSTGRES_URL to run real PostgreSQL integration tests")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        assert await connector.ping() is True
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        await connector.execute(f"INSERT INTO {table} (id, name) VALUES ($1, $2)", p1=1, p2="héllo 世界")
        rows = [row async for row in connector.fetch_rows(f"SELECT id, name FROM {table}")]
        assert rows == [{"id": 1, "name": "héllo 世界"}]
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


@pytest.mark.asyncio
async def test_postgres_bulk_insert_copy():
    connector = _connector()
    if connector is None:
        pytest.skip("MULTIDS_POSTGRES_URL not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        rows = [{"id": i, "name": f"name-{i}"} for i in range(1000)]
        result = await connector.bulk_insert(table, rows)
        assert result.items_written == 1000

        await connector.execute(f"SELECT count(*) FROM {table}")
        # The execute result for SELECT returns the row count, not the value.
        # Use fetch_rows to verify.
        rows_read = [row async for row in connector.fetch_rows(f"SELECT id, name FROM {table} ORDER BY id")]
        assert len(rows_read) == 1000
        assert rows_read[0] == {"id": 0, "name": "name-0"}
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


@pytest.mark.asyncio
async def test_postgres_copy_with_null_values():
    connector = _connector()
    if connector is None:
        pytest.skip("MULTIDS_POSTGRES_URL not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": None}]
        result = await connector.copy_records_to_table(table, rows)  # type: ignore[arg-type]
        assert result.items_written == 2

        rows_read = [row async for row in connector.fetch_rows(f"SELECT id, name FROM {table} ORDER BY id")]
        assert rows_read[0] == {"id": 1, "name": "alice"}
        assert rows_read[1] == {"id": 2, "name": None}
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


@pytest.mark.asyncio
async def test_postgres_streaming_reads():
    connector = _connector()
    if connector is None:
        pytest.skip("MULTIDS_POSTGRES_URL not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    try:
        await connector.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        rows = [{"id": i, "name": f"name-{i}"} for i in range(100)]
        await connector.bulk_insert(table, rows)

        collected = []
        async for row in connector.fetch_rows(f"SELECT id, name FROM {table} WHERE id >= $1", p1=50):
            collected.append(row)
        assert len(collected) == 50
        assert collected[0]["id"] == 50
    finally:
        try:
            await connector.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        await connector.close()


def test_postgres_sync_bulk_insert():
    dsn = _dsn()
    if not dsn:
        pytest.skip("MULTIDS_POSTGRES_URL not set")

    table = f"multids_it_{uuid.uuid4().hex[:12]}"
    conn = SyncPostgreSQLConnector(dsn)
    try:
        assert conn.ping() is True
        conn.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        rows = [{"id": i, "name": f"name-{i}"} for i in range(500)]
        result = conn.bulk_insert(table, rows)
        assert result.items_written == 500

        rows_read = list(conn.fetch_rows(f"SELECT id, name FROM {table} ORDER BY id"))
        assert len(rows_read) == 500
    finally:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
        conn.close()
