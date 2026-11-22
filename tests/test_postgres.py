"""Unit tests for the PostgreSQL connectors (mocked asyncpg / psycopg2)."""

from unittest.mock import patch

import pytest

from multids.connectors.postgres import PostgreSQLConnector, _affected_rows
from multids.connectors.sync.postgres import SyncPostgreSQLConnector
from multids.contracts import BulkWriteResult, WriteResult
from multids.errors import ConnectorDependencyError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeAsyncpgConn:
    """Minimal asyncpg connection stub."""

    def __init__(self):
        self.fetched: list = []
        self.executed: list = []
        self.copied: list = []

    async def fetch(self, query, *args):
        self.fetched.append((query, args))
        return [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

    async def fetchval(self, query, *args):
        return 1

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return "INSERT 0 2"

    async def copy_records_to_table(self, table, *, records, columns=None):
        self.copied.append((table, records, columns))

    def transaction(self):
        conn = self

        class Txn:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return Txn()


class FakePool:
    def __init__(self, conn):
        self.conn = conn
        self._acquired = False

    def acquire(self):
        pool = self

        class Ctx:
            async def __aenter__(self):
                pool._acquired = True
                return pool.conn

            async def __aexit__(self, exc_type, exc, tb):
                pool._acquired = False
                return False

        return Ctx()

    async def close(self):
        pass


# ---------------------------------------------------------------------------
# _affected_rows
# ---------------------------------------------------------------------------


class TestAffectedRows:
    def test_insert(self):
        assert _affected_rows("INSERT 0 5") == 5

    def test_update(self):
        assert _affected_rows("UPDATE 3") == 3

    def test_delete(self):
        assert _affected_rows("DELETE 2") == 2

    def test_select(self):
        assert _affected_rows("SELECT 4") == 4

    def test_ddl_no_count(self):
        assert _affected_rows("CREATE TABLE") == 1

    def test_empty(self):
        assert _affected_rows("") == 0


# ---------------------------------------------------------------------------
# PostgreSQLConnector (async)
# ---------------------------------------------------------------------------


class TestPostgreSQLConnector:
    @pytest.mark.asyncio
    async def test_fetch_rows(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        rows = []
        async for row in pg.fetch_rows("SELECT * FROM users WHERE id > $1", min_id=0):
            rows.append(row)

        assert len(rows) == 2
        assert rows[0] == {"id": 1, "name": "alice"}
        assert conn.fetched[0][1] == (0,)

    @pytest.mark.asyncio
    async def test_execute(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        result = await pg.execute("INSERT INTO users (name) VALUES ($1)", name="alice")
        assert isinstance(result, WriteResult)
        assert result.items_written == 2

    @pytest.mark.asyncio
    async def test_bulk_insert_uses_copy(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        result = await pg.bulk_insert("users", rows)

        assert isinstance(result, BulkWriteResult)
        assert result.items_written == 2
        assert len(conn.copied) == 1
        assert conn.copied[0][0] == "users"
        assert conn.copied[0][2] == ["id", "name"]

    @pytest.mark.asyncio
    async def test_copy_records_to_table_empty(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        result = await pg.copy_records_to_table("users", [])
        assert result.items_written == 0
        assert len(conn.copied) == 0

    @pytest.mark.asyncio
    async def test_copy_records_to_table_with_columns(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        rows = [{"id": 1, "name": "a", "extra": "x"}]
        result = await pg.copy_records_to_table("users", rows, columns=["id", "name"])

        assert result.items_written == 1
        table, tuples, cols = conn.copied[0]
        assert cols == ["id", "name"]
        assert tuples == [(1, "a")]

    @pytest.mark.asyncio
    async def test_write_records(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        result = await pg.write_records("users", records)

        assert result.items_written == 2

    @pytest.mark.asyncio
    async def test_write_records_batch_size_validation(self):
        pg = PostgreSQLConnector("postgresql://localhost/test")
        with pytest.raises(ValueError, match="batch_size"):
            await pg.write_records("users", [], batch_size=0)

    @pytest.mark.asyncio
    async def test_ping(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        assert await pg.ping() is True

    @pytest.mark.asyncio
    async def test_ping_failure(self):
        pg = PostgreSQLConnector("postgresql://localhost/test")

        with patch.object(pg, "_get_pool", side_effect=Exception("no connection")):
            assert await pg.ping() is False

    @pytest.mark.asyncio
    async def test_close(self):
        conn = FakeAsyncpgConn()
        pool = FakePool(conn)
        pg = PostgreSQLConnector("postgresql://localhost/test")
        pg._pool = pool

        await pg.close()
        assert pg._pool is None

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        pg = PostgreSQLConnector("postgresql://localhost/test")
        await pg.close()
        await pg.close()

    @pytest.mark.asyncio
    async def test_dependency_error_when_asyncpg_missing(self):
        pg = PostgreSQLConnector("postgresql://localhost/test")
        with patch("multids.connectors.postgres.asyncpg", None):
            with pytest.raises(ConnectorDependencyError, match="asyncpg"):
                await pg._get_pool()

    @pytest.mark.asyncio
    async def test_fetch_rows_maps_errors(self):
        pg = PostgreSQLConnector("postgresql://localhost/test")

        async def failing_pool():
            raise ConnectionRefusedError("connection refused")

        with patch.object(pg, "_get_pool", side_effect=ConnectionRefusedError("connection refused")):
            with pytest.raises(Exception, match="could not reach the backend"):
                async for _ in pg.fetch_rows("SELECT 1"):
                    pass


# ---------------------------------------------------------------------------
# SyncPostgreSQLConnector
# ---------------------------------------------------------------------------


class FakePsycopg2Cursor:
    def __init__(self):
        self.executed = []
        self.description = [("id",), ("name",)]
        self.rowcount = 2
        self._fetchall = [(1, "alice"), (2, "bob")]

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self._fetchall

    def copy_expert(self, sql, buffer):
        self.executed.append((sql, buffer.read()))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakePsycopg2Conn:
    def __init__(self):
        self.closed = False
        self.autocommit = False
        self._cursor = FakePsycopg2Cursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class TestSyncPostgreSQLConnector:
    def test_fetch_rows(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        rows = list(pg.fetch_rows("SELECT * FROM users", (0,)))
        assert len(rows) == 2
        assert rows[0] == {"id": 1, "name": "alice"}

    def test_execute(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        result = pg.execute("INSERT INTO users (name) VALUES (%s)", ("alice",))
        assert isinstance(result, WriteResult)
        assert result.items_written == 2
        assert conn.commits == 1

    def test_execute_rollback_on_error(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        with patch.object(conn._cursor, "execute", side_effect=RuntimeError("boom")):
            with pytest.raises(Exception):
                pg.execute("INSERT INTO users (name) VALUES (%s)", ("alice",))

        assert conn.rollbacks == 1

    def test_copy_records_to_table(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        result = pg.copy_records_to_table("users", rows)

        assert result.items_written == 2
        assert conn.commits == 1

    def test_copy_records_to_table_empty(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        result = pg.copy_records_to_table("users", [])
        assert result.items_written == 0

    def test_bulk_insert_uses_copy(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        rows = [{"id": 1, "name": "a"}]
        result = pg.bulk_insert("users", rows)
        assert result.items_written == 1

    def test_write_records(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        result = pg.write_records("users", [{"id": 1, "name": "a"}])
        assert result.items_written == 1

    def test_write_records_batch_size_validation(self):
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        with pytest.raises(ValueError, match="batch_size"):
            pg.write_records("users", [], batch_size=0)

    def test_ping(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn
        assert pg.ping() is True

    def test_ping_failure(self):
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        with patch.object(pg, "_get_conn", side_effect=Exception("no connection")):
            assert pg.ping() is False

    def test_close(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn
        pg.close()
        assert conn.closed is True

    def test_close_idempotent(self):
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg.close()
        pg.close()

    def test_dependency_error_when_psycopg2_missing(self):
        with patch("multids.connectors.sync.postgres.psycopg2", None):
            with pytest.raises(ConnectorDependencyError, match="psycopg2"):
                SyncPostgreSQLConnector("postgresql://localhost/test")

    def test_copy_records_handles_null(self):
        conn = FakePsycopg2Conn()
        pg = SyncPostgreSQLConnector("postgresql://localhost/test")
        pg._conn = conn

        rows = [{"id": 1, "name": None}]
        result = pg.copy_records_to_table("users", rows)
        assert result.items_written == 1
        # The COPY buffer should contain \N for NULL
        copy_call = conn._cursor.executed[0]
        assert "\\N" in copy_call[1]
