import pytest

from multids.connectors.mssql import MSSQLConnector


class DummyCursor:
    def __init__(self, rows=None, description=None):
        self._rows = rows or []
        self._iter = iter(self._rows)
        self.description = description or [("col1",), ("col2",)]
        self.executemany_calls = []
        self.executed = []

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))

    async def executemany(self, sql, seq_of_params):
        self.executemany_calls.append((sql, seq_of_params))

    def __aiter__(self):
        async def _aiter():
            for r in self._rows:
                yield r

        return _aiter()


class DummyCursorCtx:
    def __init__(self, cursor: DummyCursor):
        self.cursor = cursor

    async def __aenter__(self):
        return self.cursor

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def __init__(self, cursor: DummyCursor):
        self._cursor = cursor

    def cursor(self):
        return DummyCursorCtx(self._cursor)


class DummyAcquire:
    def __init__(self, conn: DummyConn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyPool:
    def __init__(self, cursor: DummyCursor):
        self._cursor = cursor

    def acquire(self):
        return DummyAcquire(DummyConn(self._cursor))


@pytest.mark.asyncio
async def test_fetch_rows_and_iter():
    rows = [(1, "a"), (2, "b")]
    cur = DummyCursor(rows=rows, description=[("id",), ("name",)])
    pool = DummyPool(cur)

    m = MSSQLConnector()
    m._pool = pool

    res = await m.fetch_rows("select 1")
    assert isinstance(res, list)
    assert res[0]["id"] == 1

    got = []
    async for r in m.fetch_iter("select 1"):
        got.append(r)
    assert len(got) == 2


@pytest.mark.asyncio
async def test_bulk_insert_batches():
    cur = DummyCursor()
    pool = DummyPool(cur)

    m = MSSQLConnector()
    m._pool = pool

    rows = [[1, "a"] for _ in range(25)]
    await m.bulk_insert("tbl", ["c1", "c2"], rows, batch_size=10)

    # executemany should be called in batches
    assert cur.executemany_calls, "executemany was not called"
    total = sum(len(batch) for (_, batch) in cur.executemany_calls)
    assert total == 25
