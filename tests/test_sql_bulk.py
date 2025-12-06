import pytest

from multids.connectors.sql import MySQLConnector


class FakeConn:
    def __init__(self):
        self.executed = []

    async def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))


class FakeEngine:
    def __init__(self):
        self.conn = FakeConn()

    def begin(self):
        connection = self.conn

        class Ctx:
            async def __aenter__(self):
                return connection

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return Ctx()


@pytest.mark.asyncio
async def test_bulk_insert():
    m = MySQLConnector("mysql+asyncmy://x")
    # inject fake engine
    m._engine = FakeEngine()

    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    await m.bulk_insert("test_table", rows)

    # confirm that ONE execute call occurred (batched)
    assert len(m._engine.conn.executed) == 1
