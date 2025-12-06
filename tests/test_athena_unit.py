import pytest


class DummyClient:
    def __init__(self):
        self._started = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def start_query_execution(self, **kwargs):
        return {"QueryExecutionId": "qid-123"}

    async def get_query_execution(self, QueryExecutionId=None):  # noqa: N803
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    async def get_query_results(self, QueryExecutionId=None, NextToken=None):  # noqa: N803
        # header + two rows
        return {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "col1"}, {"Name": "col2"}]},
                "Rows": [
                    {"Data": [{"VarCharValue": "col1"}, {"VarCharValue": "col2"}]},
                    {"Data": [{"VarCharValue": "r1c1"}, {"VarCharValue": "r1c2"}]},
                    {"Data": [{"VarCharValue": "r2c1"}, {"VarCharValue": "r2c2"}]},
                ],
            }
        }


class DummyAioboto3:
    def Session(self):  # noqa: N802
        class DummySession:
            def client(self, service_name, region_name=None):
                assert service_name == "athena"
                return DummyClient()

        return DummySession()


@pytest.mark.asyncio
async def test_athena_query_stream(monkeypatch):
    # patch aioboto3 client factory
    monkeypatch.setitem(__import__("sys").modules, "aioboto3", DummyAioboto3())
    # Alternative patch: place a module named aioboto3 in sys.modules
    import sys

    sys.modules["aioboto3"] = DummyAioboto3()

    from multids.connectors.athena import AthenaConnector

    a = AthenaConnector()
    rows = []
    async for r in a.query_stream("SELECT 1"):
        rows.append(r)

    assert len(rows) == 2
    assert rows[0]["col1"] == "r1c1"
