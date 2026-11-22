"""Phase 3: reliability and observability parity.

Covers the exit criteria:
* every remote connector exposes the same error/retry/logging behaviour;
* structured start/completion/retry/failure events carry duration, backend,
  operation and safe identifiers;
* credentials, SQL parameters and record payloads never appear in telemetry.

All tests use mocks; no live services are contacted.
"""

from __future__ import annotations

import sys

import pytest

from multids.connectors.athena import AthenaConnector
from multids.connectors.mssql import MSSQLConnector
from multids.connectors.opensearch import OpenSearchConnector
from multids.connectors.s3 import S3Connector
from multids.connectors.sql import SQLConnectorBase
from multids.connectors.sync.sql import SyncSQLConnectorBase
from multids.error_mapping import map_botocore_error, map_pyodbc_error, map_sqlalchemy_error
from multids.errors import (
    ConnectorConnectionError,
    ConnectorDataError,
    ConnectorDependencyError,
    ConnectorOperationError,
    ConnectorSchemaError,
)
from multids.reliability import RetryConfig, run_operation, run_operation_sync
from multids.telemetry import Telemetry, TelemetryEvent

FAST = RetryConfig(max_retries=3, backoff_factor=0.0, max_backoff=0.0, timeout=None)
NO_RETRY = RetryConfig(max_retries=0, backoff_factor=0.0, max_backoff=0.0, timeout=None)


def _collector() -> tuple[list[TelemetryEvent], Telemetry]:
    events: list[TelemetryEvent] = []
    return events, Telemetry(tracer=events.append)


# ---------------------------------------------------------------------------
# Telemetry lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_operation_emits_start_then_completion():
    events, tel = _collector()

    async def op():
        return 7

    result = await run_operation(
        op, config=FAST, backend="test", operation_name="test.op", telemetry=tel, resource="res"
    )

    assert result == 7
    assert [e.event for e in events] == ["start", "completion"]
    assert events[0].backend == "test"
    assert events[0].operation == "test.op"
    assert events[0].resource == "res"
    assert events[1].duration_seconds >= 0.0


@pytest.mark.asyncio
async def test_run_operation_emits_retry_events_then_completion():
    events, tel = _collector()
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectorConnectionError("transient")
        return "ok"

    result = await run_operation(op, config=FAST, backend="test", operation_name="test.retry", telemetry=tel)

    assert result == "ok"
    kinds = [e.event for e in events]
    assert kinds[0] == "start"
    assert kinds.count("retry") == 2
    assert kinds[-1] == "completion"
    retry_events = [e for e in events if e.event == "retry"]
    assert [e.retry_number for e in retry_events] == [1, 2]
    assert all(e.error_type == "ConnectorConnectionError" for e in retry_events)


@pytest.mark.asyncio
async def test_run_operation_emits_failure_event():
    events, tel = _collector()

    async def op():
        raise ConnectorConnectionError("boom")

    with pytest.raises(ConnectorConnectionError):
        await run_operation(op, config=NO_RETRY, backend="test", operation_name="test.fail", telemetry=tel)

    assert [e.event for e in events] == ["start", "failure"]
    assert events[-1].error_type == "ConnectorConnectionError"


@pytest.mark.asyncio
async def test_run_operation_non_idempotent_does_not_retry():
    events, tel = _collector()
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        raise ConnectorConnectionError("down")

    with pytest.raises(ConnectorConnectionError):
        await run_operation(
            op,
            config=FAST,
            backend="test",
            operation_name="test.bulk",
            telemetry=tel,
            retry=False,
        )

    assert calls["n"] == 1
    assert [e.event for e in events] == ["start", "failure"]


def test_metrics_callback_receives_events():
    seen: list[TelemetryEvent] = []
    tel = Telemetry(metrics=seen.append)

    tel.start("s3", "s3.get_object", "bucket/key")
    tel.completion("s3", "s3.get_object", duration_seconds=0.1, resource="bucket/key")

    assert [e.event for e in seen] == ["start", "completion"]
    assert seen[1].duration_seconds == 0.1


def test_observer_exception_is_isolated():
    def bad(_event: TelemetryEvent) -> None:
        raise RuntimeError("observer exploded")

    tel = Telemetry(tracer=bad, metrics=bad)
    # Must not raise: telemetry can never break data movement.
    tel.start("s3", "s3.get_object")
    tel.failure("s3", "s3.get_object", duration_seconds=0.0, error_type="ConnectorConnectionError")


def test_event_log_extra_has_closed_safe_field_set():
    event = TelemetryEvent(
        event="failure",
        backend="sql",
        operation="sql.execute",
        duration_seconds=1.5,
        resource="users",
        error_type="ConnectorConnectionError",
    )
    extra = event.as_log_extra()

    assert set(extra) == {
        "telemetry_event",
        "connector_backend",
        "connector_operation",
        "connector_duration_seconds",
        "connector_resource",
        "connector_error_type",
    }
    # No free-form payload field exists.
    assert "connector_payload" not in extra
    assert "connector_params" not in extra


# ---------------------------------------------------------------------------
# Redaction: secrets / params / payloads never reach telemetry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exception_message_never_appears_in_telemetry():
    events, tel = _collector()

    async def op():
        raise ConnectorConnectionError("password=hunter2 SELECT * FROM t WHERE ssn=111-22-3333")

    with pytest.raises(ConnectorConnectionError):
        await run_operation(
            op, config=NO_RETRY, backend="sql", operation_name="sql.execute", telemetry=tel, resource="users"
        )

    blob = "\n".join(repr(e) for e in events)
    assert "hunter2" not in blob
    assert "ssn" not in blob
    assert "111-22-3333" not in blob
    assert events[-1].error_type == "ConnectorConnectionError"


class _FakeResult:
    rowcount = 1


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple] = []

    async def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))
        return _FakeResult()


class _FakeBeginCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self, conn: _FakeConn | None = None) -> None:
        self.conn = conn or _FakeConn()

    def begin(self):
        return _FakeBeginCtx(self.conn)


@pytest.mark.asyncio
async def test_sql_parameters_do_not_appear_in_telemetry():
    events, tel = _collector()
    engine = _FakeEngine()
    connector = SQLConnectorBase("sqlite+aiosqlite:///:memory:")
    connector._engine = engine
    connector.telemetry = tel

    secret = "hunter2"
    result = await connector.execute("UPDATE users SET s = :s WHERE id = :id", s=secret, id=1)

    assert result.items_written == 1
    blob = "\n".join(repr(e) for e in events)
    assert secret not in blob
    assert engine.conn.executed[0][1]["s"] == secret  # params still reach the driver
    assert any(e.event == "completion" for e in events)


@pytest.mark.asyncio
async def test_sql_bulk_insert_rows_do_not_appear_in_telemetry():
    events, tel = _collector()
    connector = SQLConnectorBase("sqlite+aiosqlite:///:memory:")
    connector._engine = _FakeEngine()
    connector.telemetry = tel

    secret = "hunter2"
    await connector.bulk_insert("accounts", [{"password": secret}])

    blob = "\n".join(repr(e) for e in events)
    assert secret not in blob
    # table name is a safe identifier and is allowed
    assert any(e.resource == "accounts" for e in events)


@pytest.mark.asyncio
async def test_s3_payload_does_not_appear_in_telemetry():
    events, tel = _collector()

    class _Client:
        async def put_object(self, Bucket, Key, Body):  # noqa: N803
            return {}

    class _ClientCtx:
        async def __aenter__(self):
            return _Client()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Session:
        def client(self, *args, **kwargs):
            return _ClientCtx()

    connector = S3Connector(max_retries=0, backoff_factor=0.0)
    connector._session = _Session()
    connector.telemetry = tel

    secret = b"TOP-SECRET-PAYLOAD"

    async def gen():
        yield secret

    await connector.write_stream(gen(), bucket="my-bucket", key="my-key")

    blob = "\n".join(repr(e) for e in events)
    assert "TOP-SECRET-PAYLOAD" not in blob
    assert any(e.resource == "my-bucket/my-key" for e in events)


@pytest.mark.asyncio
async def test_mssql_password_does_not_appear_in_telemetry():
    events, tel = _collector()

    class _FakeAioodbc:
        async def create_pool(self, **kwargs):
            raise ConnectorConnectionError("db down")

    connector = MSSQLConnector(
        host="db.internal",
        user="alice",
        password="hunter2",
        max_retries=0,
        backoff_factor=0.0,
        telemetry=tel,
    )
    connector._aioodbc = _FakeAioodbc()

    with pytest.raises(ConnectorConnectionError):
        await connector.connect_pool()

    blob = "\n".join(repr(e) for e in events)
    assert "hunter2" not in blob
    assert "alice" not in blob
    assert any(e.event == "failure" for e in events)


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


def test_map_sqlalchemy_connection_error():
    class OperationalError(Exception):
        pass

    err = map_sqlalchemy_error(OperationalError("gone"), operation="sql.execute")
    assert isinstance(err, ConnectorConnectionError)


def test_map_sqlalchemy_schema_error():
    class ProgrammingError(Exception):
        pass

    err = map_sqlalchemy_error(ProgrammingError("bad syntax"), operation="sql.execute")
    assert isinstance(err, ConnectorSchemaError)


def test_map_sqlalchemy_data_errors():
    class DataError(Exception):
        pass

    class IntegrityError(Exception):
        pass

    assert isinstance(map_sqlalchemy_error(DataError("bad value"), operation="sql.execute"), ConnectorDataError)
    assert isinstance(map_sqlalchemy_error(IntegrityError("dup"), operation="sql.execute"), ConnectorDataError)


def test_map_sqlalchemy_dependency_error():
    err = map_sqlalchemy_error(ModuleNotFoundError("no driver"), operation="sql.execute")
    assert isinstance(err, ConnectorDependencyError)


def test_map_botocore_server_error_is_connection():
    class ClientError(Exception):
        def __init__(self, status):
            self.status_code = status

    assert isinstance(
        map_botocore_error(ClientError(503), backend="s3", operation="s3.get_object"),
        ConnectorConnectionError,
    )


def test_map_botocore_client_error_is_operation():
    class ClientError(Exception):
        def __init__(self, status):
            self.status_code = status

    assert isinstance(
        map_botocore_error(ClientError(404), backend="s3", operation="s3.get_object"),
        ConnectorOperationError,
    )


def test_map_botocore_connection_error():
    class EndpointConnectionError(Exception):
        pass

    assert isinstance(
        map_botocore_error(EndpointConnectionError("dns"), backend="s3", operation="s3.get_object"),
        ConnectorConnectionError,
    )


def test_map_pyodbc_errors():
    class OperationalError(Exception):
        pass

    class IntegrityError(Exception):
        pass

    assert isinstance(map_pyodbc_error(OperationalError("down"), operation="mssql.execute"), ConnectorConnectionError)
    assert isinstance(map_pyodbc_error(IntegrityError("dup"), operation="mssql.execute"), ConnectorDataError)


def test_connector_errors_pass_through_unchanged():
    original = ConnectorConnectionError("already normalized")
    assert map_sqlalchemy_error(original, operation="sql.execute") is original


# ---------------------------------------------------------------------------
# Retry/logging parity across connectors
# ---------------------------------------------------------------------------


class _FlakyAsyncEngine:
    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0
        self.conn = _FakeConn()

    def begin(self):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectorConnectionError("db down")
        return _FakeBeginCtx(self.conn)


@pytest.mark.asyncio
async def test_sql_connector_retries_and_emits_telemetry():
    events, tel = _collector()
    engine = _FlakyAsyncEngine(fail_times=2)
    connector = SQLConnectorBase("sqlite+aiosqlite:///:memory:")
    connector._engine = engine
    connector.retry_config = FAST
    connector.telemetry = tel

    result = await connector.execute("UPDATE t SET x = 1")

    assert result.items_written == 1
    assert engine.calls == 3
    kinds = [e.event for e in events]
    assert kinds.count("retry") == 2
    assert kinds[-1] == "completion"
    assert all(e.backend == "sql" for e in events)
    assert all(e.operation == "sql.execute" for e in events)


@pytest.mark.asyncio
async def test_mssql_connector_retries_and_emits_telemetry():  # noqa: C901
    events, tel = _collector()

    class _Cursor:
        def __init__(self) -> None:
            self.rowcount = 1
            self.description = [("id",)]
            self.calls = 0

        async def execute(self, sql, params=None):
            self.calls += 1
            if self.calls == 1:
                raise ConnectorConnectionError("db down")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _CursorCtx:
        def __init__(self, cur):
            self.cur = cur

        async def __aenter__(self):
            return self.cur

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Conn:
        def __init__(self, cur):
            self.cur = cur

        def cursor(self):
            return _CursorCtx(self.cur)

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Acquire(self.conn)

    cur = _Cursor()
    connector = MSSQLConnector(max_retries=2, backoff_factor=0.0, telemetry=tel)
    connector._pool = _Pool(_Conn(cur))

    result = await connector.execute("UPDATE t SET x = 1")

    assert result.items_written == 1
    assert cur.calls == 2
    kinds = [e.event for e in events]
    assert kinds.count("retry") == 1
    assert kinds[-1] == "completion"
    assert all(e.backend == "mssql" for e in events)


@pytest.mark.asyncio
async def test_s3_connector_retries_and_emits_telemetry():
    events, tel = _collector()

    class _Body:
        async def read(self, size=-1):
            return b""

        async def close(self):
            return None

    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        async def get_object(self, Bucket, Key):  # noqa: N803
            self.calls += 1
            if self.calls <= 2:
                raise ConnectorConnectionError("s3 down")
            return {"Body": _Body()}

    client = _Client()

    class _ClientCtx:
        async def __aenter__(self):
            return client

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Session:
        def client(self, *args, **kwargs):
            return _ClientCtx()

    connector = S3Connector(max_retries=2, backoff_factor=0.0)
    connector._session = _Session()
    connector.telemetry = tel

    data = await connector.read_bytes("bucket", "key")

    assert data == b""
    assert client.calls == 3
    kinds = [e.event for e in events]
    assert kinds.count("retry") == 2
    assert kinds[-1] == "completion"
    assert all(e.backend == "s3" for e in events)
    assert any(e.resource == "bucket/key" for e in events)


@pytest.mark.asyncio
async def test_athena_connector_retries_and_emits_telemetry(monkeypatch):
    events, tel = _collector()

    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        async def start_query_execution(self, **kwargs):
            self.calls += 1
            if self.calls <= 2:
                raise ConnectorConnectionError("athena down")
            return {"QueryExecutionId": "qid-1"}

    client = _Client()

    class _Session:
        def client(self, service_name, region_name=None):
            return client

    class _Aioboto3:
        def Session(self):  # noqa: N802
            return _Session()

    monkeypatch.setitem(sys.modules, "aioboto3", _Aioboto3())

    connector = AthenaConnector(max_retries=2, backoff_factor=0.0, telemetry=tel)
    connector._client = client

    qid = await connector.start_query("SELECT 1")

    assert qid == "qid-1"
    assert client.calls == 3
    kinds = [e.event for e in events]
    assert kinds.count("retry") == 2
    assert kinds[-1] == "completion"
    assert all(e.backend == "athena" for e in events)


@pytest.mark.asyncio
async def test_opensearch_connector_retries_and_emits_telemetry():
    events, tel = _collector()

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {}

    class _Flaky(OpenSearchConnector):
        def __init__(self) -> None:
            super().__init__("http://localhost:9200", max_retries=2, backoff_factor=0.0, telemetry=tel)
            self.calls = 0

        async def _call_client(self, method, path, **kwargs):
            self.calls += 1
            if self.calls <= 2:
                raise ConnectorConnectionError("os down")
            return _Resp()

    connector = _Flaky()
    response = await connector._request("GET", "/")

    assert response.status_code == 200
    assert connector.calls == 3
    kinds = [e.event for e in events]
    assert kinds.count("retry") == 2
    assert kinds[-1] == "completion"
    assert all(e.backend == "opensearch" for e in events)


@pytest.mark.asyncio
async def test_dependency_error_is_not_retried_and_emits_failure():
    events, tel = _collector()

    class _Flaky(SQLConnectorBase):
        def __init__(self) -> None:
            super().__init__("sqlite+aiosqlite:///:memory:")
            self.retry_config = FAST
            self.telemetry = tel
            self.calls = 0

        def _get_engine(self):
            self.calls += 1
            raise ConnectorDependencyError("no driver")

    connector = _Flaky()

    with pytest.raises(ConnectorDependencyError):
        await connector.execute("UPDATE t SET x = 1")

    assert connector.calls == 1
    assert [e.event for e in events] == ["start", "failure"]


# ---------------------------------------------------------------------------
# Sync parity
# ---------------------------------------------------------------------------


class _SyncResult:
    rowcount = 1


class _SyncConn:
    def execute(self, stmt, params=None):
        return _SyncResult()


class _SyncBeginCtx:
    def __enter__(self):
        return _SyncConn()

    def __exit__(self, exc_type, exc, tb):
        return False


class _SyncFlakyEngine:
    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    def begin(self):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectorConnectionError("db down")
        return _SyncBeginCtx()


def test_sync_run_operation_retries_and_emits_telemetry():
    events, tel = _collector()
    calls = {"n": 0}

    def op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectorConnectionError("transient")
        return "ok"

    result = run_operation_sync(
        op,
        config=FAST,
        backend="test",
        operation_name="test.sync_retry",
        telemetry=tel,
        sleep=lambda _delay: None,
    )

    assert result == "ok"
    assert calls["n"] == 3
    kinds = [e.event for e in events]
    assert kinds.count("retry") == 2
    assert kinds[-1] == "completion"


def test_sync_sql_connector_retries_and_emits_telemetry():
    events, tel = _collector()
    engine = _SyncFlakyEngine(fail_times=2)
    connector = SyncSQLConnectorBase("sqlite:///:memory:")
    connector._engine = engine
    connector.retry_config = FAST
    connector.telemetry = tel

    result = connector.execute("UPDATE t SET x = 1")

    assert result.items_written == 1
    assert engine.calls == 3
    kinds = [e.event for e in events]
    assert kinds.count("retry") == 2
    assert kinds[-1] == "completion"
    assert all(e.backend == "sql" for e in events)


def test_sync_sql_bulk_insert_does_not_retry():
    events, tel = _collector()

    class _FailEngine:
        def __init__(self) -> None:
            self.calls = 0

        def begin(self):
            self.calls += 1
            raise ConnectorConnectionError("db down")

    engine = _FailEngine()
    connector = SyncSQLConnectorBase("sqlite:///:memory:")
    connector._engine = engine
    connector.retry_config = FAST
    connector.telemetry = tel

    with pytest.raises(ConnectorConnectionError):
        connector.bulk_insert("t", [{"id": 1}])

    assert engine.calls == 1
    assert [e.event for e in events] == ["start", "failure"]


# ---------------------------------------------------------------------------
# Parity surface: every connector accepts and exposes telemetry
# ---------------------------------------------------------------------------


def test_async_connectors_expose_telemetry(monkeypatch):
    tel = Telemetry()

    sql = SQLConnectorBase("sqlite+aiosqlite:///:memory:", telemetry=tel)
    mssql = MSSQLConnector(telemetry=tel)
    s3 = S3Connector(telemetry=tel)
    opensearch = OpenSearchConnector("http://localhost:9200", telemetry=tel)

    class _Session:
        def client(self, service_name, region_name=None):
            return None

    class _Aioboto3:
        def Session(self):  # noqa: N802
            return _Session()

    monkeypatch.setitem(sys.modules, "aioboto3", _Aioboto3())
    athena = AthenaConnector(telemetry=tel)

    for connector in (sql, mssql, s3, opensearch, athena):
        assert connector.telemetry is tel


def test_sync_connectors_expose_telemetry():
    tel = Telemetry()
    sql = SyncSQLConnectorBase("sqlite:///:memory:", telemetry=tel)
    assert sql.telemetry is tel


def test_default_telemetry_is_available_without_configuration():
    connector = SQLConnectorBase("sqlite+aiosqlite:///:memory:")
    assert isinstance(connector.telemetry, Telemetry)
