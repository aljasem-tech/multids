from unittest.mock import AsyncMock, MagicMock

import pytest

from multids.connectors.athena import AthenaConnector
from multids.connectors.local import LocalConnector
from multids.connectors.mssql import MSSQLConnector
from multids.connectors.s3 import S3Connector
from multids.connectors.sql import MySQLConnector
from multids.interfaces import Connector


@pytest.mark.asyncio
async def test_local_ping():
    c = LocalConnector()
    assert await c.ping() is True


@pytest.mark.asyncio
async def test_s3_ping():
    # Mock aioboto3 session
    mock_session = MagicMock()
    mock_client_cm = AsyncMock()
    mock_client = AsyncMock()
    mock_client_cm.__aenter__.return_value = mock_client
    mock_session.client.return_value = mock_client_cm

    # Patch S3Connector to use our mock session
    c = S3Connector()
    c._session = mock_session

    # Success case
    assert await c.ping() is True
    assert mock_client.list_buckets.called

    # Failure case
    mock_client.list_buckets.side_effect = Exception("boom")
    assert await c.ping() is False


@pytest.mark.asyncio
async def test_mysql_ping():
    mock_engine = MagicMock()
    mock_conn = AsyncMock()
    # Mock engine.connect() -> async context manager -> conn
    mock_cm = AsyncMock()
    mock_cm.__aenter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_cm

    c = MySQLConnector("mysql+asyncmy://u:p@h/d")
    c._engine = mock_engine

    assert await c.ping() is True
    assert mock_conn.execute.called

    mock_conn.execute.side_effect = Exception("db down")
    assert await c.ping() is False


@pytest.mark.asyncio
async def test_mssql_ping():
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_cur = AsyncMock()

    # pool.acquire() -> returns AsyncContextManager yielding mock_conn
    # pool.acquire is a METHOD. So we mock the method.
    mock_pool_cm = AsyncMock()
    mock_pool_cm.__aenter__.return_value = mock_conn
    mock_pool.acquire.return_value = mock_pool_cm

    # conn.cursor() -> returns AsyncContextManager yielding mock_cur
    mock_conn_cm = AsyncMock()
    mock_conn_cm.__aenter__.return_value = mock_cur
    mock_conn.cursor.return_value = mock_conn_cm

    c = MSSQLConnector()
    c._pool = mock_pool

    assert await c.ping() is True
    assert mock_cur.execute.called


@pytest.mark.asyncio
async def test_athena_refactor_and_ping():
    from unittest.mock import patch

    # Verify inheritance
    assert issubclass(AthenaConnector, Connector)

    # Mock internals for ping
    mock_aioboto3 = MagicMock()
    mock_session = MagicMock()
    mock_client = AsyncMock()

    # Context manager setup for client
    mock_session.client.return_value = MagicMock()

    mock_exit_stack_cls = MagicMock()
    mock_exit_stack_instance = AsyncMock()
    mock_exit_stack_cls.return_value = mock_exit_stack_instance
    mock_exit_stack_instance.enter_async_context.return_value = mock_client

    mock_aioboto3.Session.return_value = mock_session

    # We patch AsyncExitStack where it is imported in athena.py
    # Since we changed it to 'from contextlib import AsyncExitStack' at top level,
    # we patch 'multids.connectors.athena.AsyncExitStack'
    with patch("multids.connectors.athena.AsyncExitStack", mock_exit_stack_cls), patch.dict(
        "sys.modules", {"aioboto3": mock_aioboto3}
    ):
        c = AthenaConnector()
        # Inject mocks
        c._aioboto3 = mock_aioboto3

        # First ping triggers _get_client which initializes everything
        assert await c.ping() is True
        assert mock_client.list_work_groups.called

        # Check that client is cached
        assert c._client is mock_client

        # Failure case
        mock_client.list_work_groups.side_effect = Exception("athena error")
        assert await c.ping() is False

        # Close
        await c.close()
        assert c._client is None
        assert mock_exit_stack_instance.aclose.called
