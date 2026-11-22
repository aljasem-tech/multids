"""Contract-conformance tests for every built-in connector (Phase 1).

These tests verify that each connector exposes the capability protocols it is
expected to satisfy. They use ``issubclass`` against the runtime-checkable
protocols, so they do not require live services or optional drivers.
"""

import pytest

from multids.connectors.athena import AthenaConnector
from multids.connectors.local import LocalConnector
from multids.connectors.mssql import MSSQLConnector
from multids.connectors.opensearch import OpenSearchConnector
from multids.connectors.s3 import S3Connector
from multids.connectors.sql import MySQLConnector, SQLConnectorBase
from multids.connectors.sync.athena import SyncAthenaConnector
from multids.connectors.sync.local import SyncLocalConnector
from multids.connectors.sync.mssql import SyncMSSQLConnector
from multids.connectors.sync.s3 import SyncS3Connector
from multids.connectors.sync.sql import SyncMySQLConnector, SyncSQLConnectorBase
from multids.contracts import (
    AsyncObjectStorage,
    AsyncRecordReader,
    AsyncRecordWriter,
    ObjectRef,
    SyncObjectStorage,
    SyncRecordReader,
    SyncRecordWriter,
)

ASYNC_OBJECT_STORAGE = [LocalConnector, S3Connector]
SYNC_OBJECT_STORAGE = [SyncLocalConnector, SyncS3Connector]

ASYNC_RECORD_READERS = [AthenaConnector, MSSQLConnector, OpenSearchConnector, SQLConnectorBase, MySQLConnector]
ASYNC_RECORD_WRITERS = [MSSQLConnector, OpenSearchConnector, SQLConnectorBase, MySQLConnector]

SYNC_RECORD_READERS = [
    SyncAthenaConnector,
    SyncMSSQLConnector,
    SyncMySQLConnector,
    SyncSQLConnectorBase,
]
SYNC_RECORD_WRITERS = [SyncMSSQLConnector, SyncMySQLConnector, SyncSQLConnectorBase]


@pytest.mark.parametrize("connector_cls", ASYNC_OBJECT_STORAGE)
def test_async_object_storage_conformance(connector_cls):
    assert issubclass(connector_cls, AsyncObjectStorage)


@pytest.mark.parametrize("connector_cls", SYNC_OBJECT_STORAGE)
def test_sync_object_storage_conformance(connector_cls):
    assert issubclass(connector_cls, SyncObjectStorage)


@pytest.mark.parametrize("connector_cls", ASYNC_RECORD_READERS)
def test_async_record_reader_conformance(connector_cls):
    assert issubclass(connector_cls, AsyncRecordReader)


@pytest.mark.parametrize("connector_cls", ASYNC_RECORD_WRITERS)
def test_async_record_writer_conformance(connector_cls):
    assert issubclass(connector_cls, AsyncRecordWriter)


@pytest.mark.parametrize("connector_cls", SYNC_RECORD_READERS)
def test_sync_record_reader_conformance(connector_cls):
    assert issubclass(connector_cls, SyncRecordReader)


@pytest.mark.parametrize("connector_cls", SYNC_RECORD_WRITERS)
def test_sync_record_writer_conformance(connector_cls):
    assert issubclass(connector_cls, SyncRecordWriter)


@pytest.mark.asyncio
async def test_local_object_storage_roundtrip(tmp_path):
    """Exercise the full AsyncObjectStorage contract end-to-end on local files."""
    connector = LocalConnector(str(tmp_path))

    write = await connector.write_object(ObjectRef("a.txt"), b"hi")
    assert write.bytes_written == 2

    page = await connector.list_object_page(page_size=10)
    assert [item.key for item in page.items] == ["a.txt"]

    assert await connector.read_object(ObjectRef("a.txt")) == b"hi"

    deleted = await connector.delete_object_ref(ObjectRef("a.txt"))
    assert deleted.items_written == 1


def test_sync_local_object_storage_roundtrip(tmp_path):
    """Exercise the full SyncObjectStorage contract end-to-end on local files."""
    connector = SyncLocalConnector(str(tmp_path))

    write = connector.write_object(ObjectRef("a.txt"), b"hi")
    assert write.bytes_written == 2

    page = connector.list_object_page(page_size=10)
    assert [item.key for item in page.items] == ["a.txt"]

    assert connector.read_object(ObjectRef("a.txt")) == b"hi"

    deleted = connector.delete_object_ref(ObjectRef("a.txt"))
    assert deleted.items_written == 1


@pytest.mark.asyncio
async def test_async_close_is_idempotent():
    connector = LocalConnector()
    await connector.close()
    await connector.close()


def test_sync_close_is_idempotent():
    connector = SyncLocalConnector()
    connector.close()
    connector.close()


@pytest.mark.asyncio
async def test_opensearch_close_is_idempotent():
    from unittest.mock import AsyncMock

    connector = OpenSearchConnector("http://localhost:9200")
    connector._client = AsyncMock()
    await connector.close()
    await connector.close()
    assert connector._client is None
