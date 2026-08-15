import pytest

from multids.contracts import AsyncObjectStorage, ObjectRef, SyncObjectStorage
from multids.interfaces import AsyncConnectorContext, SyncConnectorContext


class AsyncLifecycleProbe(AsyncConnectorContext):
    def __init__(self):
        self.closed = 0

    async def close(self):
        self.closed += 1


class SyncLifecycleProbe(SyncConnectorContext):
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


@pytest.mark.asyncio
async def test_async_context_manager_closes_connector():
    probe = AsyncLifecycleProbe()
    async with probe as current:
        assert current is probe
    assert probe.closed == 1


def test_sync_context_manager_closes_connector():
    probe = SyncLifecycleProbe()
    with probe as current:
        assert current is probe
    assert probe.closed == 1


@pytest.mark.asyncio
async def test_local_object_contract_has_typed_pages(tmp_path):
    from multids.connectors.local import LocalConnector

    connector = LocalConnector(str(tmp_path))
    assert isinstance(connector, AsyncObjectStorage)

    write = await connector.write_object(ObjectRef("data.txt"), b"content")
    page = await connector.list_object_page(ObjectRef(""), page_size=1)

    assert write.bytes_written == 7
    assert page.items[0].key == "data.txt"
    assert page.next_cursor is None
    assert await connector.read_object(ObjectRef("data.txt")) == b"content"


def test_sync_object_protocol_is_runtime_checkable():
    class ObjectStore:
        def read_object(self, ref):
            return b""

        def write_object(self, ref, data):
            return None

        def list_object_page(self, prefix=ObjectRef(""), **kwargs):
            return None

        def delete_object_ref(self, ref):
            return None

    assert isinstance(ObjectStore(), SyncObjectStorage)
