from pathlib import Path

import pytest

from multids.connectors.local import LocalConnector
from multids.connectors.s3 import S3Connector


@pytest.mark.asyncio
async def test_local_connector_uses_uniform_byte_api_and_manages_paths(tmp_path):
    connector = LocalConnector(str(tmp_path))

    await connector.write_bytes(b"hello", "nested/file.txt")
    assert await connector.read_bytes("nested/file.txt") == b"hello"
    chunks = [chunk async for chunk in connector.read_stream("nested/file.txt", chunk_size=2)]
    assert chunks == [b"he", b"ll", b"o"]
    assert [path async for path in connector.list_keys()] == [str(Path("nested") / "file.txt")]

    await connector.delete_object("nested/file.txt")
    assert [path async for path in connector.list_keys()] == []


@pytest.mark.asyncio
async def test_s3_list_objects_paginates_and_delete_uses_same_client():
    class Paginator:
        def paginate(self, **kwargs):
            self.kwargs = kwargs

            async def pages():
                yield {"Contents": [{"Key": "logs/a"}]}
                yield {"Contents": [{"Key": "logs/b"}]}

            return pages()

    class Client:
        def __init__(self):
            self.paginator = Paginator()
            self.deleted = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return self.paginator

        async def delete_object(self, **kwargs):
            self.deleted = kwargs

    class Session:
        def __init__(self, client):
            self._client = client

        def client(self, *args, **kwargs):
            return self._client

    client = Client()
    connector = S3Connector(region_name="eu-central-1")
    connector._session = Session(client)

    keys = [obj["Key"] async for obj in connector.list_objects("bucket", "logs/", page_size=20)]
    assert keys == ["logs/a", "logs/b"]
    assert client.paginator.kwargs == {"Bucket": "bucket", "Prefix": "logs/", "PaginationConfig": {"PageSize": 20}}
    await connector.delete_object("bucket", "logs/a")
    assert client.deleted == {"Bucket": "bucket", "Key": "logs/a"}
