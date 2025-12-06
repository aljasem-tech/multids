import pytest

from multids.connectors.opensearch import OpenSearchConnector


@pytest.mark.asyncio
async def test_build_bulk_ndjson_sync():
    docs = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    gen = OpenSearchConnector.build_bulk_ndjson(docs, index="idx", id_field="id", chunk_size=2)

    out = []
    async for chunk in gen:
        out.append(chunk)

    assert out, "No NDJSON chunks produced"
    # ensure id metadata present
    assert '"_id": 1' in out[0]
    assert '"name": "a"' in out[0]


@pytest.mark.asyncio
async def test_build_bulk_ndjson_async():
    async def agen():
        for d in [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]:
            yield d

    gen = OpenSearchConnector.build_bulk_ndjson(agen(), index="idx", id_field="id", routing_field=None, chunk_size=1)
    chunks = []
    async for c in gen:
        chunks.append(c)

    assert len(chunks) == 2
    assert '"_id": 1' in chunks[0]


class DummyClient:
    def __init__(self):
        self.calls = []

    async def post(self, path, **kwargs):
        self.calls.append(("POST", path, kwargs))

        class R:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"ok": True}

        return R()

    async def put(self, path, **kwargs):
        self.calls.append(("PUT", path, kwargs))

        class R:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"ok": True}

        return R()


@pytest.mark.asyncio
async def test_auth_headers_and_kwargs():
    # API key header
    oc = OpenSearchConnector("http://localhost:9200", api_key="secret")
    dummy = DummyClient()
    oc._client = dummy

    await oc.index_doc("idx", {"name": "x"})
    # client call should include headers with Authorization
    assert any("Authorization" in (call[2].get("headers") or {}) for call in dummy.calls)

    # basic auth should be passed as auth kwarg
    oc2 = OpenSearchConnector("http://localhost:9200", basic_auth=("u", "p"))
    dummy2 = DummyClient()
    oc2._client = dummy2
    await oc2.index_doc("idx", {"name": "y"}, id="1")
    assert any("auth" in call[2] for call in dummy2.calls)
