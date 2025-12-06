import pytest

from multids.connectors.opensearch import OpenSearchConnector


class DummyResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class DummyClient:
    def __init__(self):
        self.posts = []
        self.calls = []
        # for scroll simulation
        self._scroll_round = 0

    async def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        if path.startswith("/_bulk"):
            return DummyResponse(200, {"took": 1})

        if path.startswith("/") and path.endswith("_search?scroll=1m"):
            # initial scroll search
            self._scroll_round = 1
            return DummyResponse(200, {"_scroll_id": "s1", "hits": {"hits": [{"_id": "1"}]}})

        if path == "/_search/scroll":
            if self._scroll_round == 1:
                self._scroll_round = 2
                return DummyResponse(200, {"_scroll_id": "s2", "hits": {"hits": [{"_id": "2"}]}})
            else:
                return DummyResponse(200, {"_scroll_id": None, "hits": {"hits": []}})

        return DummyResponse(200, {})

    async def get(self, path, **kwargs):
        return await self.request("GET", path, **kwargs)

    async def post(self, path, **kwargs):
        return await self.request("POST", path, **kwargs)

    async def delete(self, path, **kwargs):
        return await self.request("DELETE", path, **kwargs)

    async def aclose(self):
        return


@pytest.mark.asyncio
async def test_bulk_index(monkeypatch):
    oc = OpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    docs = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    res = await oc.bulk_index("test-index", docs, chunk_size=1)
    assert isinstance(res, list)


@pytest.mark.asyncio
async def test_scroll(monkeypatch):
    oc = OpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    hits = []
    async for h in oc.scroll("idx", {"query": {"match_all": {}}}):
        hits.append(h)

    assert len(hits) == 2
