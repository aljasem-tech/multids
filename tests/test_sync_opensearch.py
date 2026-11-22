from multids.connectors.sync.opensearch import SyncOpenSearchConnector


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
        self.calls = []
        self._scroll_round = 0

    def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        if path.startswith("/_bulk"):
            return DummyResponse(200, {"took": 1})

        if path.startswith("/") and path.endswith("_search?scroll=1m"):
            self._scroll_round = 1
            return DummyResponse(200, {"_scroll_id": "s1", "hits": {"hits": [{"_id": "1"}]}})

        if path == "/_search/scroll":
            if self._scroll_round == 1:
                self._scroll_round = 2
                return DummyResponse(200, {"_scroll_id": "s2", "hits": {"hits": [{"_id": "2"}]}})
            else:
                return DummyResponse(200, {"_scroll_id": None, "hits": {"hits": []}})

        return DummyResponse(200, {})

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)

    def post(self, path, **kwargs):
        return self.request("POST", path, **kwargs)

    def delete(self, path, **kwargs):
        return self.request("DELETE", path, **kwargs)

    def close(self):
        return


def test_bulk_index():
    oc = SyncOpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    docs = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    res = oc.bulk_index("test-index", docs, chunk_size=1)
    assert isinstance(res, list)


def test_scroll():
    oc = SyncOpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    hits = list(oc.scroll("idx", {"query": {"match_all": {}}}))
    assert len(hits) == 2


def test_index_doc():
    oc = SyncOpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    result = oc.index_doc("test-index", {"title": "hello"}, id="doc1")
    assert isinstance(result, dict)


def test_search():
    oc = SyncOpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    result = oc.search("test-index", {"query": {"match_all": {}}}, size=5)
    assert isinstance(result, dict)


def test_write_records():
    oc = SyncOpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    result = oc.write_records("test-index", records, batch_size=1)
    assert result.items_written == 2


def test_ping():
    oc = SyncOpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    assert oc.ping() is True


def test_close():
    oc = SyncOpenSearchConnector("http://localhost:9200")
    dummy = DummyClient()
    oc._client = dummy

    oc.close()
    assert oc._client is None
