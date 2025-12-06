import pytest

from multids.connectors.opensearch import OpenSearchConnector


class FlakyClient:
    def __init__(self, fail_times=2):
        self.calls = 0
        self.fail_times = fail_times

    async def request(self, method, path, **kwargs):
        self.calls += 1
        if self.calls <= self.fail_times:
            # simulate transient server error
            class Resp:
                status_code = 500

                def raise_for_status(self):
                    raise Exception("server error")

            return Resp()
        else:

            class Resp:
                status_code = 200

                def raise_for_status(self):
                    return None

                def json(self):
                    return {"ok": True}

            return Resp()


@pytest.mark.asyncio
async def test_request_retries():
    oc = OpenSearchConnector("http://localhost:9200", max_retries=3, backoff_factor=0.01)
    flaky = FlakyClient(fail_times=2)
    oc._client = flaky

    # call internal _request and ensure it retries (calls > 1)
    resp = await oc._request("GET", "/_cluster/health")
    assert resp.json() == {"ok": True}
    assert flaky.calls > 1
