import pytest

from multids.errors import ConnectorConnectionError, ConnectorDependencyError
from multids.reliability import RetryConfig, retry_async


@pytest.mark.asyncio
async def test_retry_async_retries_transient_errors_without_waiting(monkeypatch):
    attempts = 0

    async def operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectorConnectionError("temporary failure")
        return "ok"

    async def no_sleep(delay):
        assert delay >= 0

    monkeypatch.setattr("multids.reliability.asyncio.sleep", no_sleep)
    result = await retry_async(operation, config=RetryConfig(max_retries=2, backoff_factor=0.1), operation_name="test")

    assert result == "ok"
    assert attempts == 3


@pytest.mark.asyncio
async def test_opensearch_dependency_error_is_not_retried():
    from multids.connectors.opensearch import OpenSearchConnector

    connector = OpenSearchConnector("http://localhost:9200")
    connector._client = None
    with pytest.raises(ConnectorDependencyError):
        await connector._request("GET", "/")
