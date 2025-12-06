from unittest.mock import AsyncMock, MagicMock

import pytest

from multids.connectors.sql import SQLConnectorBase


@pytest.mark.asyncio
async def test_fetch_rows_mappings():
    from unittest.mock import patch

    with patch("multids.connectors.sql.create_async_engine") as mock_create_engine:
        # Setup mocks
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_conn = AsyncMock()
        mock_result = AsyncMock()

        # engine.connect() -> conn
        mock_cm = AsyncMock()
        mock_cm.__aenter__.return_value = mock_conn
        mock_engine.connect.return_value = mock_cm

        # conn.stream() -> result
        mock_conn.stream.return_value = mock_result

        # result.mappings is synchronous, returns async iterator
        # Force it to be MagicMock, not AsyncMock
        mock_result.mappings = MagicMock()

        async def fake_mappings():
            yield {"col": "val1"}
            yield {"col": "val2"}

        # Mock the mappings method
        mock_result.mappings.return_value = fake_mappings()

        # No specific driver needed since we mock engine creation
        c = SQLConnectorBase("sqlite+aiosqlite:///:memory:")
        c._engine = mock_engine  # Explicitly set it too just in case

        rows = []
        async for row in c.fetch_rows("SELECT *"):
            rows.append(row)

        assert len(rows) == 2
        assert rows[0] == {"col": "val1"}
        assert mock_result.mappings.called
