from unittest.mock import MagicMock, patch

from multids.connectors.sync.athena import SyncAthenaConnector
from multids.connectors.sync.mssql import SyncMSSQLConnector
from multids.connectors.sync.sql import SyncMySQLConnector, SyncSQLConnectorBase

# --- Athena Tests ---


@patch("multids.connectors.sync.athena.boto3")
def test_athena_query(mock_boto):
    mock_client = MagicMock()
    mock_boto.Session.return_value.client.return_value = mock_client

    c = SyncAthenaConnector("eu-central-1")

    # Mock start query
    mock_client.start_query_execution.return_value = {"QueryExecutionId": "q123"}

    # Mock wait query (success)
    mock_client.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    # Mock get results (paginator)
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator

    # Page 1: Metadata + Header + Data
    page1 = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Name": "id"}, {"Name": "val"}]},
            "Rows": [
                {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "val"}]},  # Header
                {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "A"}]},
            ],
        }
    }
    mock_paginator.paginate.return_value = [page1]

    results = c.fetch_all("SELECT * FROM t")
    assert len(results) == 1
    assert results[0] == {"id": "1", "val": "A"}

    mock_client.start_query_execution.assert_called_once()


# --- SQL Tests ---


@patch("multids.connectors.sync.sql.create_engine")
def test_sql_connector(mock_create):
    mock_engine = MagicMock()
    mock_create.return_value = mock_engine
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    c = SyncSQLConnectorBase("sqlite:///:memory:")

    # Execute
    c.execute("INSERT INTO t VALUES (1)")
    mock_conn.execute.assert_called()

    # Fetch
    mock_result = MagicMock()
    mock_result.mappings.return_value = [{"id": 1}]
    # mocking strict sqlalchemy behavior chain
    mock_conn.execution_options.return_value.execute.return_value = mock_result

    rows = list(c.fetch_rows("SELECT * FROM t"))
    assert rows == [{"id": 1}]


@patch("multids.connectors.sync.sql.create_engine")
def test_mysql_bulk_insert(mock_create):
    mock_engine = MagicMock()
    mock_create.return_value = mock_engine
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    c = SyncMySQLConnector("mysql://...")

    rows = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    c.bulk_insert("table", rows, chunk_size=2)

    mock_conn.execute.assert_called_once()
    # Check that it constructed the INSERT statement
    args = mock_conn.execute.call_args
    assert "INSERT INTO table" in str(args[0][0])
    assert args[0][1] == rows  # The chunk passed down


# --- MSSQL Tests ---


@patch("multids.connectors.sync.mssql.pyodbc")
def test_mssql_connector(mock_pyodbc):
    mock_conn = MagicMock()
    mock_pyodbc.connect.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    c = SyncMSSQLConnector(dsn="DSN=test")

    # Execute
    c.execute("INSERT INTO t VALUES (?)", [1])
    mock_cursor.execute.assert_called_with("INSERT INTO t VALUES (?)", [1])
    mock_conn.commit.assert_called()

    # Fetch
    mock_cursor.description = [("id",), ("val",)]
    mock_cursor.__iter__.return_value = iter([(1, "a")])

    rows = c.fetch_rows("SELECT * FROM t")
    assert rows == [{"id": 1, "val": "a"}]


@patch("multids.connectors.sync.mssql.pyodbc")
def test_mssql_bulk_insert(mock_pyodbc):
    mock_conn = MagicMock()
    mock_pyodbc.connect.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    c = SyncMSSQLConnector(dsn="DSN=test")
    rows = [[1, "a"], [2, "b"]]
    c.bulk_insert("table", ["id", "val"], rows)

    # Check fast_executemany set
    assert mock_cursor.fast_executemany is True
    mock_cursor.executemany.assert_called()
    args = mock_cursor.executemany.call_args
    assert "INSERT INTO table" in args[0][0]
    assert args[0][1] == rows
