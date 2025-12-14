from unittest.mock import MagicMock, patch

from multids.connectors.sync.local import SyncLocalConnector
from multids.connectors.sync.s3 import SyncS3Connector


def test_local_connector_read_write(tmp_path):
    connector = SyncLocalConnector(base_path=str(tmp_path))
    test_file = "test.txt"
    content = b"Hello Sync World"

    # Write bytes
    connector.write_bytes(content, test_file)
    assert (tmp_path / test_file).read_bytes() == content

    # Read bytes
    assert connector.read_bytes(test_file) == content

    # Read stream
    chunks = list(connector.read_stream(test_file, chunk_size=5))
    assert b"".join(chunks) == content


def test_local_connector_json(tmp_path):
    connector = SyncLocalConnector(base_path=str(tmp_path))
    data = {"foo": "bar", "unicode": "こんにちは"}

    connector.write_json(data, "test.json")
    read_data = connector.read_json("test.json")

    assert read_data == data
    # Check raw file for no-escape
    raw = (tmp_path / "test.json").read_text(encoding="utf-8")
    assert "こんにちは" in raw


@patch("multids.connectors.sync.s3.boto3")
def test_s3_connector_init(mock_boto):
    mock_session = MagicMock()
    mock_boto.Session.return_value = mock_session

    c = SyncS3Connector("eu-central-1")
    mock_boto.Session.assert_called_with(
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        profile_name=None,
        region_name="eu-central-1",
    )
    mock_session.client.assert_called_with("s3")
    assert isinstance(c, SyncS3Connector)


@patch("multids.connectors.sync.s3.boto3")
def test_s3_read_bytes(mock_boto):
    mock_session = MagicMock()
    mock_client = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto.Session.return_value = mock_session

    c = SyncS3Connector()

    # Mock Body with iter_chunks
    mock_body = MagicMock()
    mock_body.iter_chunks.return_value = iter([b"chunk1", b"chunk2"])
    mock_client.get_object.return_value = {"Body": mock_body}

    data = c.read_bytes("bucket", "key")
    assert data == b"chunk1chunk2"
    mock_client.get_object.assert_called_with(Bucket="bucket", Key="key")


@patch("multids.connectors.sync.s3.boto3")
def test_s3_write_bytes(mock_boto):
    mock_session = MagicMock()
    mock_client = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto.Session.return_value = mock_session

    c = SyncS3Connector()
    c.write_bytes(b"data", "bucket", "key")

    mock_client.put_object.assert_called_with(Bucket="bucket", Key="key", Body=b"data")


@patch("multids.connectors.sync.s3.boto3")
def test_s3_write_stream(mock_boto):
    mock_session = MagicMock()
    mock_client = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto.Session.return_value = mock_session

    c = SyncS3Connector()
    stream = iter([b"part1", b"part2"])

    c.write_stream(stream, "bucket", "key")

    mock_client.upload_fileobj.assert_called()
    call_args = mock_client.upload_fileobj.call_args
    assert call_args[0][1] == "bucket"
    assert call_args[0][2] == "key"

    # Check the file-like object
    fileobj = call_args[0][0]
    assert fileobj.read() == b"part1part2"
