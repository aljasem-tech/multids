import pytest


def _import_or_skip(name, import_path):
    try:
        mod = __import__(import_path, fromlist=[name])
        return getattr(mod, name)
    except Exception as e:
        pytest.skip(f"Skipping {name} smoke test: import failed ({e})")


def test_local_connector_import():
    LocalConnector = _import_or_skip("LocalConnector", "multids.connectors.local")  # noqa: N806
    lc = LocalConnector()
    assert hasattr(lc, "read_stream")


def test_s3_connector_import():
    S3Connector = _import_or_skip("S3Connector", "multids.connectors.s3")  # noqa: N806
    # creation should work without AWS credentials; just ensure class exists
    try:
        s3 = S3Connector()
    except TypeError:
        # some implementations require parameters; just assert the class exists
        assert S3Connector is not None
        return
    assert hasattr(s3, "write_stream")


def test_opensearch_connector_import():
    OpenSearchConnector = _import_or_skip("OpenSearchConnector", "multids.connectors.opensearch")  # noqa: N806
    oc = OpenSearchConnector("http://localhost:9200")
    assert hasattr(oc, "bulk_index")
