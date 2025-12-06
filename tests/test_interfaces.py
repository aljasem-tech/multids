from multids import interfaces


def test_interfaces_import():
    assert hasattr(interfaces, "Connector")
