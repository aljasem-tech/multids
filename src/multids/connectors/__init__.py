"""
Connector subpackage exports.

Avoid importing heavy optional dependencies at package import time. Import
individual connector modules directly (e.g. `from multids.connectors.opensearch import OpenSearchConnector`) or
access them via importlib when needed.
"""

from .athena import AthenaConnector
from .local import LocalConnector
from .mssql import MSSQLConnector
from .opensearch import OpenSearchConnector
from .s3 import S3Connector
from .sql import MySQLConnector, SQLConnectorBase

__all__ = [
    "S3Connector",
    "LocalConnector",
    "SQLConnectorBase",
    "MySQLConnector",
    "OpenSearchConnector",
    "AthenaConnector",
    "MSSQLConnector",
]
