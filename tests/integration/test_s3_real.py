"""Opt-in integration coverage for a real S3-compatible service."""

import os
import uuid

import pytest

from multids.connectors.s3 import S3Connector
from multids.contracts import ObjectRef


@pytest.mark.asyncio
@pytest.mark.real_integration
async def test_s3_round_trip_against_configured_service():
    bucket = os.environ.get("MULTIDS_S3_TEST_BUCKET")
    if not bucket or not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        pytest.skip("Set S3 test bucket and AWS credentials to run real S3 integration tests")

    key = f"multids-ci/{uuid.uuid4()}.txt"
    connector = S3Connector(region_name=os.environ.get("AWS_REGION"))
    try:
        await connector.write_bytes(b"multids", bucket, key)
        assert await connector.read_bytes(bucket, key) == b"multids"
        page = await connector.list_object_page(ObjectRef(key, bucket), page_size=1)
        assert page.items and page.items[0].key == key
    finally:
        await connector.delete_object(bucket, key)
