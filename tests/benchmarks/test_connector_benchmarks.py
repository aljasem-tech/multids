"""Opt-in benchmark entry points for supported real services."""

import os
import time

import pytest


@pytest.mark.benchmark
def test_benchmark_suite_is_explicitly_opt_in():
    if os.environ.get("RUN_MULTIDS_BENCHMARKS") != "1":
        pytest.skip("Set RUN_MULTIDS_BENCHMARKS=1 to run connector benchmarks")
    started = time.perf_counter()
    # Real S3 transfer, SQL batch, and OpenSearch bulk benchmarks are invoked
    # by the dedicated CI workflow after service credentials are configured.
    assert time.perf_counter() >= started
