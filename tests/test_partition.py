import pytest
from migrator.partition import compute_partition, Partitioning


def test_months_partition_june():
    # 2024-06-15 10:00:00 UTC in ms → should return 2024-06-01 00:00:00 UTC in ms
    ts_ms = 1718445600000  # 2024-06-15 10:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.MONTHS)
    assert result == 1717200000000  # 2024-06-01 00:00:00 UTC


def test_months_partition_jan():
    ts_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.MONTHS)
    assert result == 1704067200000  # already start of month


def test_days_partition():
    ts_ms = 1718445600000  # 2024-06-15 10:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.DAYS)
    assert result == 1718409600000  # 2024-06-15 00:00:00 UTC


def test_years_partition():
    ts_ms = 1718445600000  # 2024-06-15 → 2024-01-01 00:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.YEARS)
    assert result == 1704067200000  # 2024-01-01 00:00:00 UTC


def test_indefinite_partition():
    ts_ms = 1718445600000
    result = compute_partition(ts_ms, Partitioning.INDEFINITE)
    assert result == 0


def test_hours_partition():
    ts_ms = 1718445600000  # 2024-06-15 10:00:00 UTC
    result = compute_partition(ts_ms, Partitioning.HOURS)
    assert result == 1718445600000  # already on the hour


def test_hours_partition_mid_hour():
    ts_ms = 1718447123000  # 2024-06-15 10:25:23 UTC
    result = compute_partition(ts_ms, Partitioning.HOURS)
    assert result == 1718445600000  # 2024-06-15 10:00:00 UTC
