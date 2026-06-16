from datetime import datetime, timezone
from enum import Enum


class Partitioning(str, Enum):
    MINUTES = "MINUTES"
    HOURS = "HOURS"
    DAYS = "DAYS"
    MONTHS = "MONTHS"
    YEARS = "YEARS"
    INDEFINITE = "INDEFINITE"


def compute_partition(ts_ms: int, strategy: Partitioning) -> int:
    if strategy == Partitioning.INDEFINITE:
        return 0
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    if strategy == Partitioning.MINUTES:
        t = dt.replace(second=0, microsecond=0)
    elif strategy == Partitioning.HOURS:
        t = dt.replace(minute=0, second=0, microsecond=0)
    elif strategy == Partitioning.DAYS:
        t = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif strategy == Partitioning.MONTHS:
        t = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif strategy == Partitioning.YEARS:
        t = dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return int(t.timestamp() * 1000)
