"""
CMS — Surveillance Analytics Utilities
=======================================
Shared computation logic used by the surveillance router.

  - Time-series aggregation
  - Spike detection via rolling z-score
  - Geographic clustering helpers
"""

from __future__ import annotations

import math
import statistics
from datetime import date, timedelta
from typing import Literal

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ...database import SearchEvent
from .surveillance import DispensingRecord, SurveillanceAlert

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Z-score thresholds for spike severity classification
SPIKE_LOW    = 1.5
SPIKE_MEDIUM = 2.0
SPIKE_HIGH   = 3.0

Period = Literal["daily", "weekly", "monthly"]


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _period_trunc(d: date, period: Period) -> date:
    """Truncate a date to the start of its period bucket."""
    if period == "daily":
        return d
    if period == "weekly":
        # ISO week starts Monday
        return d - timedelta(days=d.weekday())
    # monthly
    return d.replace(day=1)


def _date_range(start: date, end: date, period: Period) -> list[date]:
    """Generate all period-start dates between start and end inclusive."""
    buckets = []
    current = _period_trunc(start, period)
    while current <= end:
        buckets.append(current)
        if period == "daily":
            current += timedelta(days=1)
        elif period == "weekly":
            current += timedelta(weeks=1)
        else:
            # advance one month
            month = current.month + 1
            year = current.year + (1 if month > 12 else 0)
            month = month if month <= 12 else 1
            current = current.replace(year=year, month=month, day=1)
    return buckets


# ---------------------------------------------------------------------------
# Spike detection
# ---------------------------------------------------------------------------

def detect_spike(
    series: list[int],
    baseline_window: int = 4,
) -> tuple[float, float, str]:
    """
    Given a time-ordered list of counts (most recent last),
    compute z-score of the last value against the rolling baseline window.

    Returns (z_score, baseline_mean, severity).
    """
    if len(series) < 2:
        return 0.0, float(series[0]) if series else 0.0, "low"

    baseline = series[:-1][-baseline_window:]
    current = series[-1]

    mean = statistics.mean(baseline) if baseline else 0.0
    std = statistics.stdev(baseline) if len(baseline) >= 2 else 0.0

    z = (current - mean) / std if std > 0 else 0.0

    if z >= SPIKE_HIGH:
        severity = "high"
    elif z >= SPIKE_MEDIUM:
        severity = "medium"
    elif z >= SPIKE_LOW:
        severity = "low"
    else:
        severity = "none"

    return round(z, 3), round(mean, 2), severity


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

async def get_search_trend(
    db: AsyncSession,
    icd_code: str,
    start: date,
    end: date,
    period: Period,
    hasc_id: str | None = None,
) -> list[dict]:
    """
    Aggregate search_events matching an ICD code or disease keyword
    into period buckets.
    NOTE: search_events don't carry hasc_id — district filter not applied here.
    """
    # Pull all matching search events in range
    stmt = select(SearchEvent).where(
        SearchEvent.entity_type.in_(["disease", "all"]),
        func.lower(SearchEvent.query).contains(icd_code.lower()),
        func.date(SearchEvent.created_at) >= start,
        func.date(SearchEvent.created_at) <= end,
    )
    result = await db.execute(stmt)
    events = result.scalars().all()

    # Bucket by period
    buckets: dict[date, int] = {d: 0 for d in _date_range(start, end, period)}
    for ev in events:
        bucket = _period_trunc(ev.created_at.date(), period)
        if bucket in buckets:
            buckets[bucket] += 1

    return [
        {"period": str(k), "search_count": v}
        for k, v in sorted(buckets.items())
    ]


async def get_dispensing_trend(
    db: AsyncSession,
    start: date,
    end: date,
    period: Period,
    icd_code: str | None = None,
    atc_code: str | None = None,
    hasc_id: str | None = None,
) -> list[dict]:
    """
    Aggregate dispensing_records into period buckets.
    Filter by ICD code, ATC prefix, or district.
    """
    stmt = select(DispensingRecord).where(
        DispensingRecord.dispensed_at >= start,
        DispensingRecord.dispensed_at <= end,
    )
    if icd_code:
        stmt = stmt.where(
            func.lower(DispensingRecord.icd_code) == icd_code.lower()
        )
    if atc_code:
        stmt = stmt.where(
            DispensingRecord.atc_code.startswith(atc_code.upper())
        )
    if hasc_id:
        stmt = stmt.where(DispensingRecord.hasc_id == hasc_id.upper())

    result = await db.execute(stmt)
    records = result.scalars().all()

    buckets: dict[date, dict] = {
        d: {"quantity": 0, "events": 0}
        for d in _date_range(start, end, period)
    }
    for rec in records:
        bucket = _period_trunc(rec.dispensed_at, period)
        if bucket in buckets:
            buckets[bucket]["quantity"] += rec.quantity
            buckets[bucket]["events"] += 1

    return [
        {
            "period": str(k),
            "dispensing_events": v["events"],
            "total_quantity": v["quantity"],
        }
        for k, v in sorted(buckets.items())
    ]
