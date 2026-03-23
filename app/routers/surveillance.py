"""
CMS — Disease Surveillance Router
===================================
Passive disease surveillance derived from search patterns
and medicine dispensing behaviour. All data is aggregate —
no patient identifiers are exposed.

Endpoints
---------
  GET  /api/v1/cms/surveillance/search-trends
       Time-series of disease-related search volume.

  GET  /api/v1/cms/surveillance/dispensing-trends
       Time-series of medicine dispensing events linked to a disease.

  GET  /api/v1/cms/surveillance/combined-trends
       Search + dispensing on one timeline for a given ICD code.

  GET  /api/v1/cms/surveillance/geographic-clusters
       Per-district dispensing counts — feeds the choropleth map layer.

  GET  /api/v1/cms/surveillance/spikes
       Active spike alerts ranked by severity and recency.

  POST /api/v1/cms/surveillance/spikes/detect
       Trigger on-demand spike detection across all diseases/districts
       for a given lookback period. Run periodically via a scheduler.

  GET  /api/v1/cms/surveillance/top-diseases
       Ranked diseases by search + dispensing volume in a period.

  GET  /api/v1/cms/surveillance/top-medicines
       Ranked medicines by dispensing volume in a period.
"""

from __future__ import annotations

import statistics
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import SearchEvent, get_db
from models.surveillance import DispensingRecord, SurveillanceAlert
from surveillance.utils import (
    Period,
    _date_range,
    _period_trunc,
    detect_spike,
    get_dispensing_trend,
    get_search_trend,
    SPIKE_LOW, SPIKE_MEDIUM, SPIKE_HIGH,
)

router = APIRouter(
    prefix="/api/v1/cms/surveillance",
    tags=["CMS – Disease Surveillance"],
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_window(days: int = 90) -> tuple[date, date]:
    end = date.today()
    start = end - timedelta(days=days)
    return start, end


# ---------------------------------------------------------------------------
# GET /search-trends
# ---------------------------------------------------------------------------

@router.get("/search-trends", summary="Disease search volume over time")
async def search_trends(
    icd_code: str = Query(..., description="ICD-10 code or keyword, e.g. 'A00' or 'cholera'"),
    start: Optional[date] = Query(None, description="Start date (default: 90 days ago)"),
    end: Optional[date] = Query(None, description="End date (default: today)"),
    period: Period = Query("weekly", description="Aggregation period: daily | weekly | monthly"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns how frequently a disease (by ICD-10 code or keyword) was
    searched over time. Derived from recorded **search_events**.

    Useful for detecting early-warning signals — a rise in searches for
    a disease often precedes confirmed case counts.
    """
    start = start or _default_window()[0]
    end = end or date.today()

    if start > end:
        raise HTTPException(status_code=400, detail="start must be before end.")

    trend = await get_search_trend(db, icd_code, start, end, period)
    counts = [t["search_count"] for t in trend]
    z_score, baseline, severity = detect_spike(counts) if len(counts) >= 2 else (0.0, 0.0, "none")

    return {
        "meta": {
            "timestamp": _now_iso(),
            "icd_code": icd_code,
            "period": period,
            "start": str(start),
            "end": str(end),
            "total_searches": sum(counts),
            "spike": {"z_score": z_score, "baseline": baseline, "severity": severity},
        },
        "data": trend,
    }


# ---------------------------------------------------------------------------
# GET /dispensing-trends
# ---------------------------------------------------------------------------

@router.get("/dispensing-trends", summary="Medicine dispensing volume over time")
async def dispensing_trends(
    icd_code: Optional[str] = Query(None, description="Filter by ICD-10 code"),
    atc_code: Optional[str] = Query(None, description="Filter by ATC code prefix e.g. 'J01' (antibiotics)"),
    hasc_id: Optional[str] = Query(None, description="Filter by district e.g. BW.GB"),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    period: Period = Query("weekly"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns dispensing event counts and quantities over time.
    Filter by disease (ICD-10), medicine category (ATC), or district.

    At least one of `icd_code` or `atc_code` is recommended to avoid
    returning aggregate totals across all medicines.
    """
    if not icd_code and not atc_code:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: icd_code, atc_code."
        )

    start = start or _default_window()[0]
    end = end or date.today()

    trend = await get_dispensing_trend(db, start, end, period, icd_code, atc_code, hasc_id)
    counts = [t["dispensing_events"] for t in trend]
    z_score, baseline, severity = detect_spike(counts) if len(counts) >= 2 else (0.0, 0.0, "none")

    return {
        "meta": {
            "timestamp": _now_iso(),
            "icd_code": icd_code,
            "atc_code": atc_code,
            "hasc_id": hasc_id,
            "period": period,
            "start": str(start),
            "end": str(end),
            "total_events": sum(counts),
            "total_quantity": sum(t["total_quantity"] for t in trend),
            "spike": {"z_score": z_score, "baseline": baseline, "severity": severity},
        },
        "data": trend,
    }


# ---------------------------------------------------------------------------
# GET /combined-trends
# ---------------------------------------------------------------------------

@router.get("/combined-trends", summary="Search + dispensing on one timeline")
async def combined_trends(
    icd_code: str = Query(..., description="ICD-10 code to correlate"),
    atc_code: Optional[str] = Query(None, description="ATC code for related medicine"),
    hasc_id: Optional[str] = Query(None),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    period: Period = Query("weekly"),
    db: AsyncSession = Depends(get_db),
):
    """
    Merges search volume and dispensing volume into a single timeline.

    Correlating searches with dispensing helps distinguish:
      - **Informational searches** (search ↑, dispensing flat) — public concern
      - **Clinical activity** (search ↑, dispensing ↑) — active cases being treated
      - **Supply-driven dispensing** (dispensing ↑, search flat) — routine restocking
    """
    start = start or _default_window()[0]
    end = end or date.today()

    search = await get_search_trend(db, icd_code, start, end, period)
    dispensing = await get_dispensing_trend(
        db, start, end, period, icd_code, atc_code, hasc_id
    )

    # Merge on period key
    dispensing_map = {d["period"]: d for d in dispensing}
    merged = []
    for s in search:
        p = s["period"]
        d = dispensing_map.get(p, {"dispensing_events": 0, "total_quantity": 0})
        merged.append({
            "period": p,
            "search_count": s["search_count"],
            "dispensing_events": d["dispensing_events"],
            "total_quantity": d["total_quantity"],
        })

    return {
        "meta": {
            "timestamp": _now_iso(),
            "icd_code": icd_code,
            "atc_code": atc_code,
            "hasc_id": hasc_id,
            "period": period,
            "start": str(start),
            "end": str(end),
        },
        "data": merged,
    }


# ---------------------------------------------------------------------------
# GET /geographic-clusters
# ---------------------------------------------------------------------------

@router.get("/geographic-clusters", summary="Per-district disease activity map layer")
async def geographic_clusters(
    icd_code: Optional[str] = Query(None, description="Filter by ICD-10 code"),
    atc_code: Optional[str] = Query(None, description="Filter by ATC code prefix"),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns dispensing counts grouped by district (HASC code).
    Feed directly to the map layer as a choropleth data source.

    Each district entry includes:
      - dispensing_events count
      - total_quantity dispensed
      - top medicines dispensed in that district
      - relative intensity score (0–1) for map colour scaling
    """
    start = start or _default_window()[0]
    end = end or date.today()

    stmt = select(DispensingRecord).where(
        DispensingRecord.dispensed_at >= start,
        DispensingRecord.dispensed_at <= end,
        DispensingRecord.hasc_id.isnot(None),
    )
    if icd_code:
        stmt = stmt.where(func.lower(DispensingRecord.icd_code) == icd_code.lower())
    if atc_code:
        stmt = stmt.where(DispensingRecord.atc_code.startswith(atc_code.upper()))

    result = await db.execute(stmt)
    records = result.scalars().all()

    # Aggregate per district
    district_data: dict[str, dict] = {}
    for rec in records:
        hid = rec.hasc_id
        if hid not in district_data:
            district_data[hid] = {
                "hasc_id": hid,
                "dispensing_events": 0,
                "total_quantity": 0,
                "medicines": {},
            }
        district_data[hid]["dispensing_events"] += 1
        district_data[hid]["total_quantity"] += rec.quantity
        med = rec.product_name
        district_data[hid]["medicines"][med] = (
            district_data[hid]["medicines"].get(med, 0) + rec.quantity
        )

    # Normalise intensity 0–1 across districts
    max_events = max((d["dispensing_events"] for d in district_data.values()), default=1)

    clusters = []
    for d in sorted(district_data.values(), key=lambda x: -x["dispensing_events"]):
        top_meds = sorted(d["medicines"].items(), key=lambda x: -x[1])[:5]
        clusters.append({
            "hasc_id": d["hasc_id"],
            "dispensing_events": d["dispensing_events"],
            "total_quantity": d["total_quantity"],
            "intensity": round(d["dispensing_events"] / max_events, 4),
            "top_medicines": [
                {"medicine": m, "quantity": q} for m, q in top_meds
            ],
        })

    return {
        "meta": {
            "timestamp": _now_iso(),
            "icd_code": icd_code,
            "atc_code": atc_code,
            "start": str(start),
            "end": str(end),
            "total_districts": len(clusters),
        },
        "data": clusters,
    }


# ---------------------------------------------------------------------------
# GET /spikes
# ---------------------------------------------------------------------------

@router.get("/spikes", summary="Active spike alerts ranked by severity")
async def get_spikes(
    severity: Optional[str] = Query(
        None, description="Filter by severity: low | medium | high"
    ),
    hasc_id: Optional[str] = Query(None, description="Filter by district"),
    alert_type: Optional[str] = Query(
        None, description="search_spike | dispensing_spike"
    ),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns active spike alerts generated by the detector.
    Sorted by severity (high → medium → low) then most recent first.
    """
    severity_order = {"high": 0, "medium": 1, "low": 2}

    stmt = select(SurveillanceAlert).where(SurveillanceAlert.is_active == True)  # noqa
    if severity:
        stmt = stmt.where(SurveillanceAlert.severity == severity.lower())
    if hasc_id:
        stmt = stmt.where(SurveillanceAlert.hasc_id == hasc_id.upper())
    if alert_type:
        stmt = stmt.where(SurveillanceAlert.alert_type == alert_type.lower())

    stmt = stmt.order_by(SurveillanceAlert.created_at.desc()).limit(limit * 3)
    result = await db.execute(stmt)
    alerts = result.scalars().all()

    # Sort: severity first, then recency
    alerts_sorted = sorted(
        alerts,
        key=lambda a: (severity_order.get(a.severity, 9), -a.created_at.timestamp())
    )[:limit]

    return {
        "meta": {
            "timestamp": _now_iso(),
            "total_active_alerts": len(alerts_sorted),
        },
        "data": [
            {
                "id": a.id,
                "alert_type": a.alert_type,
                "signal": a.signal,
                "hasc_id": a.hasc_id,
                "district_name": a.district_name,
                "period_start": str(a.period_start),
                "period_end": str(a.period_end),
                "observed_count": a.observed_count,
                "baseline_count": a.baseline_count,
                "z_score": a.z_score,
                "severity": a.severity,
                "created_at": a.created_at.isoformat(),
            }
            for a in alerts_sorted
        ],
    }


# ---------------------------------------------------------------------------
# POST /spikes/detect
# ---------------------------------------------------------------------------

@router.post("/spikes/detect", summary="Run spike detection — call via scheduler")
async def detect_spikes(
    lookback_weeks: int = Query(
        8, ge=4, le=52,
        description="Weeks of history to use for baseline calculation"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Scans recent search and dispensing data to detect anomalous spikes.
    Persists new **SurveillanceAlert** rows for anything above threshold.

    **Run this on a schedule** (e.g. nightly via APScheduler or a cron job)
    rather than on every request.

    Detection method: rolling z-score.
      - z ≥ 1.5 → low
      - z ≥ 2.0 → medium
      - z ≥ 3.0 → high
    """
    end = date.today()
    start = end - timedelta(weeks=lookback_weeks)
    alerts_created = 0

    # ── 1. Search spike detection per ICD-10 keyword ──────────────────────
    # Pull distinct disease queries from search_events
    stmt = select(SearchEvent.query).where(
        SearchEvent.entity_type.in_(["disease", "all"]),
        func.date(SearchEvent.created_at) >= start,
    ).distinct()
    result = await db.execute(stmt)
    disease_queries = [r[0] for r in result.fetchall()]

    for query in set(disease_queries):
        trend = await get_search_trend(db, query, start, end, "weekly")
        counts = [t["search_count"] for t in trend]
        if len(counts) < 4 or max(counts) == 0:
            continue

        z, baseline, severity = detect_spike(counts)
        if severity == "none":
            continue

        alert = SurveillanceAlert(
            alert_type="search_spike",
            signal=query,
            hasc_id=None,
            district_name=None,
            period_start=start,
            period_end=end,
            observed_count=counts[-1],
            baseline_count=baseline,
            z_score=z,
            severity=severity,
            is_active=True,
        )
        db.add(alert)
        alerts_created += 1

    # ── 2. Dispensing spike detection per ICD code + district ─────────────
    stmt = select(
        DispensingRecord.icd_code,
        DispensingRecord.hasc_id,
        DispensingRecord.icd_description,
    ).where(
        DispensingRecord.dispensed_at >= start,
        DispensingRecord.icd_code.isnot(None),
        DispensingRecord.hasc_id.isnot(None),
    ).distinct()
    result = await db.execute(stmt)
    combos = result.fetchall()

    for icd_code, hasc_id, icd_desc in combos:
        trend = await get_dispensing_trend(
            db, start, end, "weekly", icd_code=icd_code, hasc_id=hasc_id
        )
        counts = [t["dispensing_events"] for t in trend]
        if len(counts) < 4 or max(counts) == 0:
            continue

        z, baseline, severity = detect_spike(counts)
        if severity == "none":
            continue

        alert = SurveillanceAlert(
            alert_type="dispensing_spike",
            signal=f"{icd_code} – {icd_desc or ''}".strip(" –"),
            hasc_id=hasc_id,
            district_name=None,
            period_start=start,
            period_end=end,
            observed_count=counts[-1],
            baseline_count=baseline,
            z_score=z,
            severity=severity,
            is_active=True,
        )
        db.add(alert)
        alerts_created += 1

    await db.commit()

    return {
        "meta": {"timestamp": _now_iso()},
        "data": {
            "alerts_created": alerts_created,
            "lookback_weeks": lookback_weeks,
            "period_start": str(start),
            "period_end": str(end),
        },
    }


# ---------------------------------------------------------------------------
# GET /top-diseases
# ---------------------------------------------------------------------------

@router.get("/top-diseases", summary="Top diseases by combined search + dispensing activity")
async def top_diseases(
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    hasc_id: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """
    Ranked list of diseases by total activity (search volume + dispensing events)
    in the given period.

    Useful for the surveillance dashboard summary card.
    """
    start = start or _default_window()[0]
    end = end or date.today()

    # Search counts per disease query
    stmt = select(SearchEvent.query, func.count().label("search_count")).where(
        SearchEvent.entity_type.in_(["disease", "all"]),
        func.date(SearchEvent.created_at) >= start,
        func.date(SearchEvent.created_at) <= end,
    ).group_by(SearchEvent.query).order_by(func.count().desc()).limit(limit * 2)
    result = await db.execute(stmt)
    search_rows = {r[0]: r[1] for r in result.fetchall()}

    # Dispensing counts per ICD code
    disp_stmt = select(
        DispensingRecord.icd_code,
        DispensingRecord.icd_description,
        func.count().label("disp_count"),
        func.sum(DispensingRecord.quantity).label("total_qty"),
    ).where(
        DispensingRecord.dispensed_at >= start,
        DispensingRecord.dispensed_at <= end,
        DispensingRecord.icd_code.isnot(None),
    )
    if hasc_id:
        disp_stmt = disp_stmt.where(DispensingRecord.hasc_id == hasc_id.upper())
    disp_stmt = disp_stmt.group_by(
        DispensingRecord.icd_code, DispensingRecord.icd_description
    ).order_by(func.count().desc()).limit(limit * 2)

    result = await db.execute(disp_stmt)
    disp_rows = {r[0]: {"desc": r[1], "events": r[2], "qty": r[3]} for r in result.fetchall()}

    # Merge: use ICD code as key, fall back to search query label
    combined: dict[str, dict] = {}
    for icd, d in disp_rows.items():
        combined[icd] = {
            "disease": f"{icd} – {d['desc']}" if d["desc"] else icd,
            "search_count": search_rows.get(icd, 0),
            "dispensing_events": d["events"],
            "total_quantity": d["qty"],
            "activity_score": d["events"] + search_rows.get(icd, 0),
        }

    ranked = sorted(combined.values(), key=lambda x: -x["activity_score"])[:limit]

    return {
        "meta": {
            "timestamp": _now_iso(),
            "start": str(start),
            "end": str(end),
            "hasc_id": hasc_id,
            "total": len(ranked),
        },
        "data": ranked,
    }


# ---------------------------------------------------------------------------
# GET /top-medicines
# ---------------------------------------------------------------------------

@router.get("/top-medicines", summary="Top medicines by dispensing volume")
async def top_medicines(
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    hasc_id: Optional[str] = Query(None),
    icd_code: Optional[str] = Query(None, description="Filter to medicines dispensed for a specific disease"),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """
    Ranked medicines by total quantity dispensed in the period.
    Filter by district or disease to narrow the context.
    Useful for identifying treatment patterns linked to disease activity.
    """
    start = start or _default_window()[0]
    end = end or date.today()

    stmt = select(
        DispensingRecord.nappi_code,
        DispensingRecord.product_name,
        DispensingRecord.atc_code,
        DispensingRecord.atc_description,
        func.count().label("dispensing_events"),
        func.sum(DispensingRecord.quantity).label("total_quantity"),
    ).where(
        DispensingRecord.dispensed_at >= start,
        DispensingRecord.dispensed_at <= end,
    )
    if hasc_id:
        stmt = stmt.where(DispensingRecord.hasc_id == hasc_id.upper())
    if icd_code:
        stmt = stmt.where(func.lower(DispensingRecord.icd_code) == icd_code.lower())

    stmt = stmt.group_by(
        DispensingRecord.nappi_code,
        DispensingRecord.product_name,
        DispensingRecord.atc_code,
        DispensingRecord.atc_description,
    ).order_by(func.sum(DispensingRecord.quantity).desc()).limit(limit)

    result = await db.execute(stmt)
    rows = result.fetchall()

    return {
        "meta": {
            "timestamp": _now_iso(),
            "start": str(start),
            "end": str(end),
            "hasc_id": hasc_id,
            "icd_code": icd_code,
            "total": len(rows),
        },
        "data": [
            {
                "nappi_code": r[0],
                "product_name": r[1],
                "atc_code": r[2],
                "atc_description": r[3],
                "dispensing_events": r[4],
                "total_quantity": r[5],
            }
            for r in rows
        ],
    }