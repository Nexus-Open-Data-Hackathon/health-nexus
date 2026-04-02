"""
CMS — Universal + Per-Use-Case Search Router
=============================================
Search index is built from PostgreSQL on first request, then cached
in memory. An asyncio.Lock prevents duplicate builds under concurrent
startup requests.

  GET  /api/v1/cms/search/autocomplete    all entity types, mixed & ranked
  GET  /api/v1/cms/search/facilities      facilities only
  GET  /api/v1/cms/search/medicines       medicines only
  GET  /api/v1/cms/search/diseases        ICD-10 only
  POST /api/v1/cms/search/click           record a result selection
  POST /api/v1/cms/search/refresh-index   rebuild index from DB without restart
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from rapidfuzz import fuzz, process
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import SearchClick, SearchEvent, get_db
from ..models.core import Facility, Medicine, ICD10Code

router = APIRouter(prefix="/api/v1/cms/search", tags=["CMS – Search"])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# In-memory search index
# Populated from DB on first request, cached until /refresh-index is called.
# asyncio.Lock prevents duplicate DB queries under concurrent requests.
# ---------------------------------------------------------------------------

_search_index: dict[str, list[dict]] | None = None
_index_lock = asyncio.Lock()

_TYPE_QUOTA   = {"facility": 4, "medicine": 4, "disease": 3}
_PREFIX_SCORE = 100
_SUBSTR_SCORE = 75
_FUZZY_CUTOFF = 60


async def _build_index(db: AsyncSession) -> dict[str, list[dict]]:
    """
    Queries PostgreSQL for all facilities, active medicines, and valid
    ICD-10 codes, then builds three scored search buckets.
    Uses ORM models — no raw table name strings.
    Cached after first call; use POST /refresh-index to invalidate.
    """
    global _search_index

    # Fast path — already built
    if _search_index is not None:
        return _search_index

    async with _index_lock:
        # Re-check inside the lock in case another coroutine built it first
        if _search_index is not None:
            return _search_index

        buckets: dict[str, list[dict]] = {
            "facility": [],
            "medicine": [],
            "disease":  [],
        }

        # ── Facilities ─────────────────────────────────────────────────────
        fac_result = await db.execute(
            select(
                Facility.facility_name,
                Facility.facility_type,
                Facility.district_name,
                Facility.hasc_id,
                Facility.latitude,
                Facility.longitude,
                Facility.has_coordinates,
            ).order_by(Facility.facility_name)
        )
        for row in fac_result.mappings().all():
            buckets["facility"].append({
                "label":   row["facility_name"],
                "type":    "facility",
                "subtype": row["facility_type"] or "Other",
                "meta": {
                    "district_name": row["district_name"],
                    "hasc_id":       row["hasc_id"],
                    "coordinates": (
                        {"latitude": row["latitude"], "longitude": row["longitude"]}
                        if row["has_coordinates"] else None
                    ),
                },
                "_search_key": " ".join(filter(None, [
                    str(row["facility_name"]).lower(),
                    str(row["district_name"] or "").lower(),
                ])),
            })

        # ── Medicines (active only) ────────────────────────────────────────
        med_result = await db.execute(
            select(
                Medicine.product_name,
                Medicine.who_atc_description,
                Medicine.nappi_code,
                Medicine.who_atc_code,
                Medicine.dosage_form,
                Medicine.schedule,
                Medicine.supplier_name,
            )
            .where(Medicine.nappi_status == "Active")
            .order_by(Medicine.product_name)
        )
        for row in med_result.mappings().all():
            buckets["medicine"].append({
                "label":   row["product_name"],
                "type":    "medicine",
                "subtype": row["who_atc_description"] or "Medicine",
                "meta": {
                    "nappi_code":  str(row["nappi_code"] or ""),
                    "atc_code":    row["who_atc_code"],
                    "dosage_form": row["dosage_form"],
                    "schedule":    str(row["schedule"] or ""),
                    "supplier":    row["supplier_name"],
                },
                "_search_key": " ".join(filter(None, [
                    str(row["product_name"]).lower(),
                    str(row["who_atc_description"] or "").lower(),
                ])),
            })

        # ── Diseases (billable ICD-10 only) ───────────────────────────────
        dis_result = await db.execute(
            select(
                ICD10Code.code,
                ICD10Code.short_description,
                ICD10Code.long_description,
            )
            .where(ICD10Code.is_valid.is_(True))
            .order_by(ICD10Code.code)
        )
        for row in dis_result.mappings().all():
            buckets["disease"].append({
                "label":   f"{row['code']} – {row['short_description']}",
                "type":    "disease",
                "subtype": "ICD-10",
                "meta": {
                    "icd_code":          row["code"],
                    "short_description": row["short_description"],
                    "long_description":  row["long_description"],
                },
                "_search_key": " ".join([
                    str(row["code"]).lower(),
                    str(row["short_description"] or "").lower(),
                ]),
            })

        _search_index = buckets
        return _search_index


# ---------------------------------------------------------------------------
# Scoring  (prefix → substring → fuzzy)
# ---------------------------------------------------------------------------

def _score(
    candidates: list[dict],
    q_lower: str,
    quota: int,
) -> list[tuple[int, dict]]:
    seen: set[str] = set()
    scored: list[tuple[int, dict]] = []

    for item in candidates:
        if item["_search_key"].startswith(q_lower):
            seen.add(item["_search_key"])
            scored.append((_PREFIX_SCORE, item))

    for item in candidates:
        k = item["_search_key"]
        if k not in seen and q_lower in k:
            seen.add(k)
            scored.append((_SUBSTR_SCORE, item))

    if len(q_lower) >= 3 and len(scored) < quota:
        remaining = [c for c in candidates if c["_search_key"] not in seen]
        keys = [c["_search_key"] for c in remaining]
        if keys:
            for match, score, _ in process.extract(
                q_lower, keys, scorer=fuzz.WRatio,
                limit=quota, score_cutoff=_FUZZY_CUTOFF,
            ):
                for item in remaining:
                    if item["_search_key"] == match:
                        scored.append((score, item))
                        break

    scored.sort(key=lambda x: (-x[0], x[1]["label"]))
    return scored[:quota]


def _fmt(item: dict) -> dict:
    return {
        "label":   item["label"],
        "type":    item["type"],
        "subtype": item["subtype"],
        "meta":    item["meta"],
    }


async def _record_search(
    db: AsyncSession,
    query: str,
    entity_type: str,
    results_returned: int,
) -> int | None:
    try:
        event = SearchEvent(
            query=query[:500],
            entity_type=entity_type,
            results_returned=results_returned,
        )
        db.add(event)
        await db.commit()
        await db.refresh(event)
        return event.id
    except Exception:
        await db.rollback()
        return None


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ClickPayload(BaseModel):
    search_event_id: Optional[int] = None
    query: str
    result_label: str
    result_type: str
    result_subtype: Optional[str] = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/autocomplete", summary="Universal search — facilities, medicines, diseases")
async def autocomplete(
    q: str = Query(..., min_length=1, description="Partial search string"),
    limit: int = Query(10, ge=1, le=20, description="Max suggestions to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    Mixed, relevance-ranked suggestions across all entity types.
    Per-type quota: 4 facilities · 4 medicines · 3 diseases before final merge.
    Records a search_event row on every call.
    """
    t0 = time.monotonic()
    q_lower = q.strip().lower()
    buckets = await _build_index(db)

    all_scored: list[tuple[int, dict]] = []
    for entity_type, candidates in buckets.items():
        all_scored.extend(_score(candidates, q_lower, _TYPE_QUOTA.get(entity_type, 3)))

    all_scored.sort(key=lambda x: (-x[0], x[1]["label"]))
    results = [_fmt(item) for _, item in all_scored[:limit]]
    event_id = await _record_search(db, q, "all", len(results))

    return {
        "meta": {
            "timestamp":      _now_iso(),
            "query":          q,
            "total":          len(results),
            "search_event_id": event_id,
            "response_ms":    round((time.monotonic() - t0) * 1000, 2),
        },
        "data": results,
    }


@router.get("/facilities", summary="Search facilities only")
async def search_facilities(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
    facility_type: Optional[str] = Query(
        None, description="Hospital | Clinic | Health Post | Pharmacy | etc."
    ),
    db: AsyncSession = Depends(get_db),
):
    q_lower = q.strip().lower()
    candidates = (await _build_index(db))["facility"]

    if facility_type:
        candidates = [
            c for c in candidates
            if c["subtype"].lower() == facility_type.lower()
        ]

    results = [_fmt(i) for _, i in _score(candidates, q_lower, limit)]
    event_id = await _record_search(db, q, "facility", len(results))

    return {
        "meta": {
            "timestamp": _now_iso(), "query": q,
            "total": len(results), "search_event_id": event_id,
        },
        "data": results,
    }


@router.get("/medicines", summary="Search medicines only")
async def search_medicines(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
    atc_code: Optional[str] = Query(
        None, description="Filter by ATC prefix e.g. 'J01'"
    ),
    db: AsyncSession = Depends(get_db),
):
    q_lower = q.strip().lower()
    candidates = (await _build_index(db))["medicine"]

    if atc_code:
        candidates = [
            c for c in candidates
            if str(c["meta"].get("atc_code", "")).upper().startswith(atc_code.upper())
        ]

    results = [_fmt(i) for _, i in _score(candidates, q_lower, limit)]
    event_id = await _record_search(db, q, "medicine", len(results))

    return {
        "meta": {
            "timestamp": _now_iso(), "query": q,
            "total": len(results), "search_event_id": event_id,
        },
        "data": results,
    }


@router.get("/diseases", summary="Search ICD-10 diseases only")
async def search_diseases(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
):
    q_lower = q.strip().lower()
    candidates = (await _build_index(db))["disease"]
    results = [_fmt(i) for _, i in _score(candidates, q_lower, limit)]
    event_id = await _record_search(db, q, "disease", len(results))

    return {
        "meta": {
            "timestamp": _now_iso(), "query": q,
            "total": len(results), "search_event_id": event_id,
        },
        "data": results,
    }


@router.post("/click", summary="Record a result selection from the dropdown")
async def record_click(
    payload: ClickPayload,
    db: AsyncSession = Depends(get_db),
):
    """
    Call when the user selects a suggestion from the dropdown.
    Pass search_event_id from the autocomplete response to link
    the click back to its originating search query.
    """
    try:
        click = SearchClick(
            search_event_id=payload.search_event_id,
            query=payload.query[:500],
            result_label=payload.result_label[:500],
            result_type=payload.result_type,
            result_subtype=payload.result_subtype,
        )
        db.add(click)
        await db.commit()
        await db.refresh(click)
        return {
            "meta": {"timestamp": _now_iso()},
            "data": {"click_id": click.id, "recorded": True},
        }
    except Exception as exc:
        await db.rollback()
        return {
            "meta": {"timestamp": _now_iso()},
            "data": {"recorded": False, "error": str(exc)},
        }


@router.post("/refresh-index", summary="Rebuild in-memory search index from DB")
async def refresh_index(db: AsyncSession = Depends(get_db)):
    """
    Forces a full rebuild of the search index from PostgreSQL.
    Call after bulk-importing new facilities, medicines, or ICD codes
    without restarting the server.
    """
    global _search_index
    _search_index = None
    index = await _build_index(db)
    return {
        "meta": {"timestamp": _now_iso()},
        "data": {
            "refreshed": True,
            "counts": {k: len(v) for k, v in index.items()},
        },
    }
