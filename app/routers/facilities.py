"""
CMS — Facilities Map Router
============================
Endpoints that power the clinic/facility map view.

Provides:
  GET /api/v1/cms/facilities          – paginated facility list with coords + district
  GET /api/v1/cms/facilities/map      – GeoJSON FeatureCollection (map-ready)
  GET /api/v1/cms/facilities/{name}   – single facility detail
  GET /api/v1/cms/districts           – district polygons + facility counts (map layer)
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/v1/cms", tags=["CMS – Facilities Map"])

# ---------------------------------------------------------------------------
# HASC code → human-readable district name
# ---------------------------------------------------------------------------
DISTRICT_NAMES: dict[str, str] = {
    "BW.CE": "Central",
    "BW.CH": "Chobe",
    "BW.FR": "Francistown",
    "BW.GB": "Gaborone",
    "BW.GH": "Ghanzi",
    "BW.JW": "Jwaneng",
    "BW.KG": "Kgalagadi",
    "BW.KL": "Kgatleng",
    "BW.KW": "Kweneng",
    "BW.LB": "Lobatse",
    "BW.NC": "North West (Ngamiland)",
    "BW.NE": "North East",
    "BW.SO": "Southern",
    "BW.SP": "Selebi-Phikwe",
    "BW.SR": "South East",
    "BW.ST": "Sowa Town",
}

# ---------------------------------------------------------------------------
# Facility type inference from name keywords
# ---------------------------------------------------------------------------
def _infer_type(name: str) -> str:
    n = name.lower()
    if any(k in n for k in ["referral hospital", "academic hospital", "general hospital"]):
        return "Referral Hospital"
    if "hospital" in n:
        return "Hospital"
    if "clinic" in n:
        return "Clinic"
    if "health post" in n:
        return "Health Post"
    if "pharmacy" in n:
        return "Pharmacy"
    if "dental" in n or "dentist" in n:
        return "Dental Clinic"
    if "optical" in n or "optometrist" in n or "eye" in n or "vision" in n:
        return "Eye Care"
    if "physio" in n:
        return "Physiotherapy"
    if "laboratory" in n or "lab" in n or "diagnostics" in n or "pathology" in n:
        return "Laboratory"
    if "medical centre" in n or "medical center" in n or "medicare" in n:
        return "Medical Centre"
    return "Other"


# ---------------------------------------------------------------------------
# Assign a facility to a district using nearest district centroid
# ---------------------------------------------------------------------------
def _assign_district(lat: float, lon: float, centroids: pd.DataFrame) -> tuple[str, str]:
    """Return (hasc_id, district_name) for the nearest district centroid."""
    dists = (
        (centroids["centroid_lat"] - lat) ** 2
        + (centroids["centroid_lon"] - lon) ** 2
    )
    idx = dists.idxmin()
    hasc = centroids.loc[idx, "hasc_id"]
    return hasc, DISTRICT_NAMES.get(hasc, hasc)


# ---------------------------------------------------------------------------
# Data loader  — call once at startup and cache results
# ---------------------------------------------------------------------------
_cache: dict = {}


def _load_data() -> dict:
    if _cache:
        return _cache

    # ── Facilities (from the clinic lat/long CSV) ──────────────────────────
    facilities_df = pd.read_csv("data/clinics_lat_long.csv")
    facilities_df.columns = facilities_df.columns.str.strip().str.lower()
    facilities_df = facilities_df.rename(columns={"name": "facility_name"})
    facilities_df["longitude"] = pd.to_numeric(facilities_df["longitude"], errors="coerce")
    facilities_df["latitude"] = pd.to_numeric(facilities_df["latitude"], errors="coerce")

    # Deduplicate (keep first occurrence per name)
    facilities_df = facilities_df.drop_duplicates(subset=["facility_name"])

    # ── District geometry ──────────────────────────────────────────────────
    geo_df = pd.read_csv("data/botswana_district_geometry_boundaries.csv")
    geo_df.columns = geo_df.columns.str.strip().str.lower()

    # Compute centroid per district
    centroids = (
        geo_df.groupby("hasc_id")
        .agg(centroid_lat=("latitude", "mean"), centroid_lon=("longitude", "mean"))
        .reset_index()
    )

    # ── Enrich facilities ──────────────────────────────────────────────────
    has_coords = facilities_df["latitude"].notna() & facilities_df["longitude"].notna()

    # Validate coords are within Botswana bounding box (rough sanity check)
    valid_bbox = (
        facilities_df["latitude"].between(-27.0, -17.5)
        & facilities_df["longitude"].between(19.5, 29.5)
    )
    mappable = has_coords & valid_bbox

    facilities_df["has_coordinates"] = mappable

    # Infer facility type
    facilities_df["facility_type"] = facilities_df["facility_name"].apply(_infer_type)

    # Assign district for mappable facilities
    facilities_df["hasc_id"] = None
    facilities_df["district_name"] = None

    for idx, row in facilities_df[mappable].iterrows():
        hasc, name = _assign_district(row["latitude"], row["longitude"], centroids)
        facilities_df.at[idx, "hasc_id"] = hasc
        facilities_df.at[idx, "district_name"] = name

    # ── District polygon groups ────────────────────────────────────────────
    district_polygons: dict[str, list] = {}
    for hasc_id, grp in geo_df.groupby("hasc_id"):
        # Build nested polygon rings (polygon_idx → ring_idx → points)
        rings: dict[int, dict[int, list]] = {}
        for _, r in grp.iterrows():
            pi, ri = int(r["polygon_idx"]), int(r["ring_idx"])
            rings.setdefault(pi, {}).setdefault(ri, [])
            rings[pi][ri].append([r["longitude"], r["latitude"]])
        polygons = [
            [rings[pi][ri] for ri in sorted(rings[pi])]
            for pi in sorted(rings)
        ]
        district_polygons[hasc_id] = polygons

    _cache["facilities"] = facilities_df
    _cache["centroids"] = centroids
    _cache["district_polygons"] = district_polygons
    return _cache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _meta(total: int, page: int, page_size: int) -> dict:
    return {
        "timestamp": _now_iso(),
        "total_records": total,
        "page": page,
        "page_size": page_size,
        "total_pages": math.ceil(total / page_size) if page_size else 1,
    }


def _facility_to_dict(row: pd.Series) -> dict:
    return {
        "facility_name": row["facility_name"],
        "facility_type": row.get("facility_type"),
        "latitude": row["latitude"] if pd.notna(row.get("latitude")) else None,
        "longitude": row["longitude"] if pd.notna(row.get("longitude")) else None,
        "has_coordinates": bool(row.get("has_coordinates", False)),
        "hasc_id": row.get("hasc_id"),
        "district_name": row.get("district_name"),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/facilities", summary="List all facilities with location & district")
def list_facilities(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    district: Optional[str] = Query(None, description="Filter by district name or HASC id, e.g. 'Gaborone' or 'BW.GB'"),
    facility_type: Optional[str] = Query(None, description="Filter by type: Hospital | Clinic | Health Post | Pharmacy | etc."),
    has_coordinates: Optional[bool] = Query(None, description="True = mappable only, False = missing coords only"),
    search: Optional[str] = Query(None, description="Search by facility name"),
):
    """
    Paginated list of all health facilities.
    Each record includes coordinates, facility type, and the district it belongs to
    (district assigned via nearest district centroid for mappable facilities).
    """
    data = _load_data()
    df = data["facilities"].copy()

    # Filters
    if search:
        df = df[df["facility_name"].str.contains(search, case=False, na=False)]
    if district:
        df = df[
            df["district_name"].str.contains(district, case=False, na=False)
            | df["hasc_id"].str.contains(district, case=False, na=False)
        ]
    if facility_type:
        df = df[df["facility_type"].str.lower() == facility_type.lower()]
    if has_coordinates is not None:
        df = df[df["has_coordinates"] == has_coordinates]

    total = len(df)
    start = (page - 1) * page_size
    paged = df.iloc[start: start + page_size]

    return {
        "meta": _meta(total, page, page_size),
        "data": [_facility_to_dict(row) for _, row in paged.iterrows()],
    }


@router.get("/facilities/map", summary="GeoJSON FeatureCollection of all mappable facilities")
def facilities_geojson(
    district: Optional[str] = Query(None, description="Filter by district name or HASC id"),
    facility_type: Optional[str] = Query(None, description="Filter by facility type"),
):
    """
    Returns a GeoJSON FeatureCollection of all facilities that have valid coordinates.
    Feed directly to Mapbox / Leaflet / MapLibre as a data source.

    Each Feature carries:
      properties.facility_name, facility_type, district_name, hasc_id
    """
    data = _load_data()
    df = data["facilities"][data["facilities"]["has_coordinates"]].copy()

    if district:
        df = df[
            df["district_name"].str.contains(district, case=False, na=False)
            | df["hasc_id"].str.contains(district, case=False, na=False)
        ]
    if facility_type:
        df = df[df["facility_type"].str.lower() == facility_type.lower()]

    features = []
    for _, row in df.iterrows():
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]],
            },
            "properties": {
                "facility_name": row["facility_name"],
                "facility_type": row["facility_type"],
                "district_name": row.get("district_name"),
                "hasc_id": row.get("hasc_id"),
            },
        })

    return {
        "type": "FeatureCollection",
        "meta": {
            "timestamp": _now_iso(),
            "total_features": len(features),
        },
        "features": features,
    }


@router.get("/facilities/{facility_name}", summary="Single facility detail")
def get_facility(facility_name: str):
    """
    Returns detail for a single facility by exact name.
    URL-encode spaces as %20 or use + signs.
    """
    data = _load_data()
    df = data["facilities"]
    match = df[df["facility_name"].str.lower() == facility_name.lower()]

    if match.empty:
        raise HTTPException(status_code=404, detail=f"Facility '{facility_name}' not found.")

    row = match.iloc[0]
    return {
        "meta": {"timestamp": _now_iso()},
        "data": _facility_to_dict(row),
    }


@router.get("/districts", summary="District polygons with facility counts (map layer)")
def get_districts(
    hasc_id: Optional[str] = Query(None, description="Return a single district, e.g. BW.GB"),
):
    """
    Returns district boundary polygons in GeoJSON MultiPolygon format,
    enriched with:
      - district_name
      - centroid (lat/lon)
      - facility_count  (number of mapped facilities in district)
      - facilities_by_type breakdown

    Feed the GeoJSON features directly to your map choropleth / district layer.
    """
    data = _load_data()
    facilities = data["facilities"]
    district_polygons = data["district_polygons"]
    centroids = data["centroids"].set_index("hasc_id")

    # Facility counts per district
    mapped = facilities[facilities["has_coordinates"] & facilities["hasc_id"].notna()]
    counts = mapped.groupby("hasc_id").size().to_dict()
    type_breakdown = (
        mapped.groupby(["hasc_id", "facility_type"])
        .size()
        .reset_index(name="count")
    )

    features = []
    target_ids = [hasc_id.upper()] if hasc_id else list(district_polygons.keys())

    for hid in target_ids:
        if hid not in district_polygons:
            raise HTTPException(status_code=404, detail=f"District '{hid}' not found.")

        centroid_row = centroids.loc[hid] if hid in centroids.index else None
        type_counts = (
            type_breakdown[type_breakdown["hasc_id"] == hid]
            .set_index("facility_type")["count"]
            .to_dict()
        )

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": district_polygons[hid],
            },
            "properties": {
                "hasc_id": hid,
                "district_name": DISTRICT_NAMES.get(hid, hid),
                "centroid": {
                    "latitude": round(centroid_row["centroid_lat"], 6) if centroid_row is not None else None,
                    "longitude": round(centroid_row["centroid_lon"], 6) if centroid_row is not None else None,
                },
                "facility_count": counts.get(hid, 0),
                "facilities_by_type": type_counts,
            },
        })

    return {
        "type": "FeatureCollection",
        "meta": {
            "timestamp": _now_iso(),
            "total_districts": len(features),
        },
        "features": features,
    }