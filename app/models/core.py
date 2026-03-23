"""
CMS — Core ORM Models
======================
Defines all tables the application reads from and writes to.

Tables
------
  facilities        health facilities with coordinates + district
  districts         district boundary polygons + centroids
  medicines         NAPPI essential medicines list
  icd10_codes       ICD-10 disease classification (billable codes)

Surveillance models (dispensing_records, surveillance_alerts)
live in models/surveillance.py and import Base from here via database.py.
"""

from __future__ import annotations

from sqlalchemy import (
    Boolean, Date, DateTime, Float,
    Integer, String, Text
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..database import Base


# ---------------------------------------------------------------------------
# Facilities
# ---------------------------------------------------------------------------

class Facility(Base):
    """
    One row per health facility.
    Seeded from the clinics lat/long CSV + district geometry assignment.
    """
    __tablename__ = "facilities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    facility_name: Mapped[str] = mapped_column(
        String(500), nullable=False, index=True, unique=True
    )
    facility_type: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True,
        comment="Hospital | Clinic | Health Post | Pharmacy | Dental Clinic | etc."
    )

    # Coordinates
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    has_coordinates: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, index=True
    )

    # District linkage
    hasc_id: Mapped[str | None] = mapped_column(
        String(20), nullable=True, index=True,
        comment="e.g. BW.GB — assigned via nearest district centroid"
    )
    district_name: Mapped[str | None] = mapped_column(String(100), nullable=True)


# ---------------------------------------------------------------------------
# Districts
# ---------------------------------------------------------------------------

class District(Base):
    """
    One row per Botswana district.
    Boundary polygon stored as JSONB (MultiPolygon coordinate array).
    """
    __tablename__ = "districts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    hasc_id: Mapped[str] = mapped_column(
        String(20), nullable=False, unique=True, index=True,
        comment="e.g. BW.GB"
    )
    district_name: Mapped[str] = mapped_column(String(100), nullable=False)

    # Centroid (computed from boundary points)
    centroid_lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    centroid_lon: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Full MultiPolygon coordinate array stored as JSONB
    # Shape: [ [ [ [lon, lat], ... ] ] ]  (MultiPolygon → Polygon → Ring → Point)
    coordinates: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True,
        comment="GeoJSON MultiPolygon coordinates array"
    )


# ---------------------------------------------------------------------------
# Medicines (NAPPI essential medicines list)
# ---------------------------------------------------------------------------

class Medicine(Base):
    """
    One row per NAPPI product.
    Seeded from the essential_medicine_nappi_list CSV.
    """
    __tablename__ = "medicines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    nappi_code: Mapped[str | None] = mapped_column(
        String(50), nullable=True, index=True, unique=True
    )
    product_name: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    dosage_form: Mapped[str | None] = mapped_column(String(20), nullable=True)
    schedule: Mapped[str | None] = mapped_column(String(50), nullable=True)
    nappi_status: Mapped[str | None] = mapped_column(
        String(50), nullable=True, index=True,
        comment="Active | Discontinued"
    )
    supplier_name: Mapped[str | None] = mapped_column(String(300), nullable=True)

    # ATC classification
    who_atc_code: Mapped[str | None] = mapped_column(
        String(20), nullable=True, index=True
    )
    who_atc_description: Mapped[str | None] = mapped_column(String(300), nullable=True)

    # EML flags
    active_on_primary_healthcare_eml: Mapped[str | None] = mapped_column(String(10))
    active_on_adult_hospital_eml: Mapped[str | None] = mapped_column(String(10))
    active_on_paediatric_hospital_eml: Mapped[str | None] = mapped_column(String(10))
    active_on_tertiary_quaternary_eml: Mapped[str | None] = mapped_column(String(10))
    active_on_master_procurement_catalogue: Mapped[str | None] = mapped_column(String(10))


# ---------------------------------------------------------------------------
# ICD-10 disease classification
# ---------------------------------------------------------------------------

class ICD10Code(Base):
    """
    One row per ICD-10 code entry.
    Seeded from the idc_10_disease_classification CSV.
    is_valid=True rows are the leaf/billable codes used in searches.
    """
    __tablename__ = "icd10_codes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    order_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    code: Mapped[str] = mapped_column(
        String(20), nullable=False, unique=True, index=True
    )
    is_valid: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, index=True,
        comment="True = billable/leaf code"
    )
    short_description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    long_description: Mapped[str | None] = mapped_column(Text, nullable=True)
