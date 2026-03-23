"""
CMS — Disease Surveillance Models
===================================
Additional SQLAlchemy models for disease surveillance data.
Add these to your existing database.py Base metadata.

Tables:
  - dispensing_records     : medicine dispensing events per facility
  - surveillance_alerts    : auto-generated spike alerts
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import (
    Date, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column

from ...database import Base


class DispensingRecord(Base):
    """
    One row per medicine dispensing event at a facility.
    No patient identifiers — only anonymised aggregate-safe fields.

    Populated by your dispensing system / pharmacy integration.
    """
    __tablename__ = "dispensing_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # When & where
    dispensed_at: Mapped[date] = mapped_column(
        Date, nullable=False, index=True,
        comment="Date the medicine was dispensed"
    )
    facility_name: Mapped[str] = mapped_column(
        String(500), nullable=False, index=True
    )
    hasc_id: Mapped[str | None] = mapped_column(
        String(20), nullable=True, index=True,
        comment="District HASC code e.g. BW.GB"
    )

    # What was dispensed
    nappi_code: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    product_name: Mapped[str] = mapped_column(String(500), nullable=False)
    atc_code: Mapped[str | None] = mapped_column(
        String(20), nullable=True, index=True,
        comment="WHO ATC code — links medicine to disease category"
    )
    atc_description: Mapped[str | None] = mapped_column(String(200), nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # Disease linkage (recorded at point of dispensing if available)
    icd_code: Mapped[str | None] = mapped_column(
        String(20), nullable=True, index=True,
        comment="ICD-10 code linked to this dispensing event"
    )
    icd_description: Mapped[str | None] = mapped_column(String(300), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class SurveillanceAlert(Base):
    """
    Auto-generated alert when a spike is detected in search or dispensing volume
    for a disease or medicine within a district/period.
    """
    __tablename__ = "surveillance_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    alert_type: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True,
        comment="search_spike | dispensing_spike"
    )
    signal: Mapped[str] = mapped_column(
        String(500), nullable=False,
        comment="The ICD code, disease name, or medicine that triggered the alert"
    )
    hasc_id: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    district_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    observed_count: Mapped[int] = mapped_column(
        Integer, nullable=False,
        comment="Actual count in the alert period"
    )
    baseline_count: Mapped[float] = mapped_column(
        Float, nullable=False,
        comment="Rolling average baseline count"
    )
    z_score: Mapped[float] = mapped_column(
        Float, nullable=False,
        comment="Standard deviations above baseline"
    )
    severity: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True,
        comment="low | medium | high — based on z_score thresholds"
    )
    is_active: Mapped[bool] = mapped_column(
        Integer, nullable=False, default=True,
        comment="False once alert is resolved/acknowledged"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )