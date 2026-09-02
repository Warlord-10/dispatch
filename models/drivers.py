from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Float, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column

from db import Base


class Driver(Base):
    __tablename__ = "drivers"
    __table_args__ = (
        CheckConstraint(
            "status in ('offline', 'available', 'assigned')", name="drivers_status_valid"
        ),
        # Phase 1 matching scans this. It is meant to stop helping around Phase 2.
        Index("drivers_available_idx", "status", postgresql_where="status = 'available'"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(80))
    phone_number: Mapped[str] = mapped_column(String(20), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="offline")
    
    #car_details
    car_number: Mapped[str] = mapped_column(String(20))
    car_model: Mapped[str] = mapped_column(String(20))

    # location
    lng: Mapped[float | None] = mapped_column(Float)
    lat: Mapped[float | None] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
