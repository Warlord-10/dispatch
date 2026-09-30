from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column

from db import Base
from enums import DriverStatus, sql_values


class Driver(Base):
    __tablename__ = "drivers"
    __table_args__ = (
        # Generated from the enum so the constraint cannot drift from the code.
        CheckConstraint(
            f"status in {sql_values(DriverStatus)}", name="drivers_status_valid"
        ),
        Index(
            "drivers_available_idx",
            "status",
            postgresql_where=f"status = '{DriverStatus.AVAILABLE}'",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(80))
    phone_number: Mapped[str] = mapped_column(String(20), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default=DriverStatus.OFFLINE)

    # car details
    car_number: Mapped[str] = mapped_column(String(20))
    car_model: Mapped[str] = mapped_column(String(20))

    # Location lives in Redis only — see services/geo_index.py
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
