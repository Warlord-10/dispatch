import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Index, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

STATUSES = ("pending", "assigned", "completed", "cancelled")


class Ride(Base):
    __tablename__ = "rides"
    __table_args__ = (
        CheckConstraint(
            f"status in {STATUSES}", name="rides_status_valid"
        ),
        Index("rides_pending_idx", "status", postgresql_where="status = 'pending'"),
    )

    # UUID because ride ids go to clients and get sharded in Phase 9.
    # Changing a primary key type later is the one migration worth avoiding.
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    rider_id: Mapped[int] = mapped_column(ForeignKey("riders.id"), index=True)
    driver_id: Mapped[int | None] = mapped_column(ForeignKey("drivers.id"), index=True)

    status: Mapped[str] = mapped_column(String(16), default="pending")

    pickup_lng: Mapped[float] = mapped_column(Float)
    pickup_lat: Mapped[float] = mapped_column(Float)
    dest_lng: Mapped[float] = mapped_column(Float)
    dest_lat: Mapped[float] = mapped_column(Float)

    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
