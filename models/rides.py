import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Index, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from db import Base
from enums import RideStatus, sql_values


class Ride(Base):
    __tablename__ = "rides"
    __table_args__ = (
        CheckConstraint(
            f"status in {sql_values(RideStatus)}", name="rides_status_valid"
        ),
        Index(
            "rides_pending_idx",
            "status",
            postgresql_where=f"status = '{RideStatus.PENDING}'",
        ),
        # Scoped per rider: a global key would let one rider replay another's
        # key and read back their ride. NULLs are distinct in Postgres, so
        # requests without a key never collide.
        UniqueConstraint("rider_id", "idempotency_key", name="rides_idempotency_key"),
        # The hard invariant, held by storage rather than by code: one driver,
        # at most one assigned ride. The accept compare-and-set already
        # guarantees it; this catches the next bug that doesn't.
        Index(
            "rides_one_assigned_per_driver",
            "driver_id",
            unique=True,
            postgresql_where=f"status = '{RideStatus.ASSIGNED}'",
        ),
    )

    # UUID because ride ids go to clients and get sharded in Phase 9.
    # Changing a primary key type later is the one migration worth avoiding.
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # NULL = the client sent no Idempotency-Key. Never generated server-side:
    # a key the client does not hold is a key it can never retry with.
    idempotency_key: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    request_hash: Mapped[str | None] = mapped_column(String(64))

    rider_id: Mapped[int] = mapped_column(ForeignKey("riders.id"), index=True)
    driver_id: Mapped[int | None] = mapped_column(ForeignKey("drivers.id"), index=True)

    status: Mapped[str] = mapped_column(String(16), default=RideStatus.PENDING)

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
