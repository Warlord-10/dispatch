"""Run: uv run python tests/test_registration.py  (needs docker compose up -d)"""

import pathlib
import sys

# tests live in tests/, the app lives one level up
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import asyncio
import uuid

from pydantic import ValidationError

from db import Session, init_db
from schemas.registration import DriverRegister, RiderRegister
from services.registration import AlreadyRegistered, register_driver, register_rider


async def main():
    await init_db()

    # unique per run: these rows persist, so fixed numbers only
    # pass against a fresh database
    u = uuid.uuid4().int % 10**8
    phone_a, phone_b = f"9{u:08d}1", f"9{u:08d}2"

    # phone normalization collapses to one canonical form
    assert RiderRegister(name="A", phone_number="+91 98765-43210").phone_number == "9876543210"
    assert RiderRegister(name="A", phone_number="09876543210").phone_number == "9876543210"
    for bad in ("12345", "1234567890", "abcdefghij"):
        try:
            RiderRegister(name="A", phone_number=bad)
            raise AssertionError(f"{bad} should be rejected")
        except ValidationError:
            pass

    async with Session() as s:
        rider = await register_rider(s, RiderRegister(name="Rhea", phone_number=phone_a))
        assert rider.id and rider.phone_number == phone_a

    async with Session() as s:
        driver = await register_driver(
            s,
            DriverRegister(
                name="Sam", phone_number=f"+91 {phone_b}",
                car_number="dl-01 ab-1234", car_model="Swift",
            ),
        )
        assert driver.status == "offline"
        assert driver.car_number == "DL01AB1234", driver.car_number

    # same phone, different formatting -> must be rejected by the unique index
    async with Session() as s:
        try:
            await register_rider(s, RiderRegister(name="Dup", phone_number=f"+91{phone_a}"))
            raise AssertionError("duplicate phone should have been rejected")
        except AlreadyRegistered:
            pass

    print("registration ok")


if __name__ == "__main__":
    asyncio.run(main())
