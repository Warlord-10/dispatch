import re

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _normalize_phone(v: str) -> str:
    """Store one canonical form so '+91 98765-43210' and '9876543210' collide."""
    digits = re.sub(r"\D", "", v)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0] not in "6789":
        raise ValueError("must be a 10-digit Indian mobile number")
    return digits


# Rider
class RiderRegister(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    phone_number: str

    _norm_phone = field_validator("phone_number")(_normalize_phone)

class RiderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    phone_number: str


# Driver
class DriverRegister(RiderRegister):
    car_number: str = Field(min_length=4, max_length=20)
    car_model: str = Field(min_length=1, max_length=20)

    @field_validator("car_number")
    @classmethod
    def normalize_plate(cls, v: str) -> str:
        return re.sub(r"[\s-]", "", v).upper()

class DriverOut(RiderOut):
    status: str
    car_number: str
    car_model: str
