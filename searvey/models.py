import datetime
import enum
import logging
from typing import Final

import pydantic

logger = logging.getLogger(__name__)


class ERDDAPProtocol(str, enum.Enum):
    TABLEDAP: Final = "tabledap"
    GRIDDAP: Final = "griddap"


class BBox(pydantic.BaseModel):
    lon_min: float
    lon_max: float
    lat_min: float = pydantic.Field(default=-90, ge=-90, le=90)
    lat_max: float = pydantic.Field(default=90, ge=-90, le=90)

    class Config:
        extra = "forbid"


class SymmetricBBox(BBox):
    lon_min: float = pydantic.Field(default=-180, ge=-180, le=180)
    lon_max: float = pydantic.Field(default=180, ge=-180, le=180)


class AsymmetricBBox(BBox):
    lon_min: float = pydantic.Field(default=0, ge=0, le=360)
    lon_max: float = pydantic.Field(default=360, ge=0, le=360)


class Constraints(pydantic.BaseModel):
    bbox: BBox = pydantic.Field(...)
    start_date: datetime.datetime = pydantic.Field(...)
    end_date: datetime.datetime = pydantic.Field(default_factory=datetime.datetime.now)


class SymmetricConstraints(Constraints):
    bbox: SymmetricBBox = SymmetricBBox()


class AsymmetricConstraints(Constraints):
    bbox: AsymmetricBBox = AsymmetricBBox()


class ERDDAPDataset(pydantic.BaseModel):
    server_url: pydantic.HttpUrl
    protocol: ERDDAPProtocol = pydantic.Field(default=ERDDAPProtocol.TABLEDAP)
    dataset_id: str
    is_longitude_symmetric: bool
