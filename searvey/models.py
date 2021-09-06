import datetime
import enum
import logging

import pydantic

from typing import Final

logging.basicConfig(level=10)
logger = logging.getLogger()


class ERDDAPProtocol(str, enum.Enum):
    TABLEDAP: Final = "tabledap"
    GRIDDAP: Final = "griddap"


class Constraints(pydantic.BaseModel):
    lon_min: float
    lon_max: float
    lat_min: float = pydantic.Field(-90, ge=-90, le=90)
    lat_max: float = pydantic.Field(90, ge=-90, le=90)
    start_date: datetime.datetime = pydantic.Field(...)
    end_date: datetime.datetime = pydantic.Field(datetime.datetime.now())


class SymmetricConstraints(Constraints):
    lon_min: float = pydantic.Field(-180, ge=-180, le=180)
    lon_max: float = pydantic.Field(180, ge=-180, le=180)


class AsymmetricConstraints(Constraints):
    lon_min: float = pydantic.Field(0, ge=0, le=360)
    lon_max: float = pydantic.Field(360, ge=0, le=360)


class ERDDAPDataset(pydantic.BaseModel):
    server_url: pydantic.HttpUrl
    protocol: ERDDAPProtocol = pydantic.Field(ERDDAPProtocol.TABLEDAP)
    dataset_id: str
    is_longitude_symmetric: bool
