"""
interface with the European Marine Observation and Data Network (EMODnet) API
https://erddap.emodnet-physics.eu/erddap
"""
import datetime
from typing import Optional

import pandas as pd
import pydantic

from searvey.erddap import query_erddap
from searvey.models import ERDDAPDataset
from searvey.models import SymmetricBBox
from searvey.models import SymmetricConstraints

_CRITECH_NORMALIZED_NAMES = {
    "stationID": "station_id",
    "stationName": "station_name",
    "stationSensor": "station_sensor",
    "stationProvider": "station_provider",
    "time (UTC)": "time",
    "latitude (degrees_north)": "lat",
    "longitude (degrees_east)": "lon",
    "SLEV (m)": "sea_level",
    "timestamp (UTC)": "timestamp",
}


EMODNET_CRITECH = ERDDAPDataset(
    server_url=pydantic.parse_obj_as(pydantic.HttpUrl, "https://erddap.emodnet-physics.eu/erddap"),
    dataset_id="TAD_Tsunami_Alert_Device",
    is_longitude_symmetric=True,
)


def normalize_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=_CRITECH_NORMALIZED_NAMES)
    return df


def remove_null_sea_levels(df: pd.DataFrame) -> pd.DataFrame:
    df = df[~df.sea_level.isna()]
    return df


def normalize_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(time=pd.to_datetime(df.time))
    return df


def make_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        station_id=df.station_id.astype("category"),
        station_name=df.station_name.astype("category"),
        station_sensor=df.station_sensor.astype("category"),
        station_provider=df.station_provider.astype("category"),
        author=df.author.astype("category"),
        command=df.command.astype("category"),
    )
    return df


def get_critech_data(
    start_date: datetime.datetime,
    end_date: Optional[datetime.datetime] = None,
    lat_min: float = -90,
    lat_max: float = 90,
    lon_min: float = -180,
    lon_max: float = 180,
    timeout: int = 60,
) -> pd.DataFrame:
    bbox = SymmetricBBox(
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
    )
    constraints = SymmetricConstraints(
        bbox=bbox,
        start_date=start_date,
        end_date=end_date or datetime.datetime.now(),
    )
    df = query_erddap(dataset=EMODNET_CRITECH, constraints=constraints, timeout=timeout)
    df = normalize_names(df)
    df = remove_null_sea_levels(df)
    df = normalize_timestamps(df)
    df = make_categories(df)
    return df
