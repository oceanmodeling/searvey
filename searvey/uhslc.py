"""
interface with the University of Hawai'i at Manoa School of Ocean and Earth Science and Technology (SOEST) Sea Level Center (UHSLC) API
https://uhslc.soest.hawaii.edu/erddap
"""
import datetime
from typing import Optional

import pandas as pd
import pydantic

from searvey.erddap import query_erddap
from searvey.models import AsymmetricBBox
from searvey.models import AsymmetricConstraints
from searvey.models import ERDDAPDataset
from searvey.utils import lon3_to_lon1


_UHSLC_NORMALIZED_NAMES = {
    "sea_level (millimeters)": "sea_level",
    "time (UTC)": "time",
    "latitude (degrees_north)": "lat",
    "longitude (degrees_east)": "lon",
    "last_rq_date (UTC)": "last_rq_date",
}

SOEST_UHSLC = ERDDAPDataset(
    server_url=pydantic.parse_obj_as(pydantic.HttpUrl, "https://uhslc.soest.hawaii.edu/erddap"),
    dataset_id="global_hourly_fast",
    is_longitude_symmetric=False,
)


def normalize_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=_UHSLC_NORMALIZED_NAMES)
    return df


def normalize_longitudes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(lon=lon3_to_lon1(df.lon))
    return df


def remove_null_sea_levels(df: pd.DataFrame) -> pd.DataFrame:
    df = df[~df.sea_level.isna()]
    return df


def normalize_timestamps(df: pd.DataFrame, freq: str = "H") -> pd.DataFrame:
    df = df.assign(time=pd.to_datetime(df.time).dt.round(freq=freq))
    return df


def normalize_sea_level(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(sea_level=df.sea_level * 1000)
    return df


def make_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        # gloss_id=df.gloss_id.astype(int),
        station_name=df.station_name.astype("category"),
        station_country=df.station_country.astype("category"),
        station_country_code=df.station_country_code.astype("category"),
        ssc_id=df.ssc_id.astype("category"),
        uhslc_id=df.uhslc_id.astype("category"),
        gloss_id=df.gloss_id.astype("category"),
        record_id=df.record_id.astype("category"),
    )
    return df


def get_uhslc_data(
    start_date: datetime.datetime,
    end_date: Optional[datetime.datetime] = None,
    lat_min: float = -90,
    lat_max: float = 90,
    lon_min: float = 0,
    lon_max: float = 360,
    timeout: int = 10,
) -> pd.DataFrame:
    bbox = AsymmetricBBox(
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
    )
    constraints = AsymmetricConstraints(
        bbox=bbox,
        start_date=start_date,
        end_date=end_date or datetime.datetime.now(),
    )
    df = query_erddap(dataset=SOEST_UHSLC, constraints=constraints, timeout=timeout)
    df = normalize_names(df)
    df = normalize_longitudes(df)
    df = remove_null_sea_levels(df)
    df = normalize_timestamps(df)
    df = normalize_sea_level(df)
    df = make_categories(df)
    return df
