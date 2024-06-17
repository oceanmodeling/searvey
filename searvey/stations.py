from __future__ import annotations

import datetime
from enum import Enum

import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from . import coops
from . import ioc
from . import usgs
from . import _ndbc_api

STATIONS_COLUMNS: list[str] = [
    "provider",
    "provider_id",
    "country",
    "location",
    "lon",
    "lat",
    "is_active",
    "start_date",
    "last_observation",
    "geometry",
]


class Provider(str, Enum):
    """
    An enumeration for ``searvey`` providers.
    """

    ALL: str = "ALL"
    COOPS: str = "COOPS"
    IOC: str = "IOC"
    USGS: str = "USGS"
    NDBC: str = "NDBC"


def _get_ioc_stations(
    activity_threshold: datetime.timedelta,
    region: Polygon | MultiPolygon | None = None,
) -> gpd.GeoDataFrame:
    # Convert activity threshold to a Timezone Aware Datetime object
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    activity_threshold_ts = now_utc - activity_threshold

    # Get full metadata
    ioc_gdf = ioc.get_ioc_stations(region=region)

    # Normalize IOC
    # Drop delay `NA'` : https://github.com/oceanmodeling/searvey/issues/91
    ioc_gdf = ioc_gdf[ioc_gdf.delay != "NA'"]
    ioc_gdf = ioc_gdf[~ioc_gdf.delay.isna()]

    # Convert delay to minutes
    ioc_gdf = ioc_gdf.assign(
        delay=pd.concat(
            (
                ioc_gdf.delay[ioc_gdf.delay.str.endswith("'")].str[:-1].astype(int),
                ioc_gdf.delay[ioc_gdf.delay.str.endswith("h")].str[:-1].astype(int) * 60,
                ioc_gdf.delay[ioc_gdf.delay.str.endswith("d")].str[:-1].astype(int) * 24 * 60,
            )
        )
    )

    # Some IOC stations appear to have negative delay due to server
    # [time drift](https://www.bluematador.com/docs/troubleshooting/time-drift-ntp)
    # or for other Provider-specific reasons. IOC suggests to ignore the negative
    # delay and consider the stations as active.
    # https://github.com/oceanmodeling/searvey/issues/40#issuecomment-1219509512
    ioc_gdf.loc[(ioc_gdf.delay < 0), "delay"] = 0

    # Calculate the timestamp of the last observation
    ioc_gdf = ioc_gdf.assign(
        last_observation=now_utc - ioc_gdf.delay.apply(lambda x: datetime.timedelta(minutes=x))
    )

    ioc_gdf = ioc_gdf.assign(
        provider=Provider.IOC.value,
        provider_id=ioc_gdf.ioc_code,
        start_date=ioc_gdf.added_to_system.apply(pd.to_datetime).dt.tz_localize("UTC"),
        is_active=ioc_gdf.last_observation > activity_threshold_ts,
    )

    # Filter out columns
    ioc_gdf = ioc_gdf[STATIONS_COLUMNS]

    return ioc_gdf


def _get_coops_stations(
    region: Polygon | MultiPolygon | None = None,
) -> gpd.GeoDataFrame:
    coops_gdf = coops.get_coops_stations(region=region, metadata_source="main")
    # TODO: We don't have station type info in station API
    #    coops_gdf = coops_gdf.set_index("station_type", append=True)
    coops_gdf = coops_gdf[~coops_gdf.index.duplicated()]
    coops_gdf = coops_gdf.assign(
        provider=Provider.COOPS.value,
        provider_id=coops_gdf.index.get_level_values("nos_id"),
        country=coops_gdf.state.where(coops_gdf.state.str.len() != 2, "USA"),
        location=coops_gdf[["name", "state"]].fillna("").agg(", ".join, axis=1).str.strip(", "),
        lon=coops_gdf.geometry.x,
        lat=coops_gdf.geometry.y,
        is_active=coops_gdf.status == "active",
        start_date=pd.NaT,
        last_observation=coops_gdf[coops_gdf.status == "discontinued"].removed.dt.tz_localize("UTC"),
    )[STATIONS_COLUMNS]
    return coops_gdf


def _get_usgs_stations(
    activity_threshold: datetime.timedelta,
    region: Polygon | MultiPolygon | None = None,
) -> gpd.GeoDataFrame:
    # Convert activity threshold to a Timezone Aware Datetime object
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    activity_threshold_ts = now_utc - activity_threshold

    # Get full metadata
    usgs_gdf = usgs.get_usgs_stations(region=region)

    # Normalize USGS
    # Calculate the timestamp of the last observation
    usgs_gdf = usgs_gdf.assign(
        last_observation=usgs_gdf.end_date.dt.tz_localize("UTC"),
    )

    usgs_gdf = usgs_gdf.assign(
        country="USA",
        location=usgs_gdf.station_nm,
        lon=usgs_gdf.dec_long_va,
        lat=usgs_gdf.dec_lat_va,
        provider=Provider.USGS.value,
        provider_id=usgs_gdf.site_no,
        start_date=usgs_gdf.begin_date.dt.tz_localize("UTC"),
        is_active=usgs_gdf.last_observation > activity_threshold_ts,
    )

    # Filter out columns
    usgs_gdf = usgs_gdf[STATIONS_COLUMNS]

    return usgs_gdf

def _get_ndbc_stations(
    region: Polygon | MultiPolygon | None = None,
) -> gpd.GeoDataFrame:
    ndbc_gdf = _ndbc_api.get_ndbc_stations(region=region)

    ndbc_gdf = ndbc_gdf.assign(
        provider=Provider.NDBC.value,
        provider_id=ndbc_gdf.station_id,
        country="USA",
        location=pd.NaT,
        lon=ndbc_gdf.lon,
        lat=ndbc_gdf.lat,
        is_active=True,#assuming all NDBC stations are active
        start_date=pd.NaT,
        last_observation=pd.NaT,
    )[STATIONS_COLUMNS]
    
    return ndbc_gdf


def get_stations(
    activity_threshold: datetime.timedelta = datetime.timedelta(days=3),
    providers: list[Provider] = Provider.ALL,
    region: Polygon | MultiPolygon | None = None,
) -> gpd.GeoDataFrame:
    """
    Return a ``geopandas.GeoDataFrame`` with metadata from ``providers``.
    """
    dataframes = []
    if Provider.ALL in providers or Provider.IOC in providers:
        dataframes.append(_get_ioc_stations(activity_threshold=activity_threshold, region=region))
    if Provider.ALL in providers or Provider.COOPS in providers:
        dataframes.append(_get_coops_stations(region=region))
    if Provider.ALL in providers or Provider.USGS in providers:
        dataframes.append(_get_usgs_stations(activity_threshold=activity_threshold, region=region))
    if Provider.ALL in providers or Provider.NDBC in providers:
        dataframes.append(_get_ndbc_stations(region=region))
    df = pd.concat(dataframes).reset_index(drop=True)
    return df
