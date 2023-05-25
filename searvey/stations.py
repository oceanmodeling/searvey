from __future__ import annotations

import datetime
from enum import Enum

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from . import coops
from . import ioc
from . import usgs


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
    coops_gdf = coops.coops_stations_within_region(region=region)
    coops_gdf = coops_gdf.assign(
        provider=Provider.COOPS.value,
        provider_id=coops_gdf.index,
        country=np.where(coops_gdf.state.str.len() > 0, "USA", None),  # type: ignore[call-overload]
        location=coops_gdf[["name", "state"]].agg(", ".join, axis=1),
        lon=coops_gdf.geometry.x,
        lat=coops_gdf.geometry.y,
        is_active=coops_gdf.status == "active",
        start_date=pd.NaT,
        last_observation=coops_gdf[coops_gdf.status == "discontinued"]
        .removed.str[:18]
        .apply(pd.to_datetime)
        .dt.tz_localize("UTC"),
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
    df = pd.concat(dataframes).reset_index(drop=True)
    return df
