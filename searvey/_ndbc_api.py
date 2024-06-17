from __future__ import annotations

import functools
import logging
from typing import List
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from searvey._common import _resolve_end_date
from searvey._common import _resolve_start_date
from searvey._common import _to_utc
from searvey.custom_types import DatetimeLike
from searvey.utils import get_region
import multifutures

from ndbc_api import NdbcApi

logger = logging.getLogger(__name__)

# Create an instance of NdbcApi
ndbc_api = NdbcApi()


@functools.lru_cache
def _get_ndbc_stations() -> gpd.GeoDataFrame:
    """
    Return NDBC station metadata.
    :return: ``geopandas.GeoDataFrame`` with the station metadata
    """
    stations_df = ndbc_api.stations()
    stations_df[["lat", "ns", "lon", "ew"]] = stations_df["Location Lat/Long"].str.extract(
        r"(\d+\.\d+)([N|S]) (\d+\.\d+)([E|W])"
    )
    stations_df["lat"] = pd.to_numeric(stations_df["lat"])
    stations_df["lon"] = pd.to_numeric(stations_df["lon"])
    stations_df["lat"] = stations_df["lat"] * np.where(stations_df["ns"] == "S", -1, 1)
    stations_df["lon"] = stations_df["lon"] * np.where(stations_df["ew"] == "W", -1, 1)
    stations_df = stations_df.drop(columns=["Location Lat/Long"])
    
        
    stations_df = gpd.GeoDataFrame(
        data=stations_df,
        geometry=gpd.points_from_xy(stations_df.lon, stations_df.lat, crs="EPSG:4326"),
    )
    return stations_df


def get_ndbc_stations(
    region: Optional[MultiPolygon | Polygon] = None,
    lon_min: Optional[float] = None,
    lon_max: Optional[float] = None,
    lat_min: Optional[float] = None,
    lat_max: Optional[float] = None,
) -> gpd.GeoDataFrame:
    """
    Return NDBC station metadata.
    If `region` is defined then the stations that are outside of the region are
    filtered out. If the coordinates of the Bounding Box are defined then
    stations outside of the BBox are filtered out. If both ``region`` and the
    Bounding Box are defined, then an exception is raised.
    :param region: ``Polygon`` or ``MultiPolygon`` denoting region of interest
    :param lon_min: The minimum Longitude of the Bounding Box.
    :param lon_max: The maximum Longitude of the Bounding Box.
    :param lat_min: The minimum Latitude of the Bounding Box.
    :param lat_max: The maximum Latitude of the Bounding Box.
    :return: ``geopandas.GeoDataFrame`` with the station metadata
    """
    region = get_region(
        region=region,
        lon_min=lon_min,
        lon_max=lon_max,
        lat_min=lat_min,
        lat_max=lat_max,
        symmetric=True,
    )

    ndbc_stations = _get_ndbc_stations()
    if region:
        ndbc_stations = ndbc_stations[ndbc_stations.within(region)]
    return ndbc_stations
