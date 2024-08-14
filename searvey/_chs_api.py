from __future__ import annotations

import logging
import typing as T
from typing import List, Union
import functools
import geopandas as gpd
import multifutures
import pandas as pd
import httpx
from shapely.geometry import MultiPolygon, Polygon

from searvey._common import _resolve_end_date, _resolve_start_date, _resolve_http_client, _resolve_rate_limit
import json
from searvey._common import _fetch_url
from searvey.custom_types import DatetimeLike
from searvey.utils import get_region

logger = logging.getLogger(__name__)

CHS_BASE_URL = "https://api-iwls.dfo-mpo.gc.ca/api/v1"

@functools.lru_cache
def get_chs_stations(
    region: MultiPolygon | Polygon | None = None,
    lon_min: float | None = None,
    lon_max: float | None = None,
    lat_min: float | None = None,
    lat_max: float | None = None,
) -> gpd.GeoDataFrame:
    """
    Return CHS station metadata.
    If `region` is defined then the stations that are outside of the region are
    filtered out. If the coordinates of the Bounding Box are defined then
    stations outside of the BBox are filtered out. If both `region` and the
    Bounding Box are defined, then an exception is raised.
    """
    url = f"{CHS_BASE_URL}/stations"
    response = httpx.get(url)
    stations_data = response.json()

    stations_df = pd.DataFrame(stations_data)
    stations_df = gpd.GeoDataFrame(
        data=stations_df,
        geometry=gpd.points_from_xy(stations_df.longitude, stations_df.latitude, crs="EPSG:4326"),
    )

    region = get_region(
        region=region,
        lon_min=lon_min,
        lon_max=lon_max,
        lat_min=lat_min,
        lat_max=lat_max,
        symmetric=True,
    )

    if region:
        stations_df = stations_df[stations_df.within(region)]

    return stations_df