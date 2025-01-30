from __future__ import annotations

import functools
import json
import logging
from typing import Any
from typing import Dict
from typing import List

import geopandas as gpd
import httpx
import multifutures
import pandas as pd
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from searvey._common import _fetch_url
from searvey._common import _resolve_end_date
from searvey._common import _resolve_http_client
from searvey._common import _resolve_rate_limit
from searvey._common import _resolve_start_date
from searvey._common import _to_utc
from searvey.custom_types import DatetimeLike
from searvey.utils import get_region

logger = logging.getLogger(__name__)

CHS_BASE_URL = "https://api.iwls-sine.azure.cloud-nuage.dfo-mpo.gc.ca/api/v1"


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


def _fetch_chs(
    station_ids: List[str],
    time_series_code: str,
    start_dates: pd.DatetimeIndex,
    end_dates: pd.DatetimeIndex,
    rate_limit: multifutures.RateLimit | None = None,
    http_client: httpx.Client | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Retrieve the TimeSeries for multiple CHS stations using multithreading.
    """
    rate_limit = _resolve_rate_limit(rate_limit)
    http_client = _resolve_http_client(http_client)

    start_dates = _to_utc(start_dates, warn=True)
    end_dates = _to_utc(end_dates, warn=True)

    # Ensure that each station has a start_date and end_date
    if len(start_dates) != len(station_ids) or len(end_dates) != len(station_ids):
        raise ValueError("Each station must have a start_date and end_date")

    # Prepare arguments for each function call
    func_kwargs = [
        {
            "station_id": station_id,
            "time_series_code": time_series_code,
            "start_time": start_date,
            "end_time": end_date,
            "client": http_client,
            "rate_limit": rate_limit,
        }
        for station_id, start_date, end_date in zip(station_ids, start_dates, end_dates)
    ]
    # Fetch data concurrently using multithreading
    results: list[multifutures.FutureResult] = multifutures.multithread(
        func=_fetch_chs_station_data,
        func_kwargs=func_kwargs,
        executor=multithreading_executor,
    )

    dataframes = {
        result.kwargs["station_id"]: pd.DataFrame(result.result)
        for result in results
        if result.kwargs is not None and result.result is not None
    }
    return dataframes


def _fetch_chs_station_data(
    station_id: str,
    time_series_code: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    client: httpx.Client,
    rate_limit: multifutures.RateLimit,
) -> List[Dict[str, Any]]:
    """
    Fetch data for a single CHS station.
    Should not be called directly, this is a method used by other methods to generate a url and fetch data.
    """
    # Check if the data interval is more than 7 days
    if (end_time - start_time) > pd.Timedelta(days=7):
        raise ValueError("Data interval cannot be more than 7 days")

    if end_time < start_time:
        raise ValueError(f"'end_date' must be after 'start_date': {end_time} vs {start_time}")
    if start_time == end_time:
        return []

    url = f"{CHS_BASE_URL}/stations/{station_id}/data?"
    # Include time-series code directly in the URL
    url += f"time-series-code={time_series_code}"
    # Use ISO formatted strings for timestamps
    url += f"&from={start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    url += f"&to={end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    data = _fetch_url(url=url, client=client, rate_limit=rate_limit, redirect=False)
    data = json.loads(data)
    return data


def fetch_chs_station(
    station_id: str,
    time_series_code: str,
    start_date: DatetimeLike,
    end_date: DatetimeLike,
    rate_limit: multifutures.RateLimit | None = None,
    http_client: httpx.Client | None = None,
) -> pd.DataFrame:
    """
    Retrieve the TimeSeries of a single CHS station.
    Make a query to the CHS API for data for `station_id`
    and return the results as a pandas dataframe.
    """
    logger.info("CHS-%s: Starting data retrieval: %s - %s", station_id, start_date, end_date)
    try:
        now = pd.Timestamp.now("utc")
        df = _fetch_chs(
            station_ids=[station_id],
            time_series_code=time_series_code,
            start_dates=_resolve_start_date(now, start_date),
            end_dates=_resolve_end_date(now, end_date),
            rate_limit=rate_limit,
            http_client=http_client,
        )[station_id]

        if df.empty:
            logger.warning(f"No data available for station {station_id}")
        # write an example df with a row of fake data
        logger.info("CHS-%s: Finished data retrieval: %s - %s", station_id, start_date, end_date)

        return df

    except Exception as e:
        logger.error(f"Error fetching data for station {station_id}: {str(e)}")
        return pd.DataFrame()
