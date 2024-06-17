from __future__ import annotations

import functools
import logging
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from searvey._common import _resolve_end_date
from searvey._common import _resolve_start_date
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
    region: MultiPolygon | Polygon | None = None,
    lon_min: float | None = None,
    lon_max: float | None = None,
    lat_min: float | None = None,
    lat_max: float | None = None,
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



def _fetch_ndbc_station_data(
    station_id: str,
    mode: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Retrieve the TimeSeries of a single NDBC station."""
    try:
        df = ndbc_api.get_data(
            station_id=station_id,
            mode=mode,
            start_time=start_time,
            end_time=end_time,
            as_df=True,
            cols=columns,
        )
        if df.empty:
            logger.warnings(f"No data avaliable for station {station_id}")
        return df
    except Exception as e:
        logger.error(f"Error fetching data for station {station_id}: {str(e)}")
        return pd.DataFrame()

def fetch_ndbc_stations_data(
    station_ids: List[str],
    mode: str,
    start_date: DatetimeLike | None = None,
    end_date: DatetimeLike | None = None,
    columns: list[str] | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Retrieve the TimeSeries for multiple stations using multithreading.

    :param station_ids: A list of station identifiers.
    :param mode: Data mode. One of  ``'txt'``, ``'json'``, ``'spec'``.
    :param start_date: The starting date of the query. Defaults to 7 days ago.
    :param end_date: The finishing date of the query. Defaults to "now".
    :param columns:
    :param multithreading_executor: A multithreading executor.
    :return: A dictionary mapping station identifiers to their respective
        TimeSeries.
    """
    now = pd.Timestamp.now("utc")
    start_date = _resolve_start_date(now, start_date)
    end_date = _resolve_end_date(now, end_date)

    # Prepare arguments for each function call
    func_kwargs = [
        {
            "station_id": station_id,
            "mode": mode,
            "start_time": start_date[0],
            "end_time": end_date[0],
            "columns": columns,
        }
        for station_id in station_ids
    ]

    # Fetch data concurrently using multithreading
    results: list[multifutures.FutureResult] = multifutures.multithread(
        func=_fetch_ndbc_station_data,
        func_kwargs=func_kwargs,
        executor=multithreading_executor,
    )

    # Check for errors and collect results
    multifutures.check_results(results)
    dataframes = {
        result.kwargs["station_id"]: result.result
        for result in results
        if result.exception is None
    }
    return dataframes