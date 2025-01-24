from __future__ import annotations

import logging
from typing import List
from typing import Union

import geopandas as gpd
import multifutures
import numpy as np
import pandas as pd
from ndbc_api import NdbcApi
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from searvey._common import _resolve_end_date
from searvey._common import _resolve_start_date
from searvey.custom_types import DatetimeLike
from searvey.utils import get_region

logger = logging.getLogger(__name__)


def _get_ndbc_stations(
    ndbc_api_client: NdbcApi | None,
    executor: multifutures.ExecutorProtocol | None,
) -> gpd.GeoDataFrame:
    """
    Return NDBC station metadata.
    :return: ``geopandas.GeoDataFrame`` with the station metadata
    """
    if ndbc_api_client is None:
        ndbc_api_client = NdbcApi()

    stations_df = ndbc_api_client.stations()
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

    kwargs = [{"station_id": st} for st in stations_df.Station]
    results = multifutures.multithread(
        func=ndbc_api_client.station,
        func_kwargs=kwargs,
        check=False,
        executor=executor,
        progress_bar=False,
    )
    details_df = pd.DataFrame.from_dict(
        {result.kwargs["station_id"]: result.result for result in results if result.kwargs is not None},
        orient="index",
    )
    stations_df = pd.merge(stations_df, details_df, left_on="Station", right_index=True).reset_index(
        drop=True
    )

    return stations_df


def get_ndbc_stations(
    region: MultiPolygon | Polygon | None = None,
    lon_min: float | None = None,
    lon_max: float | None = None,
    lat_min: float | None = None,
    lat_max: float | None = None,
    ndbc_api_client: NdbcApi | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
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

    ndbc_stations = _get_ndbc_stations(
        ndbc_api_client=ndbc_api_client,
        executor=multithreading_executor,
    )
    if region:
        ndbc_stations = ndbc_stations[ndbc_stations.within(region)]
    return ndbc_stations


def _fetch_ndbc(
    station_ids: List[str],
    mode: str,
    start_dates: Union[DatetimeLike, List[DatetimeLike]] = None,
    end_dates: Union[DatetimeLike, List[DatetimeLike]] = None,
    columns: list[str] | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
    ndbc_api_client: NdbcApi | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Retrieve the TimeSeries for multiple stations using multithreading.

    :param station_ids: A list of station identifiers.
    :param mode: Data mode. One of  ``'txt'``, ``'json'``, ``'spec'``.
    :param start_dates: The starting date of the query. Defaults to 7 days ago.
    :param end_dates: The finishing date of the query. Defaults to "now".
    :param columns: Optional list of columns to retrieve.
    :param multithreading_executor: A multithreading executor.
    :return: A dictionary mapping station identifiers to their respective
        TimeSeries.
    """
    now = pd.Timestamp.now("utc")
    if ndbc_api_client is None:
        ndbc_api_client = NdbcApi()

    # Ensure start_dates and end_dates are lists
    if not isinstance(start_dates, list):
        start_dates = [start_dates] * len(station_ids)
    if not isinstance(end_dates, list):
        end_dates = [end_dates] * len(station_ids)

    # Ensure that each station has a start_date and end_date
    if len(start_dates) != len(station_ids) or len(end_dates) != len(station_ids):
        raise ValueError("Each station must have a start_date and end_date")

    # Prepare arguments for each function call
    func_kwargs = [
        {
            "station_id": station_id,
            "mode": mode,
            "start_time": _resolve_start_date(now, start_dates)[0],
            "end_time": _resolve_end_date(now, end_dates)[0],
            "cols": columns,
        }
        for station_id, start_dates, end_dates in zip(station_ids, start_dates, end_dates)
    ]
    # Fetch data concurrently using multithreading
    results: list[multifutures.FutureResult] = multifutures.multithread(
        func=ndbc_api_client.get_data,
        func_kwargs=func_kwargs,
        executor=multithreading_executor,
    )

    dataframes = {
        result.kwargs["station_id"]: pd.DataFrame(result.result)
        for result in results
        if result.kwargs is not None
    }
    return dataframes


def fetch_ndbc_station(
    station_id: str,
    mode: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    columns: list[str] | None = None,
    ndbc_api_client: NdbcApi | None = None,
) -> pd.DataFrame:
    """
    Retrieve the TimeSeries of a single NDBC station.
    Make a query to the NDBC API for data for ``station_id``
    and return the results as a pandas dataframe.

    :param station_id: The station identifier.
    :param mode: Data mode. Read the example ndbc file for more info.
    :param start_date: The starting date of the query.
    :param end_date: The finishing date of the query.
    :param columns: Optional list of columns to retrieve.
    :return: ``pandas.DataFrame`` with the station data.
    """
    logger.info("NDBC-%s: Starting data retrieval: %s - %s", station_id, start_date, end_date)

    try:

        df = _fetch_ndbc(
            station_ids=[station_id],
            mode=mode,
            start_dates=start_date,
            end_dates=end_date,
            columns=columns,
            ndbc_api_client=ndbc_api_client,
        )[station_id]

        if df.empty:
            logger.warning(f"No data available for station {station_id}")

        logger.info("NDBC-%s: Finished data retrieval: %s - %s", station_id, start_date, end_date)

        return df

    except Exception as e:
        logger.error(f"Error fetching data for station {station_id}: {str(e)}")
        return pd.DataFrame()
