from __future__ import annotations

import collections
import json
import logging
import typing as T
from collections import abc
from datetime import timedelta
from typing import Any

import httpx
import multifutures
import numpy
import pandas
import pandas as pd

from ._common import _fetch_url
from ._common import _resolve_end_date
from ._common import _resolve_http_client
from ._common import _resolve_rate_limit
from ._common import _resolve_start_date
from ._common import _to_utc
from .coops import COOPS_Interval
from .coops import COOPS_Product
from .coops import COOPS_TidalDatum
from .coops import COOPS_Units
from .custom_types import DatetimeLike
from .utils import pairwise


logger = logging.getLogger(__name__)

# constants
COOPS_URL_TS_FORMAT = "%Y%m%d %H:%M"
COOPS_BASE_URL = "https://tidesandcurrents.noaa.gov/api/datagetter?"

COOPS_ProductFieldTypes = {
    "bin": int,
    "degree": float,
    "depth": float,
    "direction": str,
    "ebb_dir": float,
    "flags": str,  # TODO:
    "flood_dir": float,
    "gust": float,
    "highest": float,
    "inferred": bool,
    "lowest": float,
    "month": int,
    "quality": str,
    "salinity": float,
    "sigma": float,
    "specific_gravity": float,
    "speed": float,
    "time": "datetime64[ns]",
    "type": str,
    "value": float,
    "velocity": float,
    "year": int,
    "datum": str,
}


COOPS_ProductIntervalMap = {
    COOPS_Product.WATER_LEVEL: [COOPS_Interval.NONE],
    COOPS_Product.HOURLY_HEIGHT: [COOPS_Interval.NONE],
    COOPS_Product.HIGH_LOW: [COOPS_Interval.NONE],
    # COOPS_Product.DAILY_MEAN: [COOPS_Interval.NONE],
    COOPS_Product.MONTHLY_MEAN: [COOPS_Interval.NONE],
    COOPS_Product.ONE_MINUTE_WATER_LEVEL: [COOPS_Interval.NONE],
    COOPS_Product.PREDICTIONS: [
        COOPS_Interval.H,
        COOPS_Interval.ONE,
        COOPS_Interval.FIVE,
        COOPS_Interval.SIX,
        COOPS_Interval.TEN,
        COOPS_Interval.FIFTEEN,
        COOPS_Interval.THIRTY,
        COOPS_Interval.SIXTY,
        COOPS_Interval.HILO,
        COOPS_Interval.NONE,
    ],
    COOPS_Product.CURRENTS: [COOPS_Interval.H, COOPS_Interval.SIX, COOPS_Interval.NONE],
    COOPS_Product.CURRENTS_PREDICTIONS: [
        COOPS_Interval.H,
        COOPS_Interval.ONE,
        COOPS_Interval.SIX,
        COOPS_Interval.TEN,
        COOPS_Interval.THIRTY,
        COOPS_Interval.SIXTY,
        COOPS_Interval.MAX_SLACK,
        COOPS_Interval.NONE,
    ],
    COOPS_Product.AIR_GAP: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.WIND: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.AIR_PRESSURE: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.AIR_TEMPERATURE: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.VISIBILITY: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.HUMIDITY: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.WATER_TEMPERATURE: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.CONDUCTIVITY: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    # COOPS_Product.SALINITY: [COOPS_Interval.SIX, COOPS_Interval.H, COOPS_Interval.NONE],
    COOPS_Product.DATUMS: [COOPS_Interval.NONE],
}


COOPS_MaxInterval = {
    COOPS_Product.WATER_LEVEL: {COOPS_Interval.NONE: timedelta(days=30)},
    COOPS_Product.HOURLY_HEIGHT: {COOPS_Interval.NONE: timedelta(days=365)},
    COOPS_Product.HIGH_LOW: {COOPS_Interval.NONE: timedelta(days=365)},
    # COOPS_Product.DAILY_MEAN: {COOPS_Interval.NONE: timedelta(days=3650)},
    COOPS_Product.MONTHLY_MEAN: {COOPS_Interval.NONE: timedelta(days=73000)},
    COOPS_Product.ONE_MINUTE_WATER_LEVEL: {COOPS_Interval.NONE: timedelta(days=4)},
    COOPS_Product.PREDICTIONS: {
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.ONE: timedelta(days=365),
        COOPS_Interval.FIVE: timedelta(days=365),
        COOPS_Interval.SIX: timedelta(days=365),
        COOPS_Interval.TEN: timedelta(days=365),
        COOPS_Interval.FIFTEEN: timedelta(days=365),
        COOPS_Interval.THIRTY: timedelta(days=365),
        COOPS_Interval.SIXTY: timedelta(days=365),
        COOPS_Interval.HILO: timedelta(days=365),
        COOPS_Interval.NONE: timedelta(days=365),
    },
    COOPS_Product.CURRENTS: {
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.CURRENTS_PREDICTIONS: {
        COOPS_Interval.H: timedelta(days=30),
        COOPS_Interval.ONE: timedelta(days=30),
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.TEN: timedelta(days=30),
        COOPS_Interval.THIRTY: timedelta(days=30),
        COOPS_Interval.SIXTY: timedelta(days=30),
        COOPS_Interval.MAX_SLACK: timedelta(days=30),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.AIR_GAP: {
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.WIND: {
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.AIR_PRESSURE: {
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.AIR_TEMPERATURE: {
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.VISIBILITY: {
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.HUMIDITY: {
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.WATER_TEMPERATURE: {
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    COOPS_Product.CONDUCTIVITY: {
        COOPS_Interval.H: timedelta(days=365),
        COOPS_Interval.SIX: timedelta(days=30),
        COOPS_Interval.NONE: timedelta(days=30),
    },
    # COOPS_Product.SALINITY: {
    #     COOPS_Interval.H: timedelta(days=365),
    #     COOPS_Interval.SIX: timedelta(days=30),
    #     COOPS_Interval.NONE: timedelta(days=30),
    # },
    COOPS_Product.DATUMS: {COOPS_Interval.NONE: timedelta(days=30)},
}


COOPS_ProductFieldsNameMap = {
    COOPS_Product.WATER_LEVEL: {"t": "time", "v": "value", "s": "sigma", "f": "flags", "q": "quality"},
    COOPS_Product.HOURLY_HEIGHT: {"t": "time", "v": "value", "s": "sigma", "f": "flags"},
    COOPS_Product.HIGH_LOW: {"t": "time", "v": "value", "ty": "type", "f": "flags"},
    # COOPS_Product.DAILY_MEAN: {"t": "time", "v": "value", "f": "flags"},
    COOPS_Product.MONTHLY_MEAN: {
        "year": "year",
        "month": "month",
        "highest": "highest",
        "MHHW": "MHHW",
        "MHW": "MHW",
        "MSL": "MSL",
        "MTL": "MTL",
        "MLW": "MLW",
        "MLLW": "MLLW",
        "DTL": "DTL",
        "GT": "GT",
        "MN": "MN",
        "DHQ": "DHQ",
        "DLQ": "DLQ",
        "HWI": "HWI",
        "LWI": "LWI",
        "lowest": "lowest",
        "inferred": "inferred",
    },
    COOPS_Product.ONE_MINUTE_WATER_LEVEL: {"t": "time", "v": "value"},
    COOPS_Product.PREDICTIONS: {"t": "time", "v": "value"},
    COOPS_Product.AIR_GAP: {"t": "time", "v": "value", "s": "sigma", "f": "flags"},
    COOPS_Product.WIND: {
        "t": "time",
        "s": "speed",
        "d": "degree",
        "dr": "direction",
        "g": "gust",
        "f": "flags",
    },
    COOPS_Product.AIR_PRESSURE: {"t": "time", "v": "value", "f": "flags"},
    COOPS_Product.AIR_TEMPERATURE: {"t": "time", "v": "value", "f": "flags"},
    COOPS_Product.VISIBILITY: {"t": "time", "v": "value", "f": "flags"},
    COOPS_Product.HUMIDITY: {"t": "time", "v": "value", "f": "flags"},
    COOPS_Product.WATER_TEMPERATURE: {"t": "time", "v": "value", "f": "flags"},
    COOPS_Product.CONDUCTIVITY: {"t": "time", "v": "value", "f": "flags"},
    # COOPS_Product.SALINITY: {"t": "time", "s": "salinity", "g": "specific_gravity"},
    COOPS_Product.CURRENTS: {"t": "time", "s": "speed", "d": "direction", "b": "bin"},
    COOPS_Product.CURRENTS_PREDICTIONS: {
        "Time": "time",
        "Velocity_Major": "velocity",
        "meanEbbDir": "ebb_dir",
        "meanFloodDir": "flood_dir",
        "Bin": "bin",
        "Depth": "depth",
        "Speed": "speed",
        "Direction": "direction",
    },
    COOPS_Product.DATUMS: {"n": "datum", "v": "value"},
}


def _parse_coops_responses(
    coops_responses: list[multifutures.FutureResult],
    executor: multifutures.ExecutorProtocol | None,
) -> list[multifutures.FutureResult]:
    # Parse the json files using pandas
    # This is a CPU heavy process, so let's use multiprocess
    # Not all the urls contain data, so let's filter them out
    kwargs = []
    for result in coops_responses:
        station_id = result.kwargs["station_id"]  # type: ignore[index]
        product = result.kwargs["product"]  # type: ignore[index]
        if "error" in result.result:
            msg = json.loads(result.result)["error"]["message"]
            logger.error(f"{station_id}: Encountered an error response for {result.kwargs}!")
            logger.error(f"--> {msg}")
            continue
        else:
            kwargs.append(dict(station_id=station_id, product=product, content=result.result))
    logger.debug("Starting JSON parsing")
    results = multifutures.multiprocess(_parse_json, func_kwargs=kwargs, check=False, executor=executor)
    multifutures.check_results(results)
    logger.debug("Finished JSON parsing")
    return results


def _coops_date(ts: pd.Timestamp) -> str:
    formatted: str = ts.strftime(COOPS_URL_TS_FORMAT)
    return formatted


def _generate_urls(
    station_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    product: COOPS_Product = COOPS_Product.WATER_LEVEL,
    datum: COOPS_TidalDatum = COOPS_TidalDatum.MSL,
    units: COOPS_Units = COOPS_Units.METRIC,
    interval: COOPS_Interval = COOPS_Interval.NONE,
    **aux_params: Any,
) -> list[httpx.URL]:
    if end_date < start_date:
        raise ValueError(f"'end_date' must be after 'start_date': {end_date} vs {start_date}")
    if end_date == start_date:
        return []
    duration = end_date - start_date
    periods = duration.days // COOPS_MaxInterval[product][interval].days + 2
    urls = []
    date_range = pd.date_range(start_date, end_date, periods=periods, unit="us", inclusive="both")
    params = {
        "station": station_id,
        "product": product.value,
        "datum": datum.value,
        "units": units.value,
        "time_zone": "gmt",  # We always work with UTC/GMT
        "format": "json",
        "application": "oceanmodeling/stormevents",
    }
    if interval.value is not None:
        params["interval"] = interval.value
    params.update(**aux_params)
    for start, stop in pairwise(date_range):
        params["begin_date"] = _coops_date(start)
        params["end_date"] = _coops_date(stop)
        url = httpx.Request("GET", COOPS_BASE_URL, params=params).url
        urls.append(url)
    return urls


def _normalize_df(df: pd.DataFrame, product: COOPS_Product) -> pd.DataFrame:
    # TODO: Add more info (datum, unit, tz)?

    nos_id = df.attrs["station_id"]
    normalized = df.rename(columns=COOPS_ProductFieldsNameMap[product])
    logger.debug("%s: df contains the following columns: %s", nos_id, normalized.columns)

    normalized[normalized == ""] = numpy.nan
    normalized = normalized.astype(
        {k: v for k, v in COOPS_ProductFieldTypes.items() if k in normalized.columns}, errors="ignore"
    )
    # NOTE: Datum and mean products doesn't have time!
    if "time" in normalized.columns:
        normalized["time"] = pandas.to_datetime(normalized["time"], utc=True)
        normalized.set_index("time", inplace=True)

    return normalized


def _parse_json(content: str, station_id: str, product: COOPS_Product) -> pd.DataFrame:
    err_msg = ""
    content_json = {}
    try:
        content_json = json.loads(content)
        if not content_json:
            err_msg = f"{station_id}: The station does not contain any data for product.value!"
    except json.JSONDecodeError as e:
        err_msg = f"{station_id}: Error decoding JSON {str(e)}"

    if err_msg:
        logger.error(err_msg)
        return pd.DataFrame()

    data = []
    if product == COOPS_Product.CURRENTS_PREDICTIONS:
        data = content_json["current_predictions"]["cp"]
    elif product == COOPS_Product.PREDICTIONS:
        data = content_json["predictions"]
    elif product == COOPS_Product.DATUMS:
        data = content_json["datums"]
    else:
        data = content_json["data"]

    df = pd.DataFrame(data)
    df.attrs["station_id"] = f"COOPS-{station_id}"
    df = _normalize_df(df, product)
    return df


def _group_results(
    station_ids: abc.Collection[str],
    parsed_responses: list[multifutures.FutureResult],
) -> dict[str, pd.DataFrame]:
    # Group per COOPS code
    df_groups = collections.defaultdict(list)
    for item in parsed_responses:
        df_groups[item.kwargs["station_id"]].append(item.result)  # type: ignore[index]

    # Concatenate dataframes
    dataframes: dict[str, pd.DataFrame] = {}
    for station_id in station_ids:
        if station_id in df_groups:
            df_group = df_groups[station_id]
            df = pd.concat(df_group)
            df = df.sort_index()
            logger.debug("COOPS-%s: Timestamps: %d", station_id, len(df))
        else:
            logger.warning("COOPS-%s: No data. Creating a dummy dataframe", station_id)
            df = T.cast(
                pd.DataFrame, pd.DataFrame(columns=["time"], dtype="datetime64[ns]").set_index("time")
            )
        dataframes[station_id] = df
        logger.debug("COOPS-%s: Finished conversion to pandas", station_id)

    return dataframes


def _retrieve_coops_data(
    station_ids: abc.Collection[str],
    start_dates: abc.Collection[pd.Timestamp],
    end_dates: abc.Collection[pd.Timestamp],
    product: COOPS_Product,
    datum: COOPS_TidalDatum,
    units: COOPS_Units,
    interval: COOPS_Interval,
    rate_limit: multifutures.RateLimit,
    http_client: httpx.Client,
    executor: multifutures.ExecutorProtocol | None,
    **aux_params: Any,
) -> list[multifutures.FutureResult]:
    kwargs = []

    valid_intervals = COOPS_ProductIntervalMap[product]
    if interval not in valid_intervals:
        raise ValueError(
            "interval must be one of '{}'".format("', '".join(str(v.value) for v in valid_intervals))
        )

    for station_id, start_date, end_date in zip(station_ids, start_dates, end_dates):
        url_kwargs = {
            "station_id": station_id,
            "start_date": start_date,
            "end_date": end_date,
            "product": product,
            "datum": datum,
            "units": units,
            "interval": interval,
            **aux_params,
        }
        for url in _generate_urls(**url_kwargs):
            if url:
                kwargs.append(
                    dict(
                        station_id=station_id,
                        url=url,
                        client=http_client,
                        rate_limit=rate_limit,
                        product=product,
                        redirect=True,
                    ),
                )
    with http_client:
        logger.debug("Starting data retrieval")
        results = multifutures.multithread(
            func=_fetch_url, func_kwargs=kwargs, check=False, executor=executor
        )
        logger.debug("Finished data retrieval")
    multifutures.check_results(results)
    return results


def _fetch_coops(
    station_ids: abc.Collection[str],
    start_dates: pd.DatetimeIndex,
    end_dates: pd.DatetimeIndex,
    *,
    product: COOPS_Product | str,
    datum: COOPS_TidalDatum | str,
    units: COOPS_Units | str,
    interval: COOPS_Interval | int | str | None,
    rate_limit: multifutures.RateLimit | None,
    http_client: httpx.Client | None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None,
    multithreading_executor: multifutures.ExecutorProtocol | None,
    **aux_params: Any,
) -> dict[str, pd.DataFrame]:
    rate_limit = _resolve_rate_limit(rate_limit)
    http_client = _resolve_http_client(http_client)
    start_dates = _to_utc(start_dates)
    end_dates = _to_utc(end_dates)
    # Fetch json files from the COOPS website
    # We use multithreading in order to be able to use RateLimit + to take advantage of higher performance

    coops_responses: list[multifutures.FutureResult] = _retrieve_coops_data(
        station_ids=station_ids,
        start_dates=start_dates,
        end_dates=end_dates,
        product=COOPS_Product(product),
        datum=COOPS_TidalDatum(datum),
        units=COOPS_Units(units),
        interval=COOPS_Interval(interval),
        rate_limit=rate_limit,
        http_client=http_client,
        executor=multithreading_executor,
        **aux_params,
    )
    # Parse the json files using pandas
    # This is a CPU heavy process, so we are using multiprocessing here
    parsed_responses: list[multifutures.FutureResult] = _parse_coops_responses(
        coops_responses=coops_responses,
        executor=multiprocessing_executor,
    )
    # OK, now we have a list of dataframes. We need to group them per coops_code, concatenate them and remove duplicates
    dataframes = _group_results(station_ids=station_ids, parsed_responses=parsed_responses)
    return dataframes


def fetch_coops_station(
    station_id: str,
    start_date: DatetimeLike | None = None,
    end_date: DatetimeLike | None = None,
    *,
    rate_limit: multifutures.RateLimit | None = None,
    http_client: httpx.Client | None = None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
    product: COOPS_Product | str = COOPS_Product.WATER_LEVEL,
    datum: COOPS_TidalDatum | str = COOPS_TidalDatum.MSL,
    units: COOPS_Units | str = COOPS_Units.METRIC,
    interval: COOPS_Interval | str | int | None = COOPS_Interval.NONE,
    **aux_params: Any,
) -> pd.DataFrame:
    """
    Make a query to the COOPS API for tide gauge data for ``station_id``
    and return the results as a ``pandas.Dataframe``.

    ``start_date`` and ``end_date`` can be of any type that is valid for ``pandas.to_datetime()``.
    If ``start_date`` or ``end_date`` are timezone-aware timestamps they are coersed to UTC.
    The returned data are always in UTC.

    Each query to the COOPS API can request up to 30 days of data.
    When we request data for larger time spans, multiple requests are made.
    This is where ``rate_limit``, ``multiprocessing_executor`` and ``multithreading_executor``
    come into play.

    In order to make the data retrieval more efficient, a multithreading pool is spawned
    and the requests are executed concurrently, while adhering to the ``rate_limit``.
    The parsing of the JSON responses is a CPU heavy process so it is made within a multiprocessing Pool.

    If no arguments are specified, then sensible defaults are being used, but if the pools need to be
    configured, an `executor` instance needs to be passed as an argument. For example:

    .. code-block:: python

        executor = concurrent.futures.ProcessPoolExecutor(max_workers=4)
        df = fetch_coops_station("acap", multiprocessing_executor=executor)

    :param station_id: The station identifier. In COOPS terminology, this is called ``coops_code``.
    :param start_date: The starting date of the query. Defaults to 7 days ago.
    :param end_date: The finishing date of the query. Defaults to "now".
    :param rate_limit: The rate limit for making requests to the COOPS servers. Defaults to 5 requests/second.
    :param http_client: The ``httpx.Client``.
    :param multiprocessing_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
    :param multithreading_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
    """
    logger.info("COOPS-%s: Starting scraping: %s - %s", station_id, start_date, end_date)
    now = pd.Timestamp.now("utc")
    df = _fetch_coops(
        station_ids=[station_id],
        start_dates=_resolve_start_date(now, start_date),
        end_dates=_resolve_end_date(now, end_date),
        product=COOPS_Product(product),
        datum=COOPS_TidalDatum(datum),
        units=COOPS_Units(units),
        interval=COOPS_Interval(interval),
        **aux_params,
        rate_limit=rate_limit,
        http_client=http_client,
        multiprocessing_executor=multiprocessing_executor,
        multithreading_executor=multithreading_executor,
    )[station_id]
    logger.info("COOPS-%s: Finished scraping: %s - %s", station_id, start_date, end_date)
    return df
