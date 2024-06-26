from __future__ import annotations

import collections
import io
import logging
import typing as T
from collections import abc

import httpx
import multifutures
import pandas as pd

from ._common import _fetch_url
from ._common import _resolve_end_date
from ._common import _resolve_http_client
from ._common import _resolve_rate_limit
from ._common import _resolve_start_date
from ._common import _to_utc
from .custom_types import DatetimeLike
from .ioc import IOC_STATION_DATA_COLUMNS
from .utils import pairwise


logger = logging.getLogger(__name__)


BASE_URL = "https://www.ioc-sealevelmonitoring.org/service.php?query=data&timestart={timestart}&timestop={timestop}&code={ioc_code}"

IOC_URL_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
IOC_JSON_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _parse_ioc_responses(
    ioc_responses: list[multifutures.FutureResult],
    executor: multifutures.ExecutorProtocol | None,
    progress_bar: bool,
) -> list[multifutures.FutureResult]:
    # Parse the json files using pandas
    # This is a CPU heavy process, so let's use multiprocess
    # Not all the urls contain data, so let's filter them out
    kwargs = []
    for result in ioc_responses:
        station_id = result.kwargs["station_id"]  # type: ignore[index]
        # if a url doesn't have any data instead of a 404, it returns an empty list `[]`
        if result.result == "[]":
            continue
        # For some stations though we get a json like this:
        #    '[{"error":"code \'blri\' not found"}]'
        #    '[{"error":"code \'bmda2\' not found"}]'
        # we should ignore these, too
        elif result.result == f"""[{{"error":"code '{station_id}' not found"}}]""":
            continue
        # And if the IOC code does not match some pattern (5 letters?) then we get this error
        elif result.result == '[{"error":"Incorrect code"}]':
            continue
        else:
            kwargs.append(dict(station_id=station_id, content=io.StringIO(result.result)))
    logger.debug("Starting JSON parsing")
    results = multifutures.multiprocess(
        _parse_json, func_kwargs=kwargs, check=False, executor=executor, progress_bar=progress_bar
    )
    multifutures.check_results(results)
    logger.debug("Finished JSON parsing")
    return results


def _ioc_date(ts: pd.Timestamp) -> str:
    formatted: str = ts.strftime(IOC_URL_TS_FORMAT)
    return formatted


def _generate_urls(
    station_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[str]:
    if end_date < start_date:
        raise ValueError(f"'end_date' must be after 'start_date': {end_date} vs {start_date}")
    if end_date == start_date:
        return []
    duration = end_date - start_date
    periods = duration.days // 30 + 2
    urls = []
    date_range = pd.date_range(start_date, end_date, periods=periods, unit="us", inclusive="both")
    for start, stop in pairwise(date_range):
        timestart = _ioc_date(start)
        timestop = _ioc_date(stop)
        url = BASE_URL.format(ioc_code=station_id, timestart=timestart, timestop=timestop)
        urls.append(url)
    return urls


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = (
        df[df.sensor.isin(IOC_STATION_DATA_COLUMNS.values())]
        .assign(stime=pd.DatetimeIndex(pd.to_datetime(df.stime.str.strip(), format=IOC_JSON_TS_FORMAT)))
        .rename(columns={"stime": "time"})
    )
    # Occasionally IOC contains complete garbage. E.g. duplicate timestamps on the same sensor. We should drop those.
    # https://www.ioc-sealevelmonitoring.org/service.php?query=data&timestart=2022-03-12T11:03:40&timestop=2022-04-11T09:04:26&code=acnj
    duplicated_timestamps = normalized[["time", "sensor"]].duplicated()
    if duplicated_timestamps.sum() > 0:
        normalized = normalized[~duplicated_timestamps]
        logger.warning(
            "%s: Dropped duplicates: %d rows", normalized.attrs["station_id"], duplicated_timestamps.sum()
        )
    normalized = normalized.pivot(index="time", columns="sensor", values="slevel")
    normalized._mgr.items.name = ""
    return normalized


def _parse_json(content: str, station_id: str) -> pd.DataFrame:
    df = pd.read_json(content, orient="records")
    df.attrs["station_id"] = f"IOC-{station_id}"
    df = _normalize_df(df)
    return df


def _group_results(
    station_ids: abc.Collection[str],
    parsed_responses: list[multifutures.FutureResult],
) -> dict[str, pd.DataFrame]:
    # Group per IOC code
    df_groups = collections.defaultdict(list)
    for item in parsed_responses:
        df_groups[item.kwargs["station_id"]].append(item.result)  # type: ignore[index]

    # Concatenate dataframes and remove duplicates
    dataframes: dict[str, pd.DataFrame] = {}
    for station_id in station_ids:
        if station_id in df_groups:
            df_group = df_groups[station_id]
            df = pd.concat(df_group)
            df = df.sort_index()
            logger.debug("IOC-%s: Total timestamps : %d", station_id, len(df))
            df = df[~df.index.duplicated()]
            logger.debug("IOC-%s: Unique timestamps: %d", station_id, len(df))
        else:
            logger.warning("IOC-%s: No data. Creating a dummy dataframe", station_id)
            df = T.cast(
                pd.DataFrame, pd.DataFrame(columns=["time"], dtype="datetime64[ns]").set_index("time")
            )
        dataframes[station_id] = df
        logger.debug("IOC-%s: Finished conversion to pandas", station_id)

    return dataframes


def _retrieve_ioc_data(
    station_ids: abc.Collection[str],
    start_dates: abc.Collection[pd.Timestamp],
    end_dates: abc.Collection[pd.Timestamp],
    rate_limit: multifutures.RateLimit,
    http_client: httpx.Client,
    executor: multifutures.ExecutorProtocol | None,
    progress_bar: bool,
) -> list[multifutures.FutureResult]:
    kwargs = []
    for station_id, start_date, end_date in zip(station_ids, start_dates, end_dates):
        for url in _generate_urls(station_id=station_id, start_date=start_date, end_date=end_date):
            if url:
                kwargs.append(
                    dict(
                        station_id=station_id,
                        url=url,
                        client=http_client,
                        rate_limit=rate_limit,
                    ),
                )
    with http_client:
        logger.debug("Starting data retrieval")
        results = multifutures.multithread(
            func=_fetch_url,
            func_kwargs=kwargs,
            check=False,
            executor=executor,
            progress_bar=progress_bar,
        )
        logger.debug("Finished data retrieval")
    multifutures.check_results(results)
    return results


def _fetch_ioc(
    station_ids: abc.Collection[str],
    start_dates: pd.DatetimeIndex,
    end_dates: pd.DatetimeIndex,
    *,
    rate_limit: multifutures.RateLimit | None,
    http_client: httpx.Client | None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None,
    multithreading_executor: multifutures.ExecutorProtocol | None,
    progress_bar: bool,
) -> dict[str, pd.DataFrame]:
    rate_limit = _resolve_rate_limit(rate_limit)
    http_client = _resolve_http_client(http_client)
    start_dates = _to_utc(start_dates)
    end_dates = _to_utc(end_dates)
    # Fetch json files from the IOC website
    # We use multithreading in order to be able to use RateLimit + to take advantage of higher performance
    ioc_responses: list[multifutures.FutureResult] = _retrieve_ioc_data(
        station_ids=station_ids,
        start_dates=start_dates,
        end_dates=end_dates,
        rate_limit=rate_limit,
        http_client=http_client,
        executor=multithreading_executor,
        progress_bar=progress_bar,
    )
    # Parse the json files using pandas
    # This is a CPU heavy process, so we are using multiprocessing here
    parsed_responses: list[multifutures.FutureResult] = _parse_ioc_responses(
        ioc_responses=ioc_responses,
        executor=multiprocessing_executor,
        progress_bar=progress_bar,
    )
    # OK, now we have a list of dataframes. We need to group them per ioc_code, concatenate them and remove duplicates
    dataframes = _group_results(station_ids=station_ids, parsed_responses=parsed_responses)
    return dataframes


def fetch_ioc_station(
    station_id: str,
    start_date: DatetimeLike | None = None,
    end_date: DatetimeLike | None = None,
    *,
    rate_limit: multifutures.RateLimit | None = None,
    http_client: httpx.Client | None = None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
    progress_bar: bool = False,
) -> pd.DataFrame:
    """
    Make a query to the IOC API for tide gauge data for ``station_id``
    and return the results as a ``pandas.Dataframe``.

    .. code-block:: python

        fetch_ioc_station("acap2")
        fetch_ioc_station("acap2", start_date="2023-01-01", end_date="2023-01-02")

    ``start_date`` and ``end_date`` can be of any type that is valid for ``pandas.to_datetime()``.
    If ``start_date`` or ``end_date`` are timezone-aware timestamps they are coersed to UTC.
    The returned data are always in UTC.

    Each query to the IOC API can request up to 30 days of data.
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
        df = fetch_ioc_station("acap", multiprocessing_executor=executor)

    :param station_id: The station identifier. In IOC terminology, this is called ``ioc_code``.
    :param start_date: The starting date of the query. Defaults to 7 days ago.
    :param end_date: The finishing date of the query. Defaults to "now".
    :param rate_limit: The rate limit for making requests to the IOC servers. Defaults to 5 requests/second.
    :param http_client: The ``httpx.Client``. Can be used to setup e.g. an HTTP proxy.
    :param multiprocessing_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
    :param multithreading_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
    :param progress_bar: If ``True`` then a progress bar is displayed for monitoring the progress of the outgoing requests.
    :return: ``pandas.DataFrame`` with the station data.
    """
    logger.info("IOC-%s: Starting scraping: %s - %s", station_id, start_date, end_date)
    now = pd.Timestamp.now("utc")
    df = _fetch_ioc(
        station_ids=[station_id],
        start_dates=_resolve_start_date(now, start_date),
        end_dates=_resolve_end_date(now, end_date),
        rate_limit=rate_limit,
        http_client=http_client,
        multiprocessing_executor=multiprocessing_executor,
        multithreading_executor=multithreading_executor,
        progress_bar=progress_bar,
    )[station_id]
    logger.info("IOC-%s: Finished scraping: %s - %s", station_id, start_date, end_date)
    return df
