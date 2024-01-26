# For the IOC stations we parse their website: http://www.ioc-sealevelmonitoring.org/list.php?showall=all
#
# This page contains 3 tables:
#
# - ``general``
# - ``contacts``
# - ``performance``
#
# We parse all 3 of them and we merge them.
from __future__ import annotations

import collections
import functools
import io
import logging
import typing as T
import warnings
from collections import abc
from typing import Optional
from typing import Union

import bs4
import geopandas as gpd
import html5lib  # noqa: F401  # imported but unused
import httpx
import limits
import lxml  # noqa: F401  # imported but unused
import multifutures
import pandas as pd
import requests
import tenacity
import xarray as xr
from deprecated import deprecated
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from .custom_types import DateTimeLike
from .custom_types import DatetimeLike
from .multi import multiprocess
from .multi import multithread
from .rate_limit import RateLimit
from .rate_limit import wait
from .utils import get_region
from .utils import merge_datasets
from .utils import NOW
from .utils import pairwise
from .utils import resolve_timestamp


logger = logging.getLogger(__name__)

# constants
IOC_RATE_LIMIT = limits.parse("5/second")
IOC_MAX_DAYS_PER_REQUEST = 30
IOC_BASE_URL = "http://www.ioc-sealevelmonitoring.org/bgraph.php?code={ioc_code}&output=tab&period={period}&endtime={endtime}"
IOC_STATIONS_KWARGS = [
    {"output": "general", "skip_table_rows": 4},
    {"output": "contacts", "skip_table_rows": 4},
    {"output": "performance", "skip_table_rows": 8},
]
IOC_STATIONS_COLUMN_NAMES = {
    "general": [
        "ioc_code",
        "gloss_id",
        "country",
        "location",
        "connection",
        "dcp_id",
        "last_observation_level",
        "last_observation_time",
        "delay",
        "interval",
        "view",
    ],
    "contacts": [
        "ioc_code",
        "gloss_id",
        "lat",
        "lon",
        "country",
        "location",
        "connection",
        "contacts",
        "view",
    ],
    "performance": [
        "ioc_code",
        "gloss_id",
        "country",
        "location",
        "connection",
        "added_to_system",
        "observations_arrived_per_week",
        "observations_expected_per_week",
        "observations_ratio_per_week",
        "observations_arrived_per_month",
        "observations_expected_per_month",
        "observations_ratio_per_month",
        "observations_ratio_per_day",
        "sample_interval",
        "average_delay_per_day",
        "transmit_interval",
        "view",
    ],
}
IOC_STATION_DATA_COLUMNS_TO_DROP = [
    "bat",
    "sw1",
    "sw2",
    "(m)",
]
IOC_STATION_DATA_COLUMNS = {
    "Time (UTC)": "time",
    "aqu(m)": "aqu",
    "atm(m)": "atm",
    "bat(V)": "bat",
    "bub(m)": "bub",
    "bwl(m)": "bwl",
    "ecs(m)": "ecs",
    "enb(m)": "enb",
    "enc(m)": "enc",
    "flt(m)": "flt",
    "pr1(m)": "pr1",
    "pr2(m)": "pr2",
    "prs(m)": "prs",
    "prt(m)": "prt",
    "prte(m)": "prte",
    "pwl(m)": "pwl",
    "ra2(m)": "ra2",
    "ra3(m)": "ra3",
    "rad(m)": "rad",
    "ras(m)": "ras",
    "stp(m)": "stp",
    "sw1(min)": "sw1",
    "sw2(min)": "sw2",
    "wls(m)": "wls",
}


def get_ioc_stations_by_output(output: str, skip_table_rows: int) -> pd.DataFrame:
    url = f"https://www.ioc-sealevelmonitoring.org/list.php?showall=all&output={output}#"
    logger.debug("Downloading: %s", url)
    response = requests.get(url)
    assert response.ok, f"failed to download: {url}"
    logger.debug("Downloaded: %s", url)
    soup = bs4.BeautifulSoup(response.content, "html5lib")
    logger.debug("Created soup: %s", url)
    table = soup.find("table", {"class": "nice"})
    trs = table.find_all("tr")
    table_contents = "\n".join(str(tr) for tr in trs[skip_table_rows:])
    html = f"<table>{table_contents}</table>"
    logger.debug("Created table: %s", url)
    df = pd.read_html(io.StringIO(html))[0]
    logger.debug("Parsed table: %s", url)
    df.columns = IOC_STATIONS_COLUMN_NAMES[output]
    df = df.drop(columns="view")
    return df


def normalize_ioc_stations(df: pd.DataFrame) -> gpd.GeoDataFrame:
    df = df.assign(
        # fmt: off
        gloss_id=df.gloss_id.astype(pd.Int64Dtype()),
        observations_ratio_per_day=df.observations_ratio_per_day.replace("-", "0%").str[:-1].astype(int),
        observations_ratio_per_week=df.observations_ratio_per_week.replace("-", "0%").str[:-1].astype(int),
        observations_ratio_per_month=df.observations_ratio_per_month.replace("-", "0%").str[:-1].astype(int),
        # fmt: on
    )
    gdf = gpd.GeoDataFrame(
        data=df,
        geometry=gpd.points_from_xy(df.lon, df.lat, crs="EPSG:4326"),
    )
    return gdf


@functools.lru_cache
def _get_ioc_stations() -> gpd.GeoDataFrame:
    """
    Return IOC station metadata from: http://www.ioc-sealevelmonitoring.org/list.php?showall=all

    :return: ``pandas.DataFrame`` with the station metadata
    """

    # The IOC web page with the station metadata contains really ugly HTTP code.
    # creating the `bs4.Beautiful` instance takes as much time as you need to download the page,
    # therefore multiprocessing is actually faster than multithreading
    ioc_stations_results = multiprocess(
        func=get_ioc_stations_by_output,
        func_kwargs=IOC_STATIONS_KWARGS,
    )
    ioc_stations = functools.reduce(pd.merge, (r.result for r in ioc_stations_results))
    ioc_stations = normalize_ioc_stations(ioc_stations)
    return ioc_stations


def get_ioc_stations(
    region: Optional[Union[Polygon, MultiPolygon]] = None,
    lon_min: Optional[float] = None,
    lon_max: Optional[float] = None,
    lat_min: Optional[float] = None,
    lat_max: Optional[float] = None,
) -> gpd.GeoDataFrame:
    """
    Return IOC station metadata from: http://www.ioc-sealevelmonitoring.org/list.php?showall=all

    If `region` is defined then the stations that are outside of the region are
    filtered out.. If the coordinates of the Bounding Box are defined then
    stations outside of the BBox are filtered out. If both ``region`` and the
    Bounding Box are defined, then an exception is raised.

    Note: The longitudes of the IOC stations are in the [-180, 180] range.

    :param region: ``Polygon`` or ``MultiPolygon`` denoting region of interest
    :param lon_min: The minimum Longitude of the Bounding Box.
    :param lon_max: The maximum Longitude of the Bounding Box.
    :param lat_min: The minimum Latitude of the Bounding Box.
    :param lat_max: The maximum Latitude of the Bounding Box.
    :return: ``pandas.DataFrame`` with the station metadata
    """
    region = get_region(
        region=region,
        lon_min=lon_min,
        lon_max=lon_max,
        lat_min=lat_min,
        lat_max=lat_max,
        symmetric=True,
    )

    ioc_stations = _get_ioc_stations()
    if region:
        ioc_stations = ioc_stations[ioc_stations.within(region)]
    return ioc_stations


def normalize_ioc_station_data(ioc_code: str, df: pd.DataFrame, truncate_seconds: bool) -> pd.DataFrame:
    # Each station may have more than one sensors.
    # Some of the sensors have nothing to do with sea level height. We drop these sensors
    df = df.rename(columns=IOC_STATION_DATA_COLUMNS)
    logger.debug("%s: df contains the following columns: %s", ioc_code, df.columns)
    df = df.drop(columns=IOC_STATION_DATA_COLUMNS_TO_DROP, errors="ignore")
    if len(df.columns) == 1:
        # all the data columns have been dropped!
        msg = f"{ioc_code}: The table does not contain any sensor data!"
        logger.info(msg)
        raise ValueError(msg)
    df = df.assign(
        ioc_code=ioc_code,
        time=pd.to_datetime(df.time),
    )
    if truncate_seconds:
        # Truncate seconds from timestamps: https://stackoverflow.com/a/28783971/592289
        # WARNING: This can potentially lead to duplicates!
        df = df.assign(time=df.time.dt.floor("min"))
        if df.time.duplicated().any():
            # There are duplicates. Keep the first datapoint per minute.
            msg = f"{ioc_code}: Duplicate timestamps have been detected after the truncation of seconds. Keeping the first datapoint per minute"
            warnings.warn(msg)
            df = df.iloc[df.time.drop_duplicates().index].reset_index(drop=True)
    return df


@deprecated(
    version="0.3.11",
    reason="This function is deprecated and will be removed in the future. Replace it with `fetch_ioc_station`.",
)
def get_ioc_station_data(
    ioc_code: str,
    endtime: DateTimeLike = NOW,
    period: float = IOC_MAX_DAYS_PER_REQUEST,
    truncate_seconds: bool = True,
    rate_limit: Optional[RateLimit] = None,
) -> pd.DataFrame:
    """Retrieve the TimeSeries of a single IOC station."""

    if rate_limit:
        while rate_limit.reached(identifier="IOC"):
            wait()

    # IOC needs timestamps in UTC but they must be in non-timezone-aware
    # I.e. it accepts `2023-05-02T12:00:00`
    # but rejects     `2023-05-02T12:00:00T00:00`
    # That's why we cast the endtime to a timezone-aware timestamp using UTC
    # and then we remove the timezone with tz_localize
    endtime = resolve_timestamp(endtime, timezone="UTC", timezone_aware=False).tz_localize(tz=None)

    url = IOC_BASE_URL.format(ioc_code=ioc_code, endtime=endtime.isoformat(), period=period)
    logger.info("%s: Retrieving data from: %s", ioc_code, url)
    try:
        df = pd.read_html(url, header=0)[0]
    except ValueError as exc:
        if str(exc) == "No tables found":
            logger.info("%s: No data", ioc_code)
        else:
            logger.exception("%s: Something went wrong", ioc_code)
        raise
    df = normalize_ioc_station_data(ioc_code=ioc_code, df=df, truncate_seconds=truncate_seconds)
    return df


@deprecated(
    version="0.3.11",
    reason="This function is deprecated and will be removed in the future. Replace it with `fetch_ioc_station`.",
)
def get_ioc_data(
    ioc_metadata: pd.DataFrame,
    endtime: DateTimeLike = NOW,
    period: float = 1,  # one day
    truncate_seconds: bool = True,
    rate_limit: RateLimit = RateLimit(),
    disable_progress_bar: bool = False,
) -> xr.Dataset:
    """
    Return the data of the stations specified in ``ioc_metadata`` as an ``xr.Dataset``.

    ``truncate_seconds`` needs some explaining. IOC has more than 1000 stations.
    When you retrieve data from all (or at least most of) these stations, you
    end up with thousands of timestamps that only contain a single datapoint.
    This means that the returned ``xr.Dataset`` will contain a huge number of ``NaN``
    which means that you will need a huge amount of RAM.

    In order to reduce the amount of the required RAM we reduce the number of timestamps
    by truncating the seconds. This is how this works:

        2014-01-03 14:53:02 -> 2014-01-03 14:53:00
        2014-01-03 14:53:32 -> 2014-01-03 14:53:00
        2014-01-03 14:53:48 -> 2014-01-03 14:53:00
        2014-01-03 14:54:09 -> 2014-01-03 14:54:00
        2014-01-03 14:54:48 -> 2014-01-03 14:54:00

    Nevertheless this approach has a downside. If a station returns multiple datapoints
    within the same minute, then we end up with duplicate timestamps. When this happens
    we only keep the first datapoint and drop the subsequent ones. So potentially you
    may not retrieve all of the available data.

    If you don't want this behavior, set ``truncate_seconds`` to ``False`` and you
    will retrieve the full data.

    :param ioc_metadata: A ``pd.DataFrame`` returned by ``get_ioc_stations``
    :param endtime: The date of the "end" of the data. Defaults to ``datetime.date.today()``
    :param period: The number of days to be requested. IOC does not support values greater than 30
    :param truncate_seconds: If ``True`` then timestamps are truncated to minutes (seconds are dropped)
    :param rate_limit: The default rate limit is 5 requests/second.
    :param disable_progress_bar: If ``True`` then the progress bar is not displayed.

    """
    if period > IOC_MAX_DAYS_PER_REQUEST:
        msg = (
            f"Unsupported period. Please choose a period smaller than {IOC_MAX_DAYS_PER_REQUEST}: {period}"
        )
        raise ValueError(msg)

    func_kwargs = []
    for ioc_code in ioc_metadata.ioc_code:
        func_kwargs.append(
            dict(
                period=period,
                endtime=endtime,
                ioc_code=ioc_code,
                rate_limit=rate_limit,
                truncate_seconds=truncate_seconds,
            ),
        )

    results = multithread(
        func=get_ioc_station_data,
        func_kwargs=func_kwargs,
        n_workers=5,
        print_exceptions=False,
        disable_progress_bar=disable_progress_bar,
    )

    datasets = []
    for result in results:
        if result.result is not None:
            df = result.result
            meta = ioc_metadata[ioc_metadata.ioc_code == result.kwargs["ioc_code"]]  # type: ignore[index]
            ds = df.set_index(["ioc_code", "time"]).to_xarray()
            ds["lon"] = ("ioc_code", meta.lon)
            ds["lat"] = ("ioc_code", meta.lat)
            ds["country"] = ("ioc_code", meta.country)
            ds["location"] = ("ioc_code", meta.location)
            datasets.append(ds)

    # in order to keep memory consumption low, let's group the datasets
    # and merge them in batches
    while len(datasets) > 5:
        datasets = merge_datasets(datasets)
    # Do the final merging
    ds = xr.merge(datasets)
    return ds


############## API ################


BASE_URL = "https://www.ioc-sealevelmonitoring.org/service.php?query=data&timestart={timestart}&timestop={timestop}&code={ioc_code}"

IOC_URL_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
IOC_JSON_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _before_sleep(retry_state: T.Any) -> None:  # pragma: no cover
    logger.warning(
        "Retrying %s: attempt %s ended with: %s",
        retry_state.fn,
        retry_state.attempt_number,
        retry_state.outcome,
    )


RETRY: T.Callable[..., T.Any] = tenacity.retry(
    stop=(tenacity.stop_after_delay(90) | tenacity.stop_after_attempt(10)),
    wait=tenacity.wait_random(min=2, max=10),
    retry=tenacity.retry_if_exception_type(httpx.TransportError),
    before_sleep=_before_sleep,
)


def _fetch_url(
    url: str,
    client: httpx.Client,
) -> str:
    try:
        response = client.get(url)
    except Exception:
        logger.warning("Failed to retrieve: %s", url)
        raise
    data = response.text
    return data


@RETRY
def fetch_url(
    url: str,
    client: httpx.Client,
    rate_limit: multifutures.RateLimit | None = None,
    **kwargs: T.Any,
) -> str:
    if rate_limit is not None:  # pragma: no cover
        while rate_limit.reached():
            multifutures.wait()  # pragma: no cover
    return _fetch_url(
        url=url,
        client=client,
    )


def _parse_ioc_responses(
    ioc_responses: list[multifutures.FutureResult],
    executor: multifutures.ExecutorProtocol | None,
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
    results = multifutures.multiprocess(_parse_json, func_kwargs=kwargs, check=False, executor=executor)
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
    # Occasionaly IOC contains complete garbage. E.g. duplicate timestamps on the same sensor. We should drop those.
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
            func=fetch_url, func_kwargs=kwargs, check=False, executor=executor
        )
        logger.debug("Finished data retrieval")
    multifutures.check_results(results)
    return results


def _resolve_rate_limit(rate_limit: multifutures.RateLimit | None) -> multifutures.RateLimit:
    if rate_limit is None:
        rate_limit = multifutures.RateLimit(rate_limit=limits.parse("5/second"))
    return rate_limit


def _resolve_http_client(http_client: httpx.Client | None) -> httpx.Client:
    if http_client is None:
        timeout = httpx.Timeout(timeout=10, read=30)
        http_client = httpx.Client(timeout=timeout)
    return http_client


def _fetch_ioc(
    station_ids: abc.Collection[str],
    start_dates: pd.DatetimeIndex,
    end_dates: pd.DatetimeIndex,
    *,
    rate_limit: multifutures.RateLimit | None,
    http_client: httpx.Client | None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None,
    multithreading_executor: multifutures.ExecutorProtocol | None,
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
    )
    # Parse the json files using pandas
    # This is a CPU heavy process, so we are using multiprocessing here
    parsed_responses: list[multifutures.FutureResult] = _parse_ioc_responses(
        ioc_responses=ioc_responses,
        executor=multiprocessing_executor,
    )
    # OK, now we have a list of dataframes. We need to group them per ioc_code, concatenate them and remove duplicates
    dataframes = _group_results(station_ids=station_ids, parsed_responses=parsed_responses)
    return dataframes


def _to_utc(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    if index.tz:
        index = index.tz_convert("utc")
    else:
        index = index.tz_localize("utc")
    return index


def _to_datetime_index(ts: pd.Timestamp) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex([ts])
    return index


def _resolve_start_date(now: pd.Timestamp, start_date: DatetimeLike | None) -> pd.DatetimeIndex:
    if start_date is None:
        resolved_start_date = T.cast(pd.Timestamp, now - pd.Timedelta(days=7))
    else:
        resolved_start_date = pd.to_datetime(start_date)
    index = _to_datetime_index(resolved_start_date)
    return index


def _resolve_end_date(now: pd.Timestamp, end_date: DatetimeLike | None) -> pd.DatetimeIndex:
    if end_date is None:
        resolved_end_date = now
    else:
        resolved_end_date = pd.to_datetime(end_date)
    index = _to_datetime_index(resolved_end_date)
    return index


def fetch_ioc_station(
    station_id: str,
    start_date: DatetimeLike | None = None,
    end_date: DatetimeLike | None = None,
    *,
    rate_limit: multifutures.RateLimit | None = None,
    http_client: httpx.Client | None = None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
) -> pd.DataFrame:
    """
    Make a query to the IOC API for tide gauge data for ``station_id``
    and return the results as a ``pandas.Dataframe``.

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
    :param http_client: The ``httpx.Client``.
    :param multiprocessing_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
    :param multithreading_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
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
    )[station_id]
    logger.info("IOC-%s: Finished scraping: %s - %s", station_id, start_date, end_date)
    return df
