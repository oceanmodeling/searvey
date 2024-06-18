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

import functools
import io
import logging
import warnings
from typing import Optional
from typing import Union

import bs4
import geopandas as gpd
import html5lib  # noqa: F401  # imported but unused
import limits
import lxml  # noqa: F401  # imported but unused
import pandas as pd
import requests
import xarray as xr
from deprecated import deprecated
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from .custom_types import DateTimeLike
from .multi import multiprocess
from .multi import multithread
from .rate_limit import RateLimit
from .rate_limit import wait
from .utils import get_region
from .utils import merge_datasets
from .utils import NOW
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
    version="0.4.0",
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
    version="0.4.0",
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
