"""
For the IOC stations we parse their website_.

:: _website: http://www.ioc-sealevelmonitoring.org/list.php?showall=all

This page contains 3 tables:

- ``general``
- ``contacts``
- ``performance``

We parse all 3 of them and we merge them.
"""
from __future__ import annotations

import datetime
import functools
import logging
from typing import Optional
from typing import Union

import bs4
import geopandas as gpd
import html5lib  # noqa: F401 - imported but unused
import limits
import pandas as pd
import requests
import xarray as xr
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from .multi import multiprocess
from .multi import multithread
from .rate_limit import RateLimit
from .rate_limit import wait
from .utils import get_region
from .utils import grouper


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
    "pwl(m)": "pwl",
    "ra2(m)": "ra2",
    "rad(m)": "rad",
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
    df = pd.read_html(html)[0]
    logger.debug("Parsed table: %s", url)
    df.columns = IOC_STATIONS_COLUMN_NAMES[output]
    df = df.drop(columns="view")
    return df


def normalize_ioc_stations(df: pd.DataFrame) -> gpd.GeoDataFrame:
    df = df.assign(
        # fmt: off
        gloss_id=df.gloss_id.astype(pd.Int64Dtype()),
        country=df.country.astype("category"),
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


@functools.cache
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


def normalize_ioc_station_data(ioc_code: str, df: pd.DataFrame) -> pd.DataFrame:
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
        # Normalize timestamps to minutes: https://stackoverflow.com/a/14836359/592289
        # WARNING: the astype() truncates seconds. This can potentially lead to duplicates!
        time=pd.to_datetime(df.time).astype("datetime64[m]"),
    )
    if len(df) != len(df.time.drop_duplicates()):
        msg = f"{ioc_code}: The sampling ratio is less than 1 minute. Data have been lost"
        raise ValueError(msg)
    return df


def get_ioc_station_data(
    ioc_code: str,
    endtime: Union[str, datetime.date] = datetime.date.today(),
    period: float = IOC_MAX_DAYS_PER_REQUEST,
    rate_limit: Optional[RateLimit] = None,
) -> pd.DataFrame:
    """Retrieve the TimeSeries of a single IOC station."""

    if rate_limit:
        while rate_limit.reached(identifier="IOC"):
            wait()

    endtime = pd.to_datetime(endtime).date().isoformat()
    url = IOC_BASE_URL.format(ioc_code=ioc_code, endtime=endtime, period=period)
    logger.debug("Retrieving data from: %s", url)
    try:
        df = pd.read_html(url, header=0)[0]
    except ValueError as exc:
        if str(exc) == "No tables found":
            logger.info("No data for %s", ioc_code)
        else:
            logger.exception("Something went wrong")
        raise
    df = normalize_ioc_station_data(ioc_code=ioc_code, df=df)
    return df


def get_ioc_data(
    ioc_metadata: pd.DataFrame,
    endtime: Union[str, datetime.date] = datetime.date.today(),
    period: float = 1,  # one day
    rate_limit: RateLimit = RateLimit(),
    disable_progress_bar: bool = False,
) -> xr.Dataset:
    """
    Return the data of the stations specified in ``ioc_metadata`` as an ``xr.Dataset``.

    :param ioc_metadata: A ``pd.DataFrame`` returned by ``get_ioc_stations``.
    :param endtime: The date of the "end" of the data.
    :param period: The number of days to be requested. IOC does not support values greater than 30
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
            dict(period=period, endtime=endtime, ioc_code=ioc_code, rate_limit=rate_limit),
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
    datasets = [xr.merge(g for g in group if g) for group in grouper(datasets, 5)]
    if len(datasets) > 5:
        # There are quite a few stations left, let's do another grouping
        datasets = [xr.merge(g for g in group if g) for group in grouper(datasets, 5)]
    if len(datasets) > 5:
        # There are quite a few stations left, let's do another grouping
        datasets = [xr.merge(g for g in group if g) for group in grouper(datasets, 5)]
    # Do the final merging
    ds = xr.merge(datasets)
    return ds
