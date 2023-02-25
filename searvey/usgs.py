"""
_`dataretrieval` package developed by USGS is used for the USGS stations

:: _`dataretrieval`: https://usgs-python.github.io/dataretrieval/

This pacakge is a think wrapper around NWIS _REST API:

:: _REST: https://waterservices.usgs.gov/rest/

We take the return values from `dataretrieval` to be the original data
"""
from __future__ import annotations

import datetime
import functools
import logging
import warnings
from itertools import product
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import geopandas as gpd
import limits
import numpy as np
import pandas as pd
import xarray as xr
from dataretrieval import nwis
from dataretrieval.codes import state_codes
from dataretrieval.utils import Metadata
from shapely.geometry import mapping
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon
from shapely.geometry import shape

from .multi import multiprocess
from .multi import multithread
from .rate_limit import RateLimit
from .rate_limit import wait
from .utils import get_region
from .utils import merge_datasets


logger = logging.getLogger(__name__)


# constants
USGS_OUTPUT_OF_INTEREST = ("elevation", "flow rate")
USGS_OUTPUT_TYPE = ("iv",)
USGS_RATE_LIMIT = limits.parse("5/second")
# TODO: Should qualifier be a part of the index?
USGS_DATA_MULTIIDX = ("site_no", "datetime", "code", "option")
USGS_STATIONS_COLUMN_NAMES = [
    "site_no",
    "station_nm",
    "dec_lat_va",
    "dec_long_va",
    "dec_coord_datum_cd",
    "alt_va",
    "alt_datum_cd",
    "begin_date",
    "end_date",
]


def _filter_parameter_codes(param_cd_df: pd.DataFrame) -> pd.DataFrame:
    # Should we filter based on units?
    param_cd_df = param_cd_df[(param_cd_df.group == "Physical")]

    return param_cd_df


@functools.lru_cache(maxsize=None)
def _get_usgs_output_info() -> pd.DataFrame:
    output_info = []
    for var in USGS_OUTPUT_OF_INTEREST:
        df_param_cd, _ = nwis.get_pmcodes(var)
        df_param_cd = _filter_parameter_codes(df_param_cd)
        df_param_cd["output_cat"] = var
        output_info.append(df_param_cd)
    df_param_info = pd.concat(output_info, axis="index", join="outer", ignore_index=True)
    return df_param_info


@functools.lru_cache(maxsize=None)
def _get_usgs_output_codes() -> Dict[str, pd.DataFrame]:
    output_codes = {}
    df_param_info = _get_usgs_output_info()
    for var in USGS_OUTPUT_OF_INTEREST:
        output_codes[var] = df_param_info[df_param_info["output_cat"] == var].parameter_cd.values.tolist()

    return output_codes


def _get_usgs_stations_by_output(output: List[str], **kwargs: Dict[str, Any]) -> pd.DataFrame:
    # NOTE: Why do we have so many combinations in df for a single station?
    sites, sites_md = nwis.get_info(seriesCatalogOutput=True, parameterCd=output, **kwargs)
    # NOTE metadata object cannot be pickled due to lambda func
    return sites


def normalize_usgs_stations(df: pd.DataFrame) -> gpd.GeoDataFrame:
    if df.empty:
        return gpd.GeoDataFrame()

    df.end_date = pd.to_datetime(df.end_date, errors="coerce")
    df.begin_date = pd.to_datetime(df.begin_date, errors="coerce")
    gdf = gpd.GeoDataFrame(
        data=df,
        geometry=gpd.points_from_xy(df.dec_long_va, df.dec_lat_va, crs="EPSG:4326"),
    )
    return gdf


@functools.lru_cache(maxsize=None)
def _get_all_usgs_stations() -> gpd.GeoDataFrame:
    """
    Return USGS station metadata for all stations in all the states

    :return: ``geopandas.GeoDataFrame`` with the station metadata
    """

    usgs_stations_results = multiprocess(
        func=_get_usgs_stations_by_output,
        func_kwargs=[
            {"stateCd": st, "output": out, "hasDataType": dtp}
            for st, out, dtp in product(
                state_codes,
                _get_usgs_output_codes().values(),
                USGS_OUTPUT_TYPE,
            )
        ],
    )

    usgs_stations = functools.reduce(
        # functools.partial(pd.merge, how='outer'),
        lambda i, j: pd.concat([i, j], ignore_index=True),
        (r.result for r in usgs_stations_results if r.result is not None and not r.result.empty),
    )
    usgs_stations = normalize_usgs_stations(usgs_stations)

    return usgs_stations


@functools.lru_cache(maxsize=None)
def _get_usgs_stations_by_region(**region_json: Any) -> gpd.GeoDataFrame:
    """
    Return USGS station metadata for all stations in the specified region

    :return: ``geopandas.GeoDataFrame`` with the station metadata
    """

    region = shape(region_json)

    bbox = list(region.bounds)
    if (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) > 25:
        raise ValueError("Product of lat range and lon range cannot exceed 25!")

    usgs_stations_results = multiprocess(
        func=_get_usgs_stations_by_output,
        func_kwargs=[
            {"bBox": bbox, "output": out, "hasDataType": dtp}
            for out, dtp in product(_get_usgs_output_codes().values(), USGS_OUTPUT_TYPE)
        ],
    )
    # TODO: Guard for reduce on empty total list of stations
    usgs_stations = functools.reduce(
        # functools.partial(pd.merge, how='outer'),
        lambda i, j: pd.concat([i, j], ignore_index=True),
        (r.result for r in usgs_stations_results if r.result is not None and not r.result.empty),
    )
    usgs_stations = normalize_usgs_stations(usgs_stations)

    usgs_stations = usgs_stations[usgs_stations.within(region)]

    return usgs_stations


def get_usgs_stations(
    region: Optional[Union[Polygon, MultiPolygon]] = None,
    lon_min: Optional[float] = None,
    lon_max: Optional[float] = None,
    lat_min: Optional[float] = None,
    lat_max: Optional[float] = None,
) -> gpd.GeoDataFrame:
    """
    Return USGS station metadata from: https://waterservices.usgs.gov/rest/Site-Service.html

    If `region` is defined then the stations that are outside of the region are
    filtered out. If the coordinates of the Bounding Box are defined then
    stations outside of the BBox are filtered out. If both ``region`` and the
    Bounding Box are defined, then an exception is raised.

    Note: The longitudes of the USGS stations are in the [-180, 180] range.

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

    if region:
        # NOTE: Polygon is unhashable and cannot be used for a
        # cached function input
        region_json = mapping(region)
        usgs_stations = _get_usgs_stations_by_region(**region_json)
    else:
        usgs_stations = _get_all_usgs_stations()

    return usgs_stations


def normalize_usgs_station_data(df: pd.DataFrame, truncate_seconds: bool) -> pd.DataFrame:
    # TODO: Does truncate seconds make sense for USGS?

    if df.empty:
        return df

    df = df.reset_index()
    df = df.melt(id_vars=["datetime", "site_no"], var_name="output_id")

    df["qualifier"] = df.value.where(df.output_id.str.contains("_cd"))
    df["value"] = df.value.where(~df.output_id.str.contains("_cd"))
    df = df.dropna(subset=["value", "qualifier"], how="all")

    df["code"] = df.output_id.str.split("_").str[0]
    df["option"] = df.output_id.str.removesuffix("_cd").str.split("_").str[1:].str.join("")
    df["isqual"] = df.output_id.str.contains("_cd")
    df["output_id"] = df.output_id.str.removesuffix("_cd")
    df = df.set_index(list(USGS_DATA_MULTIIDX))

    df = (
        pd.merge(
            df.drop(columns="qualifier")[~df.isqual],
            df.qualifier[df.isqual],
            left_index=True,
            right_index=True,
            how="left",
        )
        .drop_duplicates()
        .drop(columns=["output_id", "isqual"])
    )
    df = df.reset_index()

    df_parm = _get_usgs_output_info().set_index("parameter_cd")
    df = df[df.code.isin(df_parm.index)]
    df["unit"] = df_parm.parm_unit[df.code.values].values
    df["name"] = df_parm.parm_nm[df.code.values].values

    if truncate_seconds:
        # Truncate seconds from timestamps: https://stackoverflow.com/a/28783971/592289
        # WARNING: This can potentially lead to duplicates!
        df = df.assign(datetime=df.datetime.dt.floor("min"))
        if df.duplicated(subset=list(USGS_DATA_MULTIIDX)).any():
            # There are duplicates. Keep the first datapoint per minute.
            msg = "Duplicate timestamps have been detected after the truncation of seconds. Keeping the first datapoint per minute"
            warnings.warn(msg)
            df = df.drop_duplicates(subset=list(USGS_DATA_MULTIIDX)).reset_index(drop=True)

    df = df.set_index(list(USGS_DATA_MULTIIDX))

    return df


def get_usgs_station_data(
    usgs_code: str,
    endtime: Union[str, datetime.date] = datetime.date.today(),
    period: float = 30,
    truncate_seconds: bool = True,
    rate_limit: Optional[RateLimit] = RateLimit(),
) -> pd.DataFrame:
    """Retrieve the TimeSeries of a single USGS station.

    :param usgs_code: USGS station code a.k.a. "site number"
    :param endtime: The end date for the measurement data for fetch
    :param period: Number of date for which to fetch station data
    :param truncate_seconds: If ``True`` then timestamps are truncated to minutes (seconds are dropped)
    :param rate_limit: The default rate limit is 5 requests/second.
    :return: ``pandas.DataFrame`` with the a single station measurements
    """

    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    if isinstance(endtime, str):
        endtime = datetime.date.fromisoformat(endtime)
    starttime = endtime - datetime.timedelta(days=period)
    df_iv, _ = nwis.get_iv(sites=[usgs_code], start=starttime.isoformat(), end=endtime.isoformat())
    df_iv = normalize_usgs_station_data(df=df_iv, truncate_seconds=truncate_seconds)
    return df_iv


def _get_dataset_from_query_results(
    query_result: Tuple[pd.DataFrame, Metadata], usgs_metadata: pd.DataFrame, truncate_seconds: bool
) -> xr.Dataset:
    df_iv, _ = query_result
    df_iv = df_iv.reset_index()
    df_iv = normalize_usgs_station_data(df=df_iv, truncate_seconds=truncate_seconds)
    st_meta = (
        usgs_metadata.set_index("site_no")
        .loc[df_iv.reset_index().site_no.unique()]
        .reset_index()
        .drop_duplicates(subset="site_no")
        .set_index("site_no")
    )
    pr_meta = df_iv.reset_index()[["code", "unit", "name"]].drop_duplicates().set_index("code")
    ds = df_iv.drop(columns=["unit", "name"]).to_xarray()
    ds["datetime"] = pd.DatetimeIndex(ds["datetime"].values)
    ds["lon"] = ("site_no", st_meta.loc[ds.site_no.values.tolist()].dec_long_va)
    ds["lat"] = ("site_no", st_meta.loc[ds.site_no.values.tolist()].dec_lat_va)
    ds["unit"] = ("code", pr_meta.loc[ds.code.values.tolist()].unit)
    ds["name"] = ("code", pr_meta.loc[ds.code.values.tolist()].name)
    # ds["country"] = ("site_no", st_meta.country)
    # ds["location"] = ("site_no", st_meta.location)

    return ds


def get_usgs_data(
    usgs_metadata: pd.DataFrame,
    endtime: Union[str, datetime.date] = datetime.date.today(),
    period: float = 1,  # one day
    truncate_seconds: bool = True,
    rate_limit: RateLimit = RateLimit(),
    disable_progress_bar: bool = False,
) -> xr.Dataset:
    """
    Return the data of the stations specified in ``usgs_metadata`` as an ``xr.Dataset``.

    ``truncate_seconds`` needs some explaining. USGS has more than 1000 stations.
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

    :param usgs_metadata: A ``pd.DataFrame`` returned by ``get_usgs_stations``.
    :param endtime: The date of the "end" of the data.
    :param period: The number of days to be requested. USGS does not support values greater than 30
    :param truncate_seconds: If ``True`` then timestamps are truncated to minutes (seconds are dropped)
    :param rate_limit: The default rate limit is 5 requests/second.
    :param disable_progress_bar: If ``True`` then the progress bar is not displayed.
    :return: ``xr.Dataset`` of station measurements
    """
    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    if isinstance(endtime, str):
        endtime = datetime.date.fromisoformat(endtime)
    starttime = endtime - datetime.timedelta(days=period)

    func_kwargs = []
    usgs_sites = usgs_metadata.site_no.unique()
    chunk_size = 30
    n_chunks = len(usgs_sites) // chunk_size + 1
    for usgs_code_ary in np.array_split(usgs_sites, n_chunks):
        func_kwargs.append(
            dict(sites=usgs_code_ary.tolist(), start=starttime.isoformat(), end=endtime.isoformat()),
        )

    results = multithread(
        func=nwis.get_iv,
        func_kwargs=func_kwargs,
        n_workers=5,
        print_exceptions=False,
        disable_progress_bar=disable_progress_bar,
    )

    datasets = []
    for result in results:
        if result.result is None:
            # When the `get_iv` call on station results in exception
            # in `data_retrieval` due to non-existant data
            continue

        ds = _get_dataset_from_query_results(result.result, usgs_metadata, truncate_seconds)
        datasets.append(ds)

    # in order to keep memory consumption low, let's group the datasets
    # and merge them in batches
    while len(datasets) > 5:
        datasets = merge_datasets(datasets)
    # Do the final merging
    ds = xr.merge(datasets)
    return ds
