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
import importlib.metadata
import logging
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
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from .custom_types import DateLike
from .multi import multiprocess
from .multi import multithread
from .rate_limit import RateLimit
from .rate_limit import wait
from .utils import get_region
from .utils import merge_datasets
from .utils import resolve_date
from .utils import TODAY


logger = logging.getLogger(__name__)

# This will stop working if pandas switches its versioning scheme to CalVer or something...
_PANDAS_MAJOR_VERSION = int(importlib.metadata.version("pandas").split(".")[0])

# constants
USGS_OUTPUT_OF_INTEREST = (
    "elevation",  # Overlaps some of the specific codes below
    "flow rate",  # Overlaps some of the specific codes below
    "00065",  # Gage height, feet
    "62614",  # Lake or reservoir water surface elevation above NGVD 1929, feet
    "62615",  # Lake or reservoir water surface elevation above NAVD 1988, feet
    "62620",  # Estuary or ocean water surface elevation above NAVD 1988, feet
    "63158",  # Stream water level elevation above NGVD 1929, in feet
    "63160",  # Stream water level elevation above NAVD 1988, in feet
)
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
    df_param_info = df_param_info.drop_duplicates(subset="parameter_cd")
    return df_param_info


@functools.lru_cache(maxsize=None)
def _get_usgs_output_codes() -> Dict[str, pd.DataFrame]:
    output_codes = {}
    df_param_info = _get_usgs_output_info()
    for var in USGS_OUTPUT_OF_INTEREST:
        output_codes[var] = df_param_info[df_param_info["output_cat"] == var].parameter_cd.values.tolist()

    return output_codes


def _get_usgs_stations_by_output(output: List[str], **kwargs: Any) -> pd.DataFrame:
    # NOTE: There are many combinations in df for a single station
    # if `seriesCatalogOutput` is `True`. This is due to different
    # begin and end date for each output
    sites, sites_md = nwis.get_info(seriesCatalogOutput=True, parameterCd=output, **kwargs)
    # NOTE metadata object cannot be pickled due to lambda func
    return sites


def _get_usgs_stations_by_state(output: List[str], stateCd: str, **kwargs: Any) -> pd.DataFrame:
    sites = _get_usgs_stations_by_output(output=output, stateCd=stateCd, **kwargs)
    sites["us_state"] = stateCd
    return sites


def normalize_usgs_stations(df: pd.DataFrame) -> gpd.GeoDataFrame:
    if df.empty:
        return gpd.GeoDataFrame()

    param_dict = _get_usgs_output_info().set_index("parameter_cd").to_dict()

    to_datetime_kwargs = dict(errors="coerce")
    if _PANDAS_MAJOR_VERSION >= 2:
        to_datetime_kwargs["format"] = "mixed"
    df.end_date = pd.to_datetime(df.end_date, **to_datetime_kwargs)
    df.begin_date = pd.to_datetime(df.begin_date, **to_datetime_kwargs)
    df["parm_nm"] = [param_dict["parm_nm"].get(i) for i in df.parm_cd.values]
    df["parm_unit"] = [param_dict["parm_unit"].get(i) for i in df.parm_cd.values]
    df = df.dropna(subset="parm_nm")
    # TODO: Should station duplicates (by site_no) be removed?
    gdf = gpd.GeoDataFrame(
        data=df,
        geometry=gpd.points_from_xy(df.dec_long_va, df.dec_lat_va, crs="EPSG:4326"),
    )
    return gdf


@functools.lru_cache(maxsize=None)
def _get_all_usgs_stations(normalize: bool = True) -> gpd.GeoDataFrame:
    """
    Return USGS station metadata for all stations in all the states

    :return: ``geopandas.GeoDataFrame`` with the station metadata
    """

    # NOTE: multiprocess does NOT keep order
    usgs_stations_results = multiprocess(
        func=_get_usgs_stations_by_state,
        func_kwargs=[
            {
                "stateCd": st,
                "output": set(j for i in _get_usgs_output_codes().values() for j in i),
                "hasDataType": dtp,
            }
            for st, dtp in product(
                state_codes,
                USGS_OUTPUT_TYPE,
            )
        ],
    )

    usgs_stations = functools.reduce(
        # functools.partial(pd.merge, how='outer'),
        lambda i, j: pd.concat([i, j], ignore_index=True),
        (r.result for r in usgs_stations_results if r.result is not None and not r.result.empty),
    )
    if normalize:
        usgs_stations = normalize_usgs_stations(usgs_stations)

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

    usgs_stations = _get_all_usgs_stations(normalize=True)
    if region:
        usgs_stations = usgs_stations[usgs_stations.within(region)]

    return usgs_stations


def normalize_usgs_station_data(df: pd.DataFrame) -> pd.DataFrame:
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

    df = df.set_index(list(USGS_DATA_MULTIIDX))

    return df


def get_usgs_station_data(
    usgs_code: str,
    endtime: DateLike = TODAY,
    period: float = 30,
    rate_limit: Optional[RateLimit] = RateLimit(),
) -> pd.DataFrame:
    """Retrieve the TimeSeries of a single USGS station.

    :param usgs_code: USGS station code a.k.a. "site number"
    :param endtime: The end date for the measurement data for fetch. Defaults to `datetime.date.today()`
    :param period: Number of date for which to fetch station data
    :param rate_limit: The default rate limit is 5 requests/second.
    :return: ``pandas.DataFrame`` with the a single station measurements
    """

    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    endtime = resolve_date(endtime)
    starttime = endtime - datetime.timedelta(days=period)
    df_iv, _ = nwis.get_iv(sites=[usgs_code], start=starttime.isoformat(), end=endtime.isoformat())
    df_iv = normalize_usgs_station_data(df=df_iv)
    return df_iv


def _get_dataset_from_query_results(
    query_result: Tuple[pd.DataFrame, Metadata],
    usgs_metadata: pd.DataFrame,
) -> xr.Dataset:
    df_iv, _ = query_result
    if len(df_iv) == 0:
        return xr.Dataset()

    df_iv = df_iv.reset_index()
    df_iv = normalize_usgs_station_data(df=df_iv)
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
    endtime: DateLike = TODAY,
    period: float = 1,  # one day
    rate_limit: RateLimit = RateLimit(),
    disable_progress_bar: bool = False,
) -> xr.Dataset:
    """
    Return the data of the stations specified in ``usgs_metadata`` as an ``xr.Dataset``.

    :param usgs_metadata: A ``pd.DataFrame`` returned by ``get_usgs_stations``.
    :param endtime: The date of the "end" of the data. Defaults to `datetime.date.today()`
    :param period: The number of days to be requested. USGS does not support values greater than 30
    :param rate_limit: The default rate limit is 5 requests/second.
    :param disable_progress_bar: If ``True`` then the progress bar is not displayed.
    :return: ``xr.Dataset`` of station measurements
    """
    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    endtime = resolve_date(endtime)
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

        ds = _get_dataset_from_query_results(result.result, usgs_metadata)
        if any((ds.count(ds.dims) > 0)[v] for v in ds.data_vars):
            datasets.append(ds)

    # in order to keep memory consumption low, let's group the datasets
    # and merge them in batches
    while len(datasets) > 5:
        datasets = merge_datasets(datasets)
    # Do the final merging
    ds = xr.merge(datasets)
    return ds
