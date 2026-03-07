# `dataretrieval` package developed by USGS is used for the USGS stations: https://usgs-python.github.io/dataretrieval/
#
# This module uses the modernized Water Data API via dataretrieval.waterdata
# See: https://waterdata.usgs.gov/blog/api_catalog/
from __future__ import annotations

import datetime
import functools
import logging
import os
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import geopandas as gpd
import limits
import pandas as pd
import xarray as xr
from dataretrieval import waterdata
from dataretrieval.codes import fips_codes as _DATARETRIEVAL_FIPS_CODES
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from .custom_types import DateTimeLike
from .multi import multithread
from .rate_limit import RateLimit
from .rate_limit import wait
from .utils import get_region
from .utils import NOW
from .utils import resolve_timestamp

logger = logging.getLogger(__name__)

# The modernized Water Data API requires FIPS numeric state codes (e.g., "24"
# for Maryland), not the two-letter abbreviations accepted by the legacy NWIS API.
if isinstance(_DATARETRIEVAL_FIPS_CODES, list):
    STATE_CODES = sorted(_DATARETRIEVAL_FIPS_CODES)
else:
    STATE_CODES = sorted(_DATARETRIEVAL_FIPS_CODES.values())

# Parameter codes of interest for water level/elevation data
# See: https://help.waterdata.usgs.gov/codes-and-parameters/parameters
USGS_PARAMETER_CODES = {
    "00060": {"name": "Discharge, cubic feet per second", "unit": "ft3/s"},
    "00065": {"name": "Gage height, feet", "unit": "ft"},
    "62614": {"name": "Lake or reservoir water surface elevation above NGVD 1929, feet", "unit": "ft"},
    "62615": {"name": "Lake or reservoir water surface elevation above NAVD 1988, feet", "unit": "ft"},
    "62620": {"name": "Estuary or ocean water surface elevation above NAVD 1988, feet", "unit": "ft"},
    "63158": {"name": "Stream water level elevation above NGVD 1929, in feet", "unit": "ft"},
    "63160": {"name": "Stream water level elevation above NAVD 1988, in feet", "unit": "ft"},
}

# Parameter codes grouped by variable type for availability tracking
# Water level codes (gage height, elevation datums)
USGS_WATER_LEVEL_CODES = {
    "00065",  # Gage height, feet
    "62614",  # Lake/reservoir elevation above NGVD 1929
    "62615",  # Lake/reservoir elevation above NAVD 1988
    "62616",  # Lake/reservoir elevation above local datum
    "62617",  # Reservoir water surface elevation above NGVD
    "62620",  # Estuary/ocean elevation above NAVD 1988
    "62622",  # Reservoir water surface elevation above datum
    "63158",  # Stream water level above NGVD 1929
    "63160",  # Stream water level above NAVD 1988
    "63161",  # Reservoir water surface elevation above NGVD 1929
    "72214",  # Reservoir water surface elevation above IGLD 1985
    "72215",  # Reservoir water surface elevation above IGLD 1985
    "72279",  # Water level, above NAVD88
}

# Water temperature codes
USGS_TEMPERATURE_CODES = {
    "00010",  # Temperature, water, degrees Celsius
    "00011",  # Temperature, water, degrees Fahrenheit
    "99976",  # Temperature, water, degrees Celsius
    "99980",  # Temperature, water, degrees Celsius
    "99984",  # Temperature, water, degrees Celsius
}

# Salinity codes (all essentially equivalent units)
USGS_SALINITY_CODES = {
    "00095",  # Specific conductance (related to salinity)
    "00480",  # Salinity, water, parts per thousand
    "00096",  # Salinity, water, mg/ml
    "70305",  # Salinity, water, g/l
    "72401",  # Salinity, water, PSU
    "90860",  # Salinity, water, PSU
    "90862",  # Salinity, water, PSS
}

# Current/velocity codes
USGS_CURRENT_CODES = {
    "00055",  # Stream velocity, ft/s
    "72168",  # Stream velocity, ft/s
    "72254",  # Mean velocity of water in stream, ft/s
    "72255",  # Stream velocity, ft/s
    "72294",  # Stream velocity, mph
    "72321",  # Stream velocity, mph
    "72322",  # Stream velocity, ft/s
    "70232",  # Stream velocity, knots
    "81904",  # Stream velocity, ft/s
}

# TODO: Should qualifier be a part of the index?
USGS_DATA_MULTIIDX = ("site_no", "datetime", "code", "option")

# Column names expected in station metadata
USGS_STATIONS_COLUMN_NAMES = [
    "site_no",
    "station_nm",
    "dec_lat_va",
    "dec_long_va",
    "dec_coord_datum_cd",
    "alt_va",
    "alt_datum_cd",
]

# API Key configuration
USGS_API_KEY_ENV_VAR = "API_USGS_PAT"

# Rate limits for modernized Water Data API
USGS_WATERDATA_RATE_LIMIT_NO_KEY = limits.parse("50/hour")
USGS_WATERDATA_RATE_LIMIT_WITH_KEY = limits.parse("1000/hour")

# Monitoring location ID prefix
USGS_MONITORING_LOCATION_PREFIX = "USGS-"

# Track if we've already warned about missing API key
_warned_no_api_key = False


def get_usgs_api_key(api_key: Optional[str] = None) -> Optional[str]:
    """Get USGS API key from parameter or API_USGS_PAT environment variable."""
    if api_key is not None:
        return api_key
    return os.environ.get(USGS_API_KEY_ENV_VAR)


def _set_api_key_env(api_key: Optional[str] = None) -> None:
    """Set API key in environment for dataretrieval library.

    The dataretrieval library reads the API key from the API_USGS_PAT
    environment variable. This function sets it if a key is available.

    Note: This modifies global state (os.environ). In multi-threaded code,
    ensure api_key is set before spawning threads, or set the environment
    variable externally before running.
    """
    key = get_usgs_api_key(api_key)
    if key:
        os.environ[USGS_API_KEY_ENV_VAR] = key


def get_usgs_rate_limit(api_key: Optional[str] = None) -> RateLimit:
    """Get rate limit based on API key availability."""
    global _warned_no_api_key

    key = get_usgs_api_key(api_key)
    if key:
        logger.info("USGS API key detected - using 1000 requests/hour limit")
        return RateLimit(rate_limit=USGS_WATERDATA_RATE_LIMIT_WITH_KEY)
    else:
        if not _warned_no_api_key:
            logger.warning(
                f"No USGS API key. Limited to 50 requests/hour. "
                f"Set {USGS_API_KEY_ENV_VAR} env var for higher limits."
            )
            _warned_no_api_key = True
        return RateLimit(rate_limit=USGS_WATERDATA_RATE_LIMIT_NO_KEY)


def site_no_to_monitoring_location_id(site_no: str) -> str:
    """Convert '01646500' to 'USGS-01646500' format."""
    if site_no.startswith(USGS_MONITORING_LOCATION_PREFIX):
        return site_no
    return f"{USGS_MONITORING_LOCATION_PREFIX}{site_no}"


def monitoring_location_id_to_site_no(monitoring_location_id: str) -> str:
    """Convert 'USGS-01646500' to '01646500' format."""
    if monitoring_location_id.startswith(USGS_MONITORING_LOCATION_PREFIX):
        return monitoring_location_id[len(USGS_MONITORING_LOCATION_PREFIX) :]
    return monitoring_location_id


def format_time_range(starttime: datetime.date, endtime: datetime.date) -> str:
    """Format as 'YYYY-MM-DD/YYYY-MM-DD' for waterdata API."""
    return f"{starttime.isoformat()}/{endtime.isoformat()}"


@functools.lru_cache(maxsize=None)
def _get_usgs_parameter_info() -> pd.DataFrame:
    """Return DataFrame with parameter code information."""
    data = [
        {"parameter_cd": code, "parm_nm": info["name"], "parm_unit": info["unit"]}
        for code, info in USGS_PARAMETER_CODES.items()
    ]
    return pd.DataFrame(data)


def _get_usgs_stations_by_state(
    state_code: str,
    api_key: Optional[str] = None,
    rate_limit: Optional[RateLimit] = None,
) -> pd.DataFrame:
    """Get monitoring locations for a state using waterdata API."""
    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    _set_api_key_env(api_key)

    try:
        sites, _ = waterdata.get_monitoring_locations(
            state_code=[state_code],
            skip_geometry=False,
        )
        if not sites.empty:
            sites["us_state"] = state_code
        return sites
    except Exception as e:
        logger.warning(f"Failed to get stations for state {state_code}: {e}")
        return pd.DataFrame()


def normalize_usgs_stations(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Normalize station data from waterdata API to GeoDataFrame."""
    if df.empty:
        return gpd.GeoDataFrame()

    df = df.copy()

    # Extract site_no from monitoring_location_id (e.g., "USGS-01646500" -> "01646500")
    if "monitoring_location_id" in df.columns:
        df["site_no"] = df["monitoring_location_id"].apply(monitoring_location_id_to_site_no)

    # Rename columns to match expected format
    column_mapping = {
        "monitoring_location_name": "station_nm",
        "latitude": "dec_lat_va",
        "longitude": "dec_long_va",
        "horizontal_datum": "dec_coord_datum_cd",
        "altitude": "alt_va",
        "vertical_datum": "alt_datum_cd",
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Create GeoDataFrame
    if "dec_lat_va" in df.columns and "dec_long_va" in df.columns:
        gdf = gpd.GeoDataFrame(
            data=df,
            geometry=gpd.points_from_xy(df.dec_long_va, df.dec_lat_va, crs="EPSG:4326"),
        )
    else:
        gdf = gpd.GeoDataFrame(df)

    return gdf


def get_station_parameter_availability(
    site_nos: list[str],
    api_key: Optional[str] = None,
    rate_limit: Optional[RateLimit] = None,
) -> pd.DataFrame:
    """
    Query which parameters are available at each station.

    Uses the modernized Water Data API's time-series-metadata endpoint
    to determine which parameter codes are available at each station.

    :param site_nos: List of USGS site numbers (e.g., ['01646500', '01647000'])
    :param api_key: USGS API key for higher rate limits
    :param rate_limit: Rate limit to apply
    :return: DataFrame with columns: site_no, has_water_level, has_temperature,
             has_salinity, has_currents
    """
    if rate_limit is None:
        rate_limit = get_usgs_rate_limit(api_key=api_key)

    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    _set_api_key_env(api_key)

    # Convert site numbers to monitoring location IDs
    monitoring_location_ids = [site_no_to_monitoring_location_id(s) for s in site_nos]

    try:
        df, _ = waterdata.get_time_series_metadata(
            monitoring_location_id=monitoring_location_ids,
        )
    except Exception as e:
        logger.warning(f"Failed to get parameter availability: {e}")
        # Return DataFrame with all False values
        return pd.DataFrame(
            {
                "site_no": site_nos,
                "has_water_level": False,
                "has_temperature": False,
                "has_salinity": False,
                "has_currents": False,
            }
        )

    if df.empty:
        return pd.DataFrame(
            {
                "site_no": site_nos,
                "has_water_level": False,
                "has_temperature": False,
                "has_salinity": False,
                "has_currents": False,
            }
        )

    # Extract site_no from monitoring_location_id
    if "monitoring_location_id" in df.columns:
        df["site_no"] = df["monitoring_location_id"].apply(monitoring_location_id_to_site_no)

    # Group by site and get unique parameter codes
    availability_records = []
    for site_no in site_nos:
        site_params = set()
        if "site_no" in df.columns and "parameter_code" in df.columns:
            site_df = df[df["site_no"] == site_no]
            site_params = set(site_df["parameter_code"].dropna().astype(str))

        availability_records.append(
            {
                "site_no": site_no,
                "has_water_level": bool(site_params & USGS_WATER_LEVEL_CODES),
                "has_temperature": bool(site_params & USGS_TEMPERATURE_CODES),
                "has_salinity": bool(site_params & USGS_SALINITY_CODES),
                "has_currents": bool(site_params & USGS_CURRENT_CODES),
            }
        )

    return pd.DataFrame(availability_records)


@functools.lru_cache(maxsize=None)
def _get_all_usgs_stations(normalize: bool = True) -> gpd.GeoDataFrame:
    """
    Return USGS station metadata for all stations in all the states.

    Uses the modernized waterdata API.

    :return: ``geopandas.GeoDataFrame`` with the station metadata
    """
    rate_limit = get_usgs_rate_limit()

    func_kwargs = [{"state_code": st, "rate_limit": rate_limit} for st in STATE_CODES]

    results = multithread(
        func=_get_usgs_stations_by_state,
        func_kwargs=func_kwargs,
        n_workers=5,
        print_exceptions=False,
        disable_progress_bar=False,
    )

    dfs = [r.result for r in results if r.result is not None and not r.result.empty]

    if not dfs:
        return gpd.GeoDataFrame()

    usgs_stations = pd.concat(dfs, ignore_index=True)

    if normalize:
        usgs_stations = normalize_usgs_stations(usgs_stations)

    return usgs_stations


def get_usgs_stations(
    region: Optional[Union[Polygon, MultiPolygon]] = None,
    lon_min: Optional[float] = None,
    lon_max: Optional[float] = None,
    lat_min: Optional[float] = None,
    lat_max: Optional[float] = None,
    include_parameter_availability: bool = False,
    api_key: Optional[str] = None,
) -> gpd.GeoDataFrame:
    """
    Return USGS station metadata using the modernized Water Data API.

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
    :param include_parameter_availability: If True, query which parameters
        (water_level, temperature, salinity, currents) are available at each
        station. Adds columns: has_water_level, has_temperature, has_salinity,
        has_currents. This requires additional API calls. Default False.
    :param api_key: USGS API key for higher rate limits when querying
        parameter availability.
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

    if include_parameter_availability and not usgs_stations.empty:
        site_nos = usgs_stations["site_no"].tolist()
        availability_df = get_station_parameter_availability(
            site_nos=site_nos,
            api_key=api_key,
        )
        usgs_stations = usgs_stations.merge(
            availability_df,
            on="site_no",
            how="left",
        )
        # Fill NaN values with False for availability columns
        for col in ["has_water_level", "has_temperature", "has_salinity", "has_currents"]:
            if col in usgs_stations.columns:
                usgs_stations[col] = usgs_stations[col].fillna(False)

    return usgs_stations


def _get_usgs_station_data(
    usgs_code: str,
    starttime: datetime.date,
    endtime: datetime.date,
    api_key: Optional[str] = None,
    parameter_code: Optional[str] = None,
) -> pd.DataFrame:
    """Get station data using waterdata.get_continuous().

    Returns instantaneous values (typically 15-minute intervals).
    """
    _set_api_key_env(api_key)

    monitoring_location_id = site_no_to_monitoring_location_id(usgs_code)
    time_range = format_time_range(starttime, endtime)

    kwargs: Dict[str, Any] = {
        "monitoring_location_id": monitoring_location_id,
        "time": time_range,
    }
    if parameter_code:
        kwargs["parameter_code"] = parameter_code

    try:
        df, _ = waterdata.get_continuous(**kwargs)
    except Exception as e:
        logger.warning(f"Failed to get data for station {usgs_code}: {e}")
        return pd.DataFrame()

    return _normalize_station_data(df, usgs_code)


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename waterdata API columns to standard format."""
    rename_map = {}
    if "time" in df.columns and "datetime" not in df.columns:
        rename_map["time"] = "datetime"
    if "parameter_code" in df.columns:
        rename_map["parameter_code"] = "code"
    if rename_map:
        df = df.rename(columns=rename_map)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def _normalize_qualifier_column(df: pd.DataFrame) -> pd.DataFrame:
    """Handle qualifier column - prefer approval_status over existing qualifier."""
    if "approval_status" in df.columns:
        if "qualifier" in df.columns:
            df = df.drop(columns=["qualifier"])
        df = df.rename(columns={"approval_status": "qualifier"})
    elif "qualifier" not in df.columns:
        df["qualifier"] = None
    return df


def _add_parameter_info(df: pd.DataFrame) -> pd.DataFrame:
    """Add unit/name columns from parameter code lookup."""
    if "code" not in df.columns or df.empty:
        return df
    df_parm = _get_usgs_parameter_info().set_index("parameter_cd")
    known_codes = df.code.isin(df_parm.index)
    if not known_codes.any():
        return df
    df = df[known_codes].copy()
    df["unit"] = df_parm.loc[df.code.values, "parm_unit"].values
    df["name"] = df_parm.loc[df.code.values, "parm_nm"].values
    return df


def _normalize_station_data(
    df: pd.DataFrame,
    site_no: Optional[str] = None,
    set_index: bool = True,
) -> pd.DataFrame:
    """Normalize waterdata response to standard format.

    :param df: DataFrame from waterdata API
    :param site_no: Site number to assign. If None, extracts from monitoring_location_id.
    :param set_index: Whether to set the MultiIndex. Set False for further processing.
    """
    if df.empty:
        return df

    df = df.copy()

    # Handle site_no - either from parameter or extract from monitoring_location_id
    if site_no is not None:
        df["site_no"] = site_no
    elif "monitoring_location_id" in df.columns:
        df["site_no"] = df["monitoring_location_id"].apply(monitoring_location_id_to_site_no)

    df = _normalize_column_names(df)
    df = _normalize_qualifier_column(df)

    if "option" not in df.columns:
        df["option"] = ""

    df = _add_parameter_info(df)

    # Set MultiIndex
    if set_index:
        index_cols = [c for c in USGS_DATA_MULTIIDX if c in df.columns]
        if index_cols:
            df = df.set_index(index_cols)

    return df


def get_usgs_station_data(
    usgs_code: str,
    endtime: DateTimeLike = NOW,
    period: float = 30,
    rate_limit: Optional[RateLimit] = None,
    api_key: Optional[str] = None,
    parameter_code: Optional[str] = None,
) -> pd.DataFrame:
    """Retrieve instantaneous values for a single USGS station.

    Uses the modernized Water Data API which returns continuous data
    (typically 15-minute interval measurements).

    :param usgs_code: USGS station code a.k.a. "site number"
    :param endtime: The end date for the measurement data to fetch. Defaults to today.
    :param period: Number of days for which to fetch station data
    :param rate_limit: Rate limit to apply. If None, auto-configures based on API key.
    :param api_key: USGS API key for higher rate limits. Falls back to API_USGS_PAT env var.
    :param parameter_code: Optional parameter code to filter results.
    :return: ``pandas.DataFrame`` with the station measurements
    """
    if rate_limit is None:
        rate_limit = get_usgs_rate_limit(api_key=api_key)

    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    endtime_date = resolve_timestamp(endtime, timezone_aware=False).date()
    starttime = endtime_date - datetime.timedelta(days=period)

    df = _get_usgs_station_data(
        usgs_code=usgs_code,
        starttime=starttime,
        endtime=endtime_date,
        api_key=api_key,
        parameter_code=parameter_code,
    )

    return df


def _get_dataset_from_station_data(
    df: pd.DataFrame,
    usgs_metadata: pd.DataFrame,
) -> xr.Dataset:
    """Convert station DataFrame to xarray Dataset."""
    if df.empty:
        return xr.Dataset()

    df = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()

    # Filter to only site_nos that exist in metadata to avoid KeyError
    metadata_site_nos = set(usgs_metadata.site_no.unique())
    df_site_nos = set(df.site_no.unique())
    valid_site_nos = df_site_nos & metadata_site_nos

    if not valid_site_nos:
        logger.warning("No matching site_nos between data and metadata")
        return xr.Dataset()

    # Filter df to only valid site_nos
    df = df[df.site_no.isin(valid_site_nos)].copy()

    st_meta = (
        usgs_metadata.set_index("site_no")
        .reindex(list(valid_site_nos))
        .reset_index()
        .drop_duplicates(subset="site_no")
        .set_index("site_no")
    )

    # Get parameter metadata
    cols_to_check = ["code", "unit", "name"]
    available_cols = [c for c in cols_to_check if c in df.columns]
    if "code" in available_cols:
        pr_meta = df[available_cols].drop_duplicates().set_index("code")
    else:
        pr_meta = pd.DataFrame()

    # Keep only necessary columns for the dataset
    keep_cols = list(USGS_DATA_MULTIIDX) + ["value", "qualifier"]
    df_clean = df[[c for c in keep_cols if c in df.columns]].copy()

    # Build dataset
    index_cols = [c for c in USGS_DATA_MULTIIDX if c in df_clean.columns]
    if index_cols:
        df_indexed = df_clean.set_index(index_cols)
    else:
        df_indexed = df_clean

    # Remove duplicate indices before converting to xarray
    df_indexed = df_indexed[~df_indexed.index.duplicated(keep="first")]

    ds = df_indexed.to_xarray()

    if "datetime" in ds.dims:
        ds["datetime"] = pd.DatetimeIndex(ds["datetime"].values)

    # Add coordinates from metadata
    if "site_no" in ds.dims and "dec_long_va" in st_meta.columns:
        ds["lon"] = ("site_no", st_meta.loc[ds.site_no.values.tolist()].dec_long_va)
        ds["lat"] = ("site_no", st_meta.loc[ds.site_no.values.tolist()].dec_lat_va)

    if "code" in ds.dims and not pr_meta.empty:
        if "unit" in pr_meta.columns:
            ds["unit"] = ("code", pr_meta.loc[ds.code.values.tolist()].unit)
        if "name" in pr_meta.columns:
            ds["name"] = ("code", pr_meta.loc[ds.code.values.tolist()].name)

    return ds


def get_usgs_data(
    usgs_metadata: pd.DataFrame,
    endtime: DateTimeLike = NOW,
    period: float = 1,  # one day
    rate_limit: Optional[RateLimit] = None,
    api_key: Optional[str] = None,
) -> xr.Dataset:
    """
    Return the data of the stations specified in ``usgs_metadata`` as an ``xr.Dataset``.

    Uses the modernized Water Data API which returns continuous/instantaneous data
    (typically 15-minute interval measurements).

    :param usgs_metadata: A ``pd.DataFrame`` returned by ``get_usgs_stations``.
    :param endtime: The date of the "end" of the data. Defaults to today.
    :param period: The number of days to be requested.
    :param rate_limit: Rate limit to apply. If None, auto-configures based on API key.
    :param api_key: USGS API key for higher rate limits. Falls back to API_USGS_PAT env var.
    :return: ``xr.Dataset`` of station measurements
    """
    if rate_limit is None:
        rate_limit = get_usgs_rate_limit(api_key=api_key)

    if rate_limit:
        while rate_limit.reached(identifier="USGS"):
            wait()

    endtime_resolved = resolve_timestamp(endtime)
    starttime = endtime_resolved - datetime.timedelta(days=period)
    time_range = format_time_range(starttime.date(), endtime_resolved.date())

    usgs_sites = usgs_metadata.site_no.unique()

    # Build monitoring location IDs
    monitoring_location_ids = [site_no_to_monitoring_location_id(s) for s in usgs_sites]

    # Fetch data for all stations
    _set_api_key_env(api_key)

    try:
        df, _ = waterdata.get_continuous(
            monitoring_location_id=monitoring_location_ids,
            time=time_range,
        )
    except Exception as e:
        logger.warning(f"Failed to get data: {e}")
        return xr.Dataset()

    if df.empty:
        return xr.Dataset()

    # Normalize the data using shared function (don't set index yet for dataset conversion)
    df = _normalize_station_data(df, site_no=None, set_index=False)

    if df.empty:
        return xr.Dataset()

    # Convert to dataset
    ds = _get_dataset_from_station_data(df, usgs_metadata)

    return ds
