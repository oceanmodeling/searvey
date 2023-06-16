import datetime

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely import geometry

from searvey import usgs


def test_get_usgs_stations_works_when_latlon_area_larger_than_25():
    sta = usgs.get_usgs_stations(
        lon_min=-80,
        lat_min=30,
        lon_max=-70,
        lat_max=40,
    )
    assert isinstance(sta, gpd.GeoDataFrame)


def test_get_usgs_stations_raises_when_both_region_and_bbox_specified():
    lon_min = -76
    lat_min = 39
    lon_max = -70
    lat_max = 43
    geom = geometry.box(lon_min, lat_min, lon_max, lat_max)

    with pytest.raises(ValueError) as exc:
        usgs.get_usgs_stations(
            region=geom,
            lon_min=lon_min,
            lat_min=lat_min,
            lon_max=lon_max,
            lat_max=lat_max,
        )

    assert "`region` or the `BBox` corners, not both" in str(exc)


def test_get_usgs_stations_by_region_are_within_region():
    lon_min = -76
    lat_min = 39
    lon_max = -70
    lat_max = 43
    geom = geometry.box(lon_min, lat_min, lon_max, lat_max)
    sta = usgs.get_usgs_stations(
        region=geom,
    )
    assert sta.within(geom).all()


def test_get_usgs_stations_by_bbox_and_region_equivalence():
    lon_min = -76
    lat_min = 39
    lon_max = -70
    lat_max = 43
    geom = geometry.box(lon_min, lat_min, lon_max, lat_max)

    sta_1 = usgs.get_usgs_stations(
        region=geom,
    )
    sta_2 = usgs.get_usgs_stations(
        lon_min=lon_min,
        lat_min=lat_min,
        lon_max=lon_max,
        lat_max=lat_max,
    )

    assert sta_1.equals(sta_2)


def test_get_usgs_stations():
    stations = usgs.get_usgs_stations()
    assert isinstance(stations, pd.DataFrame)
    assert isinstance(stations, gpd.GeoDataFrame)
    # check that the DataFrame has the right columns
    assert set(stations.columns).issuperset(usgs.USGS_STATIONS_COLUMN_NAMES)
    # Check that the parsing of the dates has been done correctly
    assert stations.begin_date.dtype == "<M8[ns]"
    assert stations.end_date.dtype == "<M8[ns]"


def test_usgs_metadata_has_state_info():
    stations = usgs.get_usgs_stations()
    assert "us_state" in stations.columns


def test_get_usgs_station_data():
    # TODO: Parameterize the test for station, date and offset
    df = usgs.get_usgs_station_data(
        usgs_code="15484000",
        endtime=datetime.date(2022, 10, 1),
        period=1,
    )
    assert all(col in df.columns for col in ["value", "qualifier", "unit", "name"])
    assert isinstance(df.index, pd.MultiIndex)
    assert all(col in df.index.names for col in ["site_no", "datetime", "code", "option"])

    station_utc_offset = "-08:00"
    times = df.index.get_level_values("datetime")
    assert all(
        np.logical_and(
            f"2022-09-30 00:00:00{station_utc_offset}" <= times,
            times <= f"2022-10-02 00:00:00{station_utc_offset}",
        )
    )


_USGS_METADATA_MINIMAL = pd.DataFrame.from_dict(
    {
        "agency_cd": {174: "USGS", 864: "USGS"},
        "site_no": {174: "15056500", 864: "15484000"},
        "station_nm": {174: "CHILKAT R NR KLUKWAN AK", 864: "SALCHA R NR SALCHAKET AK"},
        "dec_lat_va": {174: 59.41494719, 864: 64.47152778},
        "dec_long_va": {174: -135.9310198, 864: -146.9280556},
        "dec_coord_datum_cd": {174: "NAD83", 864: "NAD83"},
        "alt_va": {174: 0.1, 864: 637.24},
        "alt_datum_cd": {174: "NAVD88", 864: "NAVD88"},
        "parm_cd": {174: "00060", 864: "00065"},
        "begin_date": {174: "1959-07-01", 864: "2007-10-01"},
        "end_date": {174: "2023-02-02", 864: "2023-02-03"},
    }
)


def test_get_usgs_data():
    # in order to speed up the execution time of the test,
    # we don't retrieve the USGS metadata from the internet,
    # but we use a hardcoded dict instead
    usgs_metadata = _USGS_METADATA_MINIMAL
    ds = usgs.get_usgs_data(
        usgs_metadata=usgs_metadata,
        endtime="2022-09-29",
        period=2,
    )
    assert isinstance(ds, xr.Dataset)
    assert len(ds.site_no) == len(usgs_metadata)
    # Check that usgs_code, lon, lat, country and location are not modified
    assert set(ds.site_no.values).issubset(usgs_metadata.site_no.values)
    assert set(ds.lon.values).issubset(usgs_metadata.dec_long_va.values)
    assert set(ds.lat.values).issubset(usgs_metadata.dec_lat_va.values)
    # assert set(ds.country.values).issubset(usgs_metadata.country.values)
    # assert set(ds.location.values).issubset(usgs_metadata.location.values)

    # Check that actual data has been retrieved
    assert ds.isel(site_no=0, code=0).value.size > 0
    assert ds.isel(site_no=0, code=0).value.notnull().any()

    assert ds.isel(site_no=1, code=0).value.size > 0
    assert ds.isel(site_no=1, code=0).value.notnull().any()

    assert all(col in ds.dims for col in ["site_no", "datetime", "code", "option"])
    assert all(col in ds.variables for col in ["value", "qualifier", "lon", "lat", "unit", "name"])


def test_get_usgs_station_data_by_string_enddate():
    df = usgs.get_usgs_station_data(
        usgs_code="15484000",
        endtime="2022-10-01",
        period=2,
    )

    station_utc_offset = -8

    dates = pd.to_datetime(
        df.index.get_level_values("datetime")
        .to_series()
        .reset_index(drop=True)
        .dt.tz_convert(datetime.timezone(datetime.timedelta(hours=station_utc_offset)))
        .dt.date.unique()
    )
    assert len(dates) == 3
    assert dates.day.tolist() == [29, 30, 1]
    assert dates.month.tolist() == [9, 9, 10]
    assert dates.year.tolist() == [2022, 2022, 2022]


def test_normalize_empty_stations_df():
    df = pd.DataFrame()
    gdf = usgs.normalize_usgs_stations(df)

    assert gdf.empty
    assert isinstance(gdf, gpd.GeoDataFrame)


def test_normalize_empty_data_df():
    df = pd.DataFrame()
    df2 = usgs.normalize_usgs_station_data(df)

    assert df2.empty
    assert isinstance(df2, pd.DataFrame)


def test_request_nonexistant_data():
    sta = usgs.get_usgs_stations()
    sta = sta[
        sta.site_no.isin(
            [
                "373707086300703",
                "373707086300801",
                "373707086300802",
            ]
        )
    ]
    ds = usgs.get_usgs_data(
        usgs_metadata=sta,
        endtime="2015-07-04",
        period=1,
    )
    assert len(ds) == 0
