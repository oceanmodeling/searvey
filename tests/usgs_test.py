import datetime

import geopandas as gpd
import pandas as pd
import pytest
import xarray as xr
from shapely import geometry

from searvey import usgs


def test_get_usgs_stations_raises_when_latlon_area_than_25():
    with pytest.raises(ValueError) as exc:
        usgs.get_usgs_stations(
            lon_min=-80,
            lat_min=30,
            lon_max=-70,
            lat_max=40,
        )
    assert "cannot exceed 25" in str(exc)


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
    assert len(stations) > 242000
    # check that the DataFrame has the right columns
    assert set(stations.columns).issuperset(usgs.USGS_STATIONS_COLUMN_NAMES)


@pytest.mark.parametrize(
    "truncate_seconds,no_records",
    [
        pytest.param(True, 4, id="truncate_seconds=True"),
        pytest.param(False, 4, id="truncate_seconds=False"),
    ],
)
def test_get_usgs_station_data(truncate_seconds, no_records):
    """Truncate_seconds=False returns more datapoints compared to `=True`"""
    df = usgs.get_usgs_station_data(
        usgs_code="15484000",
        endtime=datetime.date(2022, 10, 1),
        period=1,
        truncate_seconds=truncate_seconds,
    )
    assert len(df) == no_records


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


@pytest.mark.parametrize(
    "truncate_seconds",
    [
        pytest.param(True, id="truncate_seconds=True"),
        pytest.param(False, id="truncate_seconds=False"),
    ],
)
def test_get_usgs_data(truncate_seconds):
    # in order to speed up the execution time of the test,
    # we don't retrieve the USGS metadata from the internet,
    # but we use a hardcoded dict instead
    usgs_metadata = _USGS_METADATA_MINIMAL
    ds = usgs.get_usgs_data(
        usgs_metadata=usgs_metadata,
        endtime="2022-09-29",
        period=2,
        truncate_seconds=truncate_seconds,
    )
    assert isinstance(ds, xr.Dataset)
    # TODO: Check if truncate_seconds does work
    assert len(ds.datetime) == 27
    assert len(ds.site_no) == len(usgs_metadata)
    # Check that usgs_code, lon, lat, country and location are not modified
    assert set(ds.site_no.values).issubset(usgs_metadata.site_no.values)
    assert set(ds.lon.values).issubset(usgs_metadata.dec_long_va.values)
    assert set(ds.lat.values).issubset(usgs_metadata.dec_lat_va.values)
    # assert set(ds.country.values).issubset(usgs_metadata.country.values)
    # assert set(ds.location.values).issubset(usgs_metadata.location.values)
    # Check that actual data has been retrieved
    assert ds.sel(site_no="15056500").value.mean() == pytest.approx(109.94, rel=1e-3)
    assert ds.sel(site_no="15484000").value.mean() == pytest.approx(637.84, rel=1e-3)


def test_get_usgs_station_data_by_string_enddate():
    df = usgs.get_usgs_station_data(
        usgs_code="15484000",
        endtime="2022-10-01",
        period=2,
        truncate_seconds=False,
    )

    assert len(df) == 4

    dates = pd.to_datetime(df.index.get_level_values("datetime").to_series().dt.date.unique())
    assert len(dates) == 2
    assert dates.day.tolist() == [29, 30]
    assert dates.month.tolist() == [9, 9]
    assert dates.year.tolist() == [2022, 2022]


def test_normalize_empty_stations_df():
    df = pd.DataFrame()
    gdf = usgs.normalize_usgs_stations(df)

    assert gdf.empty
    assert isinstance(gdf, gpd.GeoDataFrame)


def test_normalize_empty_data_df():
    df = pd.DataFrame()
    df2 = usgs.normalize_usgs_station_data(df, truncate_seconds=False)

    assert df2.empty
    assert isinstance(df2, pd.DataFrame)
