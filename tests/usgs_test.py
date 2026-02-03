import datetime

import geopandas as gpd
import pandas as pd
import pytest
import xarray as xr
from shapely import geometry

from searvey import usgs
from searvey.rate_limit import RateLimit


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


def test_usgs_metadata_has_state_info():
    stations = usgs.get_usgs_stations()
    assert "us_state" in stations.columns


def test_get_usgs_station_data():
    # Test retrieval of instantaneous values (continuous data)
    df = usgs.get_usgs_station_data(
        usgs_code="15484000",
        endtime=datetime.date(2022, 10, 1),
        period=1,
    )
    # Verify expected columns exist
    assert all(col in df.columns for col in ["value", "qualifier", "unit", "name"])
    assert isinstance(df.index, pd.MultiIndex)
    assert all(col in df.index.names for col in ["site_no", "datetime", "code", "option"])

    # Verify we got instantaneous data (multiple readings per day)
    times = df.index.get_level_values("datetime")
    assert len(times) > 10  # Instantaneous data should have many readings over 1 day

    # Verify time range is within expected bounds (use tz-aware timestamps)
    min_time = times.min()
    max_time = times.max()
    assert min_time >= pd.Timestamp("2022-09-30", tz="UTC")
    assert max_time <= pd.Timestamp("2022-10-02", tz="UTC")


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

    # Verify the dataset has expected structure
    if ds.data_vars:
        # Check dimensions
        assert all(col in ds.dims for col in ["site_no", "datetime", "code", "option"])
        # Check data variables
        assert "value" in ds.data_vars
        # Check coordinates
        if "site_no" in ds.dims and len(ds.site_no) > 0:
            assert "lon" in ds.coords or "lon" in ds.data_vars
            assert "lat" in ds.coords or "lat" in ds.data_vars
        # Check that site_nos match metadata
        assert set(ds.site_no.values).issubset(usgs_metadata.site_no.values)


def test_get_usgs_station_data_by_string_enddate():
    df = usgs.get_usgs_station_data(
        usgs_code="15484000",
        endtime="2022-10-01",
        period=2,
    )
    assert isinstance(df, pd.DataFrame)
    assert not df.empty, "Expected data for station 15484000"

    # Verify we got instantaneous data with multiple readings
    times = df.index.get_level_values("datetime")
    # Instantaneous data (15-min intervals) should have ~192 readings over 2 days
    assert len(times) > 50

    # Verify dates span the expected range
    dates = pd.to_datetime(times).normalize().unique()
    assert len(dates) >= 2  # Should have data across multiple days


def test_normalize_empty_stations_df():
    df = pd.DataFrame()
    gdf = usgs.normalize_usgs_stations(df)

    assert gdf.empty
    assert isinstance(gdf, gpd.GeoDataFrame)


def test_normalize_empty_data_df():
    df = pd.DataFrame()
    df2 = usgs._normalize_station_data(df, "12345678")

    assert df2.empty
    assert isinstance(df2, pd.DataFrame)


def test_request_nonexistant_data():
    # Create minimal metadata for non-existent stations
    sta = pd.DataFrame(
        {
            "site_no": ["373707086300703", "373707086300801", "373707086300802"],
            "dec_lat_va": [37.0, 37.0, 37.0],
            "dec_long_va": [-86.0, -86.0, -86.0],
        }
    )
    ds = usgs.get_usgs_data(
        usgs_metadata=sta,
        endtime="2015-07-04",
        period=1,
    )
    # Should return empty dataset or dataset with no data
    assert isinstance(ds, xr.Dataset)


class TestAPIKeyManagement:
    def test_get_api_key_from_param(self) -> None:
        key = usgs.get_usgs_api_key(api_key="test-key-123")
        assert key == "test-key-123"

    def test_get_api_key_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(usgs.USGS_API_KEY_ENV_VAR, "env-key-456")
        key = usgs.get_usgs_api_key()
        assert key == "env-key-456"

    def test_get_api_key_param_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(usgs.USGS_API_KEY_ENV_VAR, "env-key-456")
        key = usgs.get_usgs_api_key(api_key="param-key-789")
        assert key == "param-key-789"

    def test_get_api_key_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(usgs.USGS_API_KEY_ENV_VAR, raising=False)
        key = usgs.get_usgs_api_key()
        assert key is None


class TestIDConversion:
    def test_site_no_to_monitoring_id(self) -> None:
        result = usgs.site_no_to_monitoring_location_id("01646500")
        assert result == "USGS-01646500"

    def test_site_no_to_monitoring_id_already_prefixed(self) -> None:
        result = usgs.site_no_to_monitoring_location_id("USGS-01646500")
        assert result == "USGS-01646500"

    def test_monitoring_id_to_site_no(self) -> None:
        result = usgs.monitoring_location_id_to_site_no("USGS-01646500")
        assert result == "01646500"

    def test_monitoring_id_to_site_no_no_prefix(self) -> None:
        result = usgs.monitoring_location_id_to_site_no("01646500")
        assert result == "01646500"

    def test_format_time_range(self) -> None:
        start = datetime.date(2022, 1, 1)
        end = datetime.date(2022, 1, 31)
        result = usgs.format_time_range(start, end)
        assert result == "2022-01-01/2022-01-31"


class TestRateLimitConfiguration:
    def test_rate_limit_with_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        rate_limit = usgs.get_usgs_rate_limit(api_key="test-key")
        assert rate_limit is not None
        assert isinstance(rate_limit, RateLimit)

    def test_rate_limit_without_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(usgs.USGS_API_KEY_ENV_VAR, raising=False)
        rate_limit = usgs.get_usgs_rate_limit()
        assert rate_limit is not None
        assert isinstance(rate_limit, RateLimit)


class TestParameterInfo:
    def test_get_usgs_parameter_info(self) -> None:
        df = usgs._get_usgs_parameter_info()
        assert isinstance(df, pd.DataFrame)
        assert "parameter_cd" in df.columns
        assert "parm_nm" in df.columns
        assert "parm_unit" in df.columns
        assert len(df) == len(usgs.USGS_PARAMETER_CODES)

    def test_parameter_codes_defined(self) -> None:
        assert "00060" in usgs.USGS_PARAMETER_CODES
        assert "00065" in usgs.USGS_PARAMETER_CODES
        assert usgs.USGS_PARAMETER_CODES["00065"]["name"] == "Gage height, feet"
