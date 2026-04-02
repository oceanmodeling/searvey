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


@pytest.mark.vcr
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


@pytest.mark.vcr
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


@pytest.mark.vcr
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


@pytest.mark.vcr
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


class TestParameterCodeGroups:
    """Test parameter code group definitions."""

    def test_water_level_codes_defined(self) -> None:
        assert isinstance(usgs.USGS_WATER_LEVEL_CODES, set)
        assert "00065" in usgs.USGS_WATER_LEVEL_CODES  # Gage height
        assert "63160" in usgs.USGS_WATER_LEVEL_CODES  # Stream water level NAVD88
        assert len(usgs.USGS_WATER_LEVEL_CODES) > 0

    def test_temperature_codes_defined(self) -> None:
        assert isinstance(usgs.USGS_TEMPERATURE_CODES, set)
        assert "00010" in usgs.USGS_TEMPERATURE_CODES  # Temperature Celsius
        assert "00011" in usgs.USGS_TEMPERATURE_CODES  # Temperature Fahrenheit
        assert len(usgs.USGS_TEMPERATURE_CODES) > 0

    def test_salinity_codes_defined(self) -> None:
        assert isinstance(usgs.USGS_SALINITY_CODES, set)
        assert "00480" in usgs.USGS_SALINITY_CODES  # Salinity ppth
        assert len(usgs.USGS_SALINITY_CODES) > 0

    def test_current_codes_defined(self) -> None:
        assert isinstance(usgs.USGS_CURRENT_CODES, set)
        assert "00055" in usgs.USGS_CURRENT_CODES  # Stream velocity
        assert "72255" in usgs.USGS_CURRENT_CODES  # Stream velocity
        assert len(usgs.USGS_CURRENT_CODES) > 0

    def test_code_groups_are_disjoint(self) -> None:
        """Verify parameter code groups don't overlap (except salinity/conductance)."""
        # Water level and temperature should not overlap
        assert not usgs.USGS_WATER_LEVEL_CODES & usgs.USGS_TEMPERATURE_CODES
        # Water level and currents should not overlap
        assert not usgs.USGS_WATER_LEVEL_CODES & usgs.USGS_CURRENT_CODES
        # Temperature and currents should not overlap
        assert not usgs.USGS_TEMPERATURE_CODES & usgs.USGS_CURRENT_CODES


class TestNormalizeHelpers:
    """Tests for internal normalization helper functions (no network calls)."""

    def test_normalize_column_names_no_renames_needed(self) -> None:
        """Cover false branches when columns don't need renaming."""
        df = pd.DataFrame({"value": [1.0, 2.0], "other": ["a", "b"]})
        result = usgs._normalize_column_names(df)
        assert list(result.columns) == ["value", "other"]

    def test_normalize_column_names_with_parameter_code(self) -> None:
        """Cover parameter_code -> code rename."""
        df = pd.DataFrame(
            {
                "time": ["2022-01-01", "2022-01-02"],
                "parameter_code": ["00065", "00060"],
                "value": [1.0, 2.0],
            }
        )
        result = usgs._normalize_column_names(df)
        assert "code" in result.columns
        assert "datetime" in result.columns
        assert "parameter_code" not in result.columns

    def test_normalize_qualifier_no_qualifier_no_approval(self) -> None:
        """Cover branch where neither qualifier nor approval_status exists."""
        df = pd.DataFrame({"value": [1.0, 2.0]})
        result = usgs._normalize_qualifier_column(df)
        assert "qualifier" in result.columns
        assert result["qualifier"].isna().all()

    def test_normalize_qualifier_with_approval_and_qualifier(self) -> None:
        """Cover branch where both approval_status and qualifier exist."""
        df = pd.DataFrame(
            {
                "value": [1.0],
                "approval_status": ["Approved"],
                "qualifier": ["old"],
            }
        )
        result = usgs._normalize_qualifier_column(df)
        assert "qualifier" in result.columns
        assert result["qualifier"].iloc[0] == "Approved"

    def test_add_parameter_info_no_code_column(self) -> None:
        """Cover early return when 'code' column is missing."""
        df = pd.DataFrame({"value": [1.0]})
        result = usgs._add_parameter_info(df)
        assert result.equals(df)

    def test_add_parameter_info_empty_df(self) -> None:
        """Cover early return for empty DataFrame."""
        df = pd.DataFrame({"code": pd.Series([], dtype=str), "value": pd.Series([], dtype=float)})
        result = usgs._add_parameter_info(df)
        assert result.empty

    def test_add_parameter_info_no_matching_codes(self) -> None:
        """Cover branch when no codes match known parameter codes."""
        df = pd.DataFrame({"code": ["99999", "88888"], "value": [1.0, 2.0]})
        result = usgs._add_parameter_info(df)
        # Original df returned since no known codes match
        assert result.equals(df)

    def test_normalize_station_data_extracts_site_no(self) -> None:
        """Cover branch extracting site_no from monitoring_location_id."""
        df = pd.DataFrame(
            {
                "monitoring_location_id": ["USGS-01646500"],
                "time": ["2022-01-01"],
                "parameter_code": ["00065"],
                "value": [1.0],
                "approval_status": ["Approved"],
            }
        )
        result = usgs._normalize_station_data(df, site_no=None, set_index=False)
        assert "site_no" in result.columns
        assert result["site_no"].iloc[0] == "01646500"

    def test_normalize_station_data_adds_option_column(self) -> None:
        """Cover branch where 'option' column doesn't exist yet."""
        df = pd.DataFrame(
            {
                "time": ["2022-01-01"],
                "parameter_code": ["00065"],
                "value": [1.0],
                "approval_status": ["Approved"],
            }
        )
        result = usgs._normalize_station_data(df, site_no="01646500", set_index=False)
        assert "option" in result.columns
        assert result["option"].iloc[0] == ""

    def test_get_dataset_from_empty_df(self) -> None:
        """Cover early return for empty DataFrame."""
        df = pd.DataFrame()
        meta = pd.DataFrame({"site_no": ["01646500"], "dec_lat_va": [38.0], "dec_long_va": [-77.0]})
        result = usgs._get_dataset_from_station_data(df, meta)
        assert isinstance(result, xr.Dataset)
        assert len(result.data_vars) == 0

    def test_get_dataset_no_matching_sites(self) -> None:
        """Cover branch where data site_nos don't match metadata."""
        df = pd.DataFrame(
            {
                "site_no": ["99999999"],
                "datetime": [pd.Timestamp("2022-01-01")],
                "code": ["00065"],
                "option": [""],
                "value": [1.0],
                "qualifier": ["A"],
            }
        )
        meta = pd.DataFrame({"site_no": ["01646500"], "dec_lat_va": [38.0], "dec_long_va": [-77.0]})
        result = usgs._get_dataset_from_station_data(df, meta)
        assert isinstance(result, xr.Dataset)
        assert len(result.data_vars) == 0

    def test_set_api_key_env_with_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Cover branch where API key is set in env."""
        monkeypatch.delenv(usgs.USGS_API_KEY_ENV_VAR, raising=False)
        usgs._set_api_key_env(api_key="test-key-123")
        import os

        assert os.environ.get(usgs.USGS_API_KEY_ENV_VAR) == "test-key-123"
        # Clean up
        monkeypatch.delenv(usgs.USGS_API_KEY_ENV_VAR, raising=False)


class TestParameterAvailability:
    """Test parameter availability functions."""

    def test_get_station_parameter_availability_returns_dataframe(self) -> None:
        """Test that get_station_parameter_availability returns proper DataFrame."""
        result = usgs.get_station_parameter_availability(
            site_nos=["01646500"],  # Potomac River station
        )
        assert isinstance(result, pd.DataFrame)
        assert "site_no" in result.columns
        assert "has_water_level" in result.columns
        assert "has_temperature" in result.columns
        assert "has_salinity" in result.columns
        assert "has_currents" in result.columns
        assert len(result) == 1
        assert result.iloc[0]["site_no"] == "01646500"

    def test_get_station_parameter_availability_multiple_stations(self) -> None:
        """Test parameter availability for multiple stations."""
        result = usgs.get_station_parameter_availability(
            site_nos=["01646500", "15484000"],
        )
        assert len(result) == 2
        assert set(result["site_no"]) == {"01646500", "15484000"}

    def test_get_station_parameter_availability_nonexistent_station(self) -> None:
        """Test that non-existent stations return False for all parameters."""
        result = usgs.get_station_parameter_availability(
            site_nos=["99999999"],  # Non-existent station
        )
        assert len(result) == 1
        assert result.iloc[0]["site_no"] == "99999999"
        assert not result.iloc[0]["has_water_level"]
        assert not result.iloc[0]["has_temperature"]
        assert not result.iloc[0]["has_salinity"]
        assert not result.iloc[0]["has_currents"]

    def test_get_station_parameter_availability_boolean_values(self) -> None:
        """Test that availability columns contain boolean values."""
        result = usgs.get_station_parameter_availability(
            site_nos=["01646500"],
        )
        for col in ["has_water_level", "has_temperature", "has_salinity", "has_currents"]:
            assert result[col].dtype == bool or result.iloc[0][col] in [True, False]


class TestGetUSGSStationsWithAvailability:
    """Test get_usgs_stations with include_parameter_availability option."""

    def test_get_usgs_stations_without_availability(self) -> None:
        """Test that default behavior doesn't include availability columns."""
        stations = usgs.get_usgs_stations(
            lon_min=-77.2,
            lon_max=-77.0,
            lat_min=38.9,
            lat_max=39.0,
        )
        # Should not have availability columns by default
        assert "has_water_level" not in stations.columns

    def test_get_usgs_stations_with_availability_columns(self) -> None:
        """Test that include_parameter_availability adds the right columns."""
        stations = usgs.get_usgs_stations(
            lon_min=-77.2,
            lon_max=-77.0,
            lat_min=38.9,
            lat_max=39.0,
            include_parameter_availability=True,
        )
        if not stations.empty:
            assert "has_water_level" in stations.columns
            assert "has_temperature" in stations.columns
            assert "has_salinity" in stations.columns
            assert "has_currents" in stations.columns
