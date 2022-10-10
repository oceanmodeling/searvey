import datetime

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from searvey import ioc


@pytest.mark.vcr
def test_get_ioc_stations():
    stations = ioc.get_ioc_stations()
    assert isinstance(stations, pd.DataFrame)
    assert isinstance(stations, gpd.GeoDataFrame)
    assert len(stations) > 1000
    # check that the DataFrame has the right columns
    df_columns = {col for col in stations.columns}
    expected_columns = {col for values in ioc.IOC_STATIONS_COLUMN_NAMES.values() for col in values}
    expected_columns.remove("view")
    assert df_columns.issuperset(expected_columns)


@pytest.mark.vcr
@pytest.mark.parametrize(
    "truncate_seconds,no_records",
    [
        pytest.param(True, 145, id="truncate_seconds=True"),
        pytest.param(False, 210, id="truncate_seconds=False"),
    ],
)
def test_get_ioc_station_data(truncate_seconds, no_records):
    """Truncate_seconds=False returns more datapoints compared to `=True`"""
    df = ioc.get_ioc_station_data(
        ioc_code="vera",
        endtime=datetime.date(2022, 9, 1),
        period=0.1,
        truncate_seconds=truncate_seconds,
    )
    assert len(df) == no_records


_IOC_METADATA = pd.DataFrame.from_dict(
    {
        "ioc_code": {
            0: "abas",
            1: "abed",
            2: "abur",
            3: "acaj",
            4: "acap",
            5: "acap2",
            6: "acnj",
            7: "acnj2",
            8: "acor1",
            9: "acya",
            10: "adak",
            11: "vera",
        },
        "lon": {
            0: 144.29,
            1: -2.08,
            2: 131.41,
            3: -89.838,
            4: -99.917,
            5: -99.903,
            6: -74.418,
            7: -74.418,
            8: -8.399,
            9: -99.903,
            10: -176.632,
            11: -96.123553,
        },
        "lat": {
            0: 44.02,
            1: 57.14,
            2: 31.58,
            3: 13.574,
            4: 16.833,
            5: 16.838,
            6: 39.355,
            7: 39.355,
            8: 43.364,
            9: 16.838,
            10: 51.863,
            11: 19.192061,
        },
        "country": {
            0: "Japan",
            1: "UK",
            2: "Japan",
            3: "El Salvador",
            4: "Mexico",
            5: "Mexico",
            6: "USA",
            7: "USA",
            8: "Spain",
            9: "Mexico",
            10: "USA",
            11: "Mexico",
        },
        "location": {
            0: "Abashiri",
            1: "Aberdeen",
            2: "Aburatsu",
            3: "Acajutla",
            4: "Acapulco",
            5: "Acapulco API",
            6: "Atlantic City",
            7: "Atlantic City",
            8: "A Coruña",
            9: "Acapulco Club de Yates",
            10: "Adak",
            11: "Veracruz, Ver",
        },
    }
)


@pytest.mark.parametrize(
    "truncate_seconds",
    [
        pytest.param(True, id="truncate_seconds=True"),
        pytest.param(False, id="truncate_seconds=False"),
    ],
)
def test_get_ioc_data(truncate_seconds):
    # in order to speed up the execution time of the test,
    # we don't retrieve the IOC metadata from the internet,
    # but we use a hardcoded dict instead
    ioc_metadata = _IOC_METADATA
    ds = ioc.get_ioc_data(
        ioc_metadata=ioc_metadata,
        endtime="2022-06-01",
        period=1,
        truncate_seconds=truncate_seconds,
    )
    assert isinstance(ds, xr.Dataset)
    # Check if truncate_seconds does work
    if truncate_seconds:
        assert len(ds.time) == 1441
    else:
        assert len(ds.time) == 2811
    # Station acap has no data, so it is missing from the Dataset
    assert "acap" not in ds.ioc_code
    assert len(ds.ioc_code) == len(ioc_metadata) - 1
    # Check that ioc_code, lon, lat, country and location are not modified
    assert set(ds.ioc_code.values).issubset(ioc_metadata.ioc_code.values)
    assert set(ds.lon.values).issubset(ioc_metadata.lon.values)
    assert set(ds.lat.values).issubset(ioc_metadata.lat.values)
    assert set(ds.country.values).issubset(ioc_metadata.country.values)
    assert set(ds.location.values).issubset(ioc_metadata.location.values)
    # Check that actual data has been retrieved
    assert ds.sel(ioc_code="abas").rad.mean() == pytest.approx(1.947, rel=1e-3)
    assert ds.sel(ioc_code="abur").rad.mean() == pytest.approx(2.249, rel=1e-3)
    assert ds.sel(ioc_code="vera").rad.mean() == pytest.approx(1.592, rel=1e-3)
