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


@pytest.mark.parametrize(
    "truncate_seconds",
    [
        pytest.param(True, id="truncate_seconds=True"),
        pytest.param(False, id="truncate_seconds=False"),
    ],
)
def test_get_ioc_data(truncate_seconds):
    # in order to speed up the execution time, we don't retrieve the IOC metadata
    # from the internet
    ioc_metadata = pd.DataFrame.from_dict(
        {
            "ioc_code": {0: "abed", 1: "acap2", 2: "stlu", 3: "vung"},
            "lon": {0: -2.08, 1: -99.903, 2: -60.997, 3: 107.071},
            "lat": {0: 57.14, 1: 16.838, 2: 14.016, 3: 10.34},
            "country": {0: "UK", 1: "Mexico", 2: "Saint Lucia", 3: "Viet Nam"},
            "location": {0: "Aberdeen", 1: "Acapulco API", 2: "Ganter's Bay", 3: "Vung Tau"},
        }
    )
    ds = ioc.get_ioc_data(
        ioc_metadata=ioc_metadata,
        endtime="2022-05-31",
        period=1,
        truncate_seconds=truncate_seconds,
    )
    assert isinstance(ds, xr.Dataset)
    assert len(ds.ioc_code) == len(ioc_metadata)
    np.testing.assert_equal(ds.ioc_code.values, ioc_metadata.ioc_code.values)
    np.testing.assert_equal(ds.lon.values, ioc_metadata.lon.values)
    np.testing.assert_equal(ds.lat.values, ioc_metadata.lat.values)
    np.testing.assert_equal(ds.country.values, ioc_metadata.country.values)
    np.testing.assert_equal(ds.location.values, ioc_metadata.location.values)
