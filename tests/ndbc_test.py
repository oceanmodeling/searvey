import datetime

import geopandas as gpd
import pandas as pd

from searvey import _ndbc_api as ndbc


def test_get_ndbc_stations():
    stations = ndbc.get_ndbc_stations()
    assert isinstance(stations, gpd.GeoDataFrame)
    assert not stations.empty


def test_get_ndbc_stations_within_box():
    """
    Test that the stations are within the bounding box.
    """
    stations = ndbc.get_ndbc_stations(
        lon_min=-76,
        lat_min=39,
        lon_max=-70,
        lat_max=43,
    )
    assert isinstance(stations, gpd.GeoDataFrame)
    
    # Check that the stations are within the bounding box
    assert stations.geometry.within(
        gpd.GeoSeries.from_wkt(["POLYGON((-76 39, -76 43, -70 43, -70 39, -76 39))"])[0]
    ).all()


def test_fetch_ndbc_stations_data():
    """
    This test will attempt to get data for a single station.
    """
    dataframes = ndbc.fetch_ndbc_stations_data(
        station_ids=["SRST2"],
        mode="stdmet",
        #test that both formats work
        start_date=datetime.date(2023, 1, 1),
        end_date="2023-01-10",
    )

    assert isinstance(dataframes, dict)
    assert len(dataframes) == 1
    assert "SRST2" in dataframes
    df = dataframes["SRST2"]
    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "timestamp"
    assert all(col in df.columns for col in ["WDIR", "WSPD", "GST","DPD","APD","MWD","WTMP","DEWP","VIS","WVHT", "PRES", "ATMP","TIDE"])
    assert df.index[0] == pd.to_datetime("2023-01-01 00:00:00")
    assert df.index[-1] == pd.to_datetime("2023-01-10 00:00:00")


def test_fetch_ndbc_stations_data_multiple():
    """
    This test will attempt to get data for multiple stations.
    """

    dataframes = ndbc.fetch_ndbc_stations_data(
        station_ids=["STDM4","TPLM2"],
        mode="stdmet",
        #test that both formats work
        start_date=datetime.date(2023, 1, 1),
        end_date="2023-01-10",
    )

    assert isinstance(dataframes, dict)
    assert len(dataframes) == 2
    assert "STDM4" in dataframes
    assert "TPLM2" in dataframes
    df = dataframes["STDM4"]
    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "timestamp"
    assert all(col in df.columns for col in ["WDIR", "WSPD", "GST","DPD","APD","MWD","WTMP","DEWP","VIS","WVHT", "PRES", "ATMP","TIDE"])
    assert df.index[0] == pd.to_datetime("2023-01-01 00:00:00")
    df1 = dataframes["TPLM2"]
    assert isinstance(df1, pd.DataFrame)
    assert df1.index.name == "timestamp"
    assert all(col in df1.columns for col in ["WDIR", "WSPD", "GST","DPD","APD","MWD","WTMP","DEWP","VIS","WVHT", "PRES", "ATMP","TIDE"])
    assert df1.index[0] == pd.to_datetime("2023-01-01 00:00:00")


def test_fetch_ndbc_stations_data_multiple_unavaliable_avaliable_data():
    """
    This is a test that makes sure that the function can handle when some stations have data and some don't.
    """
    dataframes = ndbc.fetch_ndbc_stations_data(
        station_ids=["41001","STDM4"],
        mode="stdmet",
        #test that both formats work
        start_date=datetime.date(2023, 1, 1),
        end_date="2023-01-10",
    )

    assert isinstance(dataframes, dict)
    assert len(dataframes) == 2
    assert "41001" in dataframes
    assert dataframes["41001"].empty
    assert "STDM4" in dataframes
    df = dataframes["STDM4"]
    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "timestamp"
    assert all(col in df.columns for col in ["WDIR", "WSPD", "GST","DPD","APD","MWD","WTMP","DEWP","VIS","WVHT", "PRES", "ATMP","TIDE"])
    assert df.index[0] == pd.to_datetime("2023-01-01 00:00:00")
    assert df.index[-1] == pd.to_datetime("2023-01-10 00:00:00")




