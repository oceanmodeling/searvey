import datetime
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon

import searvey._chs_api as chs

def test_get_chs_stations():
    stations = chs.get_chs_stations()
    assert isinstance(stations, gpd.GeoDataFrame)
    assert not stations.empty

def test_get_chs_stations_within_box():
    """
    Test that the stations are within the bounding box.
    """
    stations = chs.get_chs_stations(
        lon_min=-150,
        lat_min=40,
        lon_max=-110,
        lat_max=60,
    )
    assert isinstance(stations, gpd.GeoDataFrame)

    # Check that the stations are within the bounding box
    assert stations.geometry.within(
        gpd.GeoSeries.from_wkt(["POLYGON((-150 40, -150 60, -110 60, -110 40, -150 40))"])[0]
    ).all()

def test_fetch_chs_station_data():
    """
    This test will attempt to get data for a single station.
    """
    df = chs.fetch_chs_station(
        station_id="5cebf1e33d0f4a073c4bc23e",
        time_series_code="wlp",  # Water level prediction
        start_date=datetime.date(2023, 1, 1),
        end_date="2023-01-07",
    )

    assert isinstance(df, pd.DataFrame)

    assert "value" in df.columns
    assert df.eventDate[0] == "2023-01-01T00:00:00Z"

def test_fetch_chs_data_multiple():
    """
    This test will attempt to get data for multiple stations.
    """
    dataframes = chs._fetch_chs(
        station_ids=["5cebf1de3d0f4a073c4bbad5", "5cebf1e33d0f4a073c4bc23e"],
        time_series_code="wlp",
        start_dates=[datetime.date(2023, 1, 1), "2023-01-01"],
        end_dates=["2023-01-07", "2023-01-07"],
    )

    assert isinstance(dataframes, dict)
    assert len(dataframes) == 2
    assert "5cebf1de3d0f4a073c4bbad5" in dataframes
    assert "5cebf1e33d0f4a073c4bc23e" in dataframes

    for station_id, df in dataframes.items():
        assert isinstance(df, pd.DataFrame)
        assert "value" in df.columns
        assert df.eventDate[0] == "2023-01-01T00:00:00Z"

def test_fetch_chs_data_unavailable_available_data():
    """
    This is a test that makes sure that the function can handle when some stations have data and some don't.
    """
    dataframes = chs._fetch_chs(
        station_ids=["94323"],
        time_series_code="wlp",
        start_dates=[datetime.datetime(2023, 1, 1, 10, 0, 0)],
        end_dates=[datetime.date(2023, 1, 7)],
    )

    assert isinstance(dataframes, dict)
    assert len(dataframes) == 1
    assert "94323" in dataframes

    df = dataframes["94323"]
    assert isinstance(df, pd.DataFrame)
    assert df.status[0] == "NOT_FOUND"
