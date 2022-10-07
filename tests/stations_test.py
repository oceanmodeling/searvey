import datetime

import geopandas as gpd
import pandas as pd
import pytest

from searvey import stations


# XXX For some reason we can't use a cassette for this test. Haven't investigated
# @pytest.mark.vcr
def test_get_stations():
    stations_gdf = stations.get_stations(activity_threshold=datetime.timedelta(days=1))
    assert isinstance(stations_gdf, pd.DataFrame)
    assert isinstance(stations_gdf, gpd.GeoDataFrame)
    assert len(stations_gdf) > 1000
    assert set(stations_gdf.columns) == set(stations.STATIONS_COLUMNS), "wrong columns"
