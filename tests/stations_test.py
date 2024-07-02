import datetime

import geopandas as gpd
import pandas as pd

from searvey import stations


# XXX For some reason we can't use a cassette for this test. Haven't investigated
# @pytest.mark.vcr
def test_get_stations():
    stations_gdf = stations.get_stations(activity_threshold=datetime.timedelta(days=1))
    assert isinstance(stations_gdf, pd.DataFrame)
    assert isinstance(stations_gdf, gpd.GeoDataFrame)
    assert len(stations_gdf) > 1000
    assert set(stations_gdf.columns) == set(stations.STATIONS_COLUMNS), "wrong columns"


def test_get_stations_specify_providers():
    threshold = datetime.timedelta(days=1)

    ioc_provider = stations.get_stations(activity_threshold=threshold, providers=stations.Provider.IOC)
    coops_provider = stations.get_stations(activity_threshold=threshold, providers=stations.Provider.COOPS)
    usgs_provider = stations.get_stations(activity_threshold=threshold, providers=stations.Provider.USGS)
    ndbc_provider = stations.get_stations(providers=stations.Provider.NDBC)
    all_providers = stations.get_stations(activity_threshold=threshold, providers=stations.Provider.ALL)
    multiple_providers = stations.get_stations(
        activity_threshold=threshold,
        providers=[
            stations.Provider.COOPS,
            stations.Provider.IOC,
            stations.Provider.USGS,
            stations.Provider.NDBC,
        ],
    )

    assert len(ioc_provider) + len(coops_provider) + len(usgs_provider) + len(ndbc_provider) == len(
        all_providers
    )
    assert len(multiple_providers) == len(all_providers)
