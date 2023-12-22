from datetime import datetime

import pytest
from shapely.geometry import box

from searvey.coops import coops_product_within_region
from searvey.coops import COOPS_Station
from searvey.coops import coops_stations
from searvey.coops import coops_stations_within_region
from searvey.coops import get_coops_stations


@pytest.mark.vcr
def test_coops_stations_old_api():
    stations = coops_stations()

    assert len(stations) > 0
    assert list(stations.columns) == [
        "nws_id",
        "name",
        "state",
        "status",
        "removed",
        "geometry",
    ]


@pytest.mark.vcr
def test_coops_stations_within_region_old_api():
    region = box(-83, 25, -75, 36)

    stations = coops_stations_within_region(region=region)

    assert len(stations) > 0
    assert list(stations.columns) == [
        "nws_id",
        "name",
        "state",
        "status",
        "removed",
        "geometry",
    ]


@pytest.mark.vcr
def test_coops_stations_new_api():
    stations = get_coops_stations(metadata_source="main")

    assert len(stations) > 0
    assert list(stations.columns) == [
        "nws_id",
        "name",
        "state",
        "lon",
        "lat",
        "removed",
        "status",
        "geometry",
    ]


@pytest.mark.vcr
def test_coops_stations_within_region_new_api():
    region = box(-83, 25, -75, 36)

    stations = get_coops_stations(region=region, metadata_source="main")

    assert len(stations) > 0
    assert list(stations.columns) == [
        "nws_id",
        "name",
        "state",
        "lon",
        "lat",
        "removed",
        "status",
        "geometry",
    ]


@pytest.mark.vcr
def test_coops_product_within_region():
    region = box(-83, 25, -75, 36)

    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 1, 0, 10)

    data = coops_product_within_region(
        "water_level",
        region=region,
        start_date=start_date,
        end_date=end_date,
    )

    assert len(data["t"]) > 0


@pytest.mark.vcr
def test_coops_station():
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 1, 0, 10)

    station_1 = COOPS_Station(1612480)
    station_2 = COOPS_Station("OOUH1")
    station_3 = COOPS_Station("Calcasieu Test Station")
    station_4 = COOPS_Station(9414458)

    assert station_1.id == "1612480"
    assert station_1.nws_id == "MOKH1"
    assert station_1.name == "Mokuoloe"
    assert station_2.id == "1612340"
    assert station_2.nws_id == "OOUH1"
    assert station_2.name == "Honolulu"
    assert station_3.id == "9999531"
    assert station_3.nws_id == ""
    assert station_3.name == "Calcasieu Test Station"
    assert station_4.id == "9414458"
    assert station_4.nws_id == "ZSMC1"
    assert station_4.name == "San Mateo Bridge"
    assert not station_4.current

    assert len(station_1.constituents) > 0
    assert len(station_2.constituents) > 0
    assert len(station_3.constituents) == 0
    assert len(station_4.constituents) > 0

    station_1_data = station_1.product("water_level", start_date, end_date)
    station_2_data = station_2.product("water_level", start_date, end_date)
    station_3_data = station_3.product("water_level", start_date, end_date)
    station_4_data = station_4.product("water_level", "2005-03-30", "2005-03-30 02:00:00")

    assert station_1_data.sizes == {"nos_id": 1, "t": 2}
    assert station_2_data.sizes == {"nos_id": 1, "t": 2}
    assert len(station_3_data["t"]) == 0
    assert station_4_data.sizes == {"nos_id": 1, "t": 21}


@pytest.mark.vcr
def test_coops_predictions_product():
    region = box(-83, 25, -75, 36)

    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 1, 0, 10)

    data = coops_product_within_region(
        "predictions",
        region=region,
        start_date=start_date,
        end_date=end_date,
    )

    assert len(data["t"]) > 0
