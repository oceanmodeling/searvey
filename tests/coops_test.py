from datetime import datetime
from datetime import timedelta
from urllib.parse import quote

import httpx
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

from searvey import fetch_coops_station
from searvey.coops import _coops_date
from searvey.coops import _generate_urls
from searvey.coops import COOPS_Product
from searvey.coops import coops_product_within_region
from searvey.coops import COOPS_ProductFieldsNameMap
from searvey.coops import COOPS_ProductFieldTypes
from searvey.coops import COOPS_Station
from searvey.coops import coops_stations
from searvey.coops import coops_stations_within_region
from searvey.coops import get_coops_stations


@pytest.mark.vcr
def test_coops_stations_nws_api():
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
def test_coops_stations_within_region_nws_api():
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
def test_coops_stations_main_api():
    stations_main_api = get_coops_stations(metadata_source="main")

    assert len(stations_main_api) > 0
    assert list(stations_main_api.columns) == [
        "nws_id",
        "name",
        "state",
        "lon",
        "lat",
        "removed",
        "status",
        "geometry",
    ]

    stations_nws_api = get_coops_stations(metadata_source="nws")
    assert len(stations_nws_api) > 0
    assert list(stations_nws_api.columns) == [
        "nws_id",
        "name",
        "state",
        "status",
        "removed",
        "geometry",
    ]

    with pytest.raises(ValueError) as excinfo:
        get_coops_stations(metadata_source="someothersource")
    assert "not a valid coops_stationmetadatasource" in str(excinfo.value).lower()


@pytest.mark.vcr
def test_coops_stations_within_region_main_api():
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


def test_generate_urls():
    station_id = "AAA"
    start_date = pd.Timestamp("2023-01-01")
    end_date = pd.Timestamp("2023-06-01")
    urls = _generate_urls(
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
    )
    assert len(urls) == 6
    assert all(isinstance(url, httpx.URL) for url in urls)
    assert all(station_id in str(url) for url in urls)
    assert quote(_coops_date(start_date)) in str(urls[0])
    assert quote(_coops_date(end_date)) in str(urls[-1])


def test_generate_urls_raises_common_start_date_and_end_date():
    station_id = "AAA"
    date = pd.Timestamp("2023-06-01")
    urls = _generate_urls(
        station_id=station_id,
        start_date=date,
        end_date=date,
    )
    assert len(urls) == 0
    assert not urls
    assert urls == []


def test_generate_urls_raises_when_end_date_before_start_date():
    start_date = pd.Timestamp("2023-01-01")
    end_date = pd.Timestamp("2022-01-01")
    with pytest.raises(ValueError) as exc:
        _generate_urls(
            station_id="aaaa",
            start_date=start_date,
            end_date=end_date,
        )
    assert str(exc.value) == f"'end_date' must be after 'start_date': {end_date} vs {start_date}"


@pytest.mark.parametrize(
    "station_id, product",
    [
        (8654467, "water_level"),
        (8636580, "one_minute_water_level"),
        (8654467, "predictions"),
        (8574730, "air_gap"),
        (8654467, "wind"),
        (8654467, "air_pressure"),
        (8654467, "air_temperature"),
        (8575437, "visibility"),
        (8720233, "humidity"),
        (8654467, "water_temperature"),
        (8737048, "conductivity"),
        ("cb1101", "currents"),
        ("cb1101", "currents_predictions"),
        (8654467, "datums"),
        # (8636580, "hourly_height"),  # Default dates won't work!
        # (8636580, "high_low"),  # Default dates won't work!
        # (8518750, "monthly_mean"),  # Default dates won't work!
        # (8720233, "salinity"),  # Can't find stations with this data!
        # (8636580, "daily_mean"),  # Not supported by searvey
    ],
)
def test_coops_data_products_default_args(station_id, product):
    df = fetch_coops_station(station_id, product=product)
    assert all(col in COOPS_ProductFieldsNameMap[COOPS_Product(product)].values() for col in df.columns)
    assert all(
        df.dtypes[col] == COOPS_ProductFieldTypes[col]
        for col in df.columns
        if df.dtypes[col] != np.dtype("O")
    )


@pytest.fixture
def now():
    # Seconds are truncated for COOPS query url
    return pd.Timestamp(datetime.now()).floor("min")


@pytest.mark.parametrize(
    "days_before_now, station_id, product",
    [
        (7, 8654467, "water_level"),
        (60, 8636580, "hourly_height"),
        (60, 8636580, "high_low"),
        (60, 8518750, "monthly_mean"),
        (7, 8636580, "one_minute_water_level"),
        (7, 8654467, "predictions"),
        (7, 8574730, "air_gap"),
        (7, 8654467, "wind"),
        (7, 8654467, "air_pressure"),
        (7, 8654467, "air_temperature"),
        (7, 8575437, "visibility"),
        (7, 8720233, "humidity"),
        (7, 8654467, "water_temperature"),
        (7, 8737048, "conductivity"),
        (7, "cb1101", "currents"),
        (7, "cb1101", "currents_predictions"),
        (7, 8654467, "datums"),
        # (7, 8720233, "salinity"),
        # (30, 8636580, "daily_mean"),
    ],
)
def test_coops_data_products_w_date_input(now, days_before_now, station_id, product):
    start_date = now - timedelta(days=days_before_now)
    end_date = now
    # Warn for no tzinfo
    with pytest.warns():
        df = fetch_coops_station(
            station_id,
            product=product,
            start_date=start_date,
            end_date=end_date,
        )

    assert all(col in COOPS_ProductFieldsNameMap[COOPS_Product(product)].values() for col in df.columns)
    assert all(
        df.dtypes[col] == COOPS_ProductFieldTypes[col]
        for col in df.columns
        if df.dtypes[col] != np.dtype("O")
    )

    if df.index.name == "time":
        # When selecting between intervals one time prior to the first
        # is also included (at most 1 out of range)
        assert (~((start_date <= df.index) & (df.index <= end_date))).sum() <= 1


def test_coops_warn_utc():
    fetch_coops_station(
        8654467,
        product="water_level",
        start_date=pd.Timestamp.now("est") - timedelta(days=7),
        end_date=pd.Timestamp.now("est"),
    )

    fetch_coops_station(
        8654467,
        product="water_level",
        start_date=pd.Timestamp.now() - timedelta(days=7),
        end_date=pd.Timestamp.now(),
    )

    assert False
