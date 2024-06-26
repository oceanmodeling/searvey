from __future__ import annotations

import unittest.mock

import pandas as pd
import pytest

from searvey import fetch_ioc_station
from searvey._ioc_api import _generate_urls
from searvey._ioc_api import _ioc_date


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
    assert all(isinstance(url, str) for url in urls)
    assert all(station_id in url for url in urls)
    assert _ioc_date(start_date) in urls[0]
    assert _ioc_date(end_date) in urls[-1]


def test_generate_urls_raises_common_start_date_and_end_date():
    station_id = "AAA"
    date = pd.Timestamp("2023-01-01")
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


@unittest.mock.patch("searvey._common._fetch_url")
def test_fetch_ioc_station_empty_responses(mocked_fetch_url):
    station_id = "blri"
    start_date = "2023-09-01"
    end_date = "2023-12-10"
    # The period between start_date and end_date should hit 4 URLs
    mocked_fetch_url.side_effect = [
        f"""[{{"error":"code '{station_id}' not found"}}]""",
        f"""[{{"error":"code '{station_id}' not found"}}]""",
        '[{"error":"Incorrect code"}]',
        "[]",
    ]
    df = fetch_ioc_station(
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
    )
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@unittest.mock.patch("searvey._common._fetch_url")
def test_fetch_ioc_station_normal_call(mocked_fetch_url):
    station_id = "acnj"
    start_date = "2022-03-12T11:04:00"
    end_date = "2022-03-12T11:06:00"
    mocked_fetch_url.side_effect = [
        """ [\
        {"slevel":0.905,"stime":"2022-03-12 11:04:00","sensor":"wls"},
        {"slevel":0.906,"stime":"2022-03-12 11:05:00","sensor":"wls"},
        {"slevel":0.896,"stime":"2022-03-12 11:06:00","sensor":"wls"}
        ]""",
    ]
    df = fetch_ioc_station(
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
    )
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 3


@unittest.mock.patch("searvey._common._fetch_url")
def test_fetch_ioc_station_duplicated_timestamps(mocked_fetch_url):
    station_id = "acnj"
    start_date = "2022-03-12T11:04:00"
    end_date = "2022-03-12T11:06:00"
    mocked_fetch_url.side_effect = [
        """ [\
        {"slevel":0.905,"stime":"2022-03-12 11:04:00","sensor":"wls"},
        {"slevel":0.906,"stime":"2022-03-12 11:05:00","sensor":"wls"},
        {"slevel":0.906,"stime":"2022-03-12 11:05:00","sensor":"wls"},
        {"slevel":0.906,"stime":"2022-03-12 11:05:00","sensor":"wls"},
        {"slevel":0.896,"stime":"2022-03-12 11:06:00","sensor":"wls"}
        ]""",
    ]
    df = fetch_ioc_station(
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
    )
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 3
    assert df.wls.max() == 0.906
    assert df.wls.min() == 0.896
    assert df.wls.median() == 0.905


def test_ioc_webserver():
    station_id = "acnj"
    start_date = "2022-03-12T11:04:00"
    end_date = "2022-03-12T11:06:00"
    df = fetch_ioc_station(
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
    )
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 3
    assert df.wls.max() == 0.906
    assert df.wls.min() == 0.896
    assert df.wls.median() == 0.905
