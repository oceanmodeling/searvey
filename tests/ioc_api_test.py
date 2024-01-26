from __future__ import annotations

import unittest.mock

import httpx
import multifutures
import pandas as pd
import pytest

from searvey import fetch_ioc_station
from searvey.ioc import _fetch_url
from searvey.ioc import _generate_urls
from searvey.ioc import _ioc_date
from searvey.ioc import _resolve_end_date
from searvey.ioc import _resolve_http_client
from searvey.ioc import _resolve_rate_limit
from searvey.ioc import _resolve_start_date
from searvey.ioc import _to_utc
from searvey.ioc import fetch_url


def test_fetch_url():
    url = "https://google.com"
    response = _fetch_url(url, client=httpx.Client())
    assert "The document has moved" in response


def test_fetch_url_failure():
    url = "http://localhost"
    with pytest.raises(httpx.ConnectError) as exc:
        _fetch_url(url, client=httpx.Client(timeout=0))
    assert "in progress" in str(exc)


def test_fetch_url_full():
    url = "https://google.com"
    response = fetch_url(url, client=httpx.Client(), rate_limit=multifutures.RateLimit())
    assert "The document has moved" in response


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


def test_resolve_rate_limit_returns_object_as_is():
    rate_limit = multifutures.RateLimit()
    resolved = _resolve_rate_limit(rate_limit=rate_limit)
    assert resolved is rate_limit


def test_resolve_http_client_returns_object_as_is():
    http_client = httpx.Client(timeout=httpx.Timeout(timeout=10, read=30))
    resolved = _resolve_http_client(http_client=http_client)
    assert resolved is http_client


def test_to_utc():
    # timestamp
    ts = pd.Timestamp("2004")
    ts_utc = pd.Timestamp("2004", tz="utc")
    assert _to_utc(ts) == ts_utc
    # DatetimeIndex
    index = pd.DatetimeIndex(["2004"])
    index_utc = pd.Timestamp("2004", tz="utc")
    assert _to_utc(index) == index_utc


def test_to_utc_with_tz():
    # timestamp
    ts_cet = pd.Timestamp("2004-01-01T01:00:00", tz="CET")
    ts_utc = pd.Timestamp("2004-01-01T00:00:00", tz="utc")
    assert _to_utc(ts_cet) == ts_utc
    # DatetimeIndex
    index_cet = pd.DatetimeIndex([ts_cet])
    index_utc = pd.DatetimeIndex([ts_utc])
    assert _to_utc(index_cet) == index_utc


def test_resolve_start_date_default():
    now = pd.Timestamp.now(tz="utc")
    expected = now - pd.Timedelta(days=7)
    resolved = _resolve_start_date(now=now, start_date=None)
    assert resolved == expected


def test_resolve_start_date_specific_value():
    now = pd.Timestamp.now(tz="utc")
    start_date = "2004"
    expected = pd.DatetimeIndex([start_date])
    assert _resolve_start_date(now, start_date) == expected


def test_resolve_end_date_default():
    now = pd.Timestamp.now(tz="utc")
    expected = now
    resolved = _resolve_end_date(now=now, end_date=None)
    assert resolved == expected


def test_resolve_end_date_specific_value():
    now = pd.Timestamp.now(tz="utc")
    end_date = "2004"
    expected = pd.DatetimeIndex([end_date])
    assert _resolve_end_date(now, end_date) == expected


@unittest.mock.patch("searvey.ioc.fetch_url")
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


@unittest.mock.patch("searvey.ioc.fetch_url")
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


@unittest.mock.patch("searvey.ioc.fetch_url")
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
