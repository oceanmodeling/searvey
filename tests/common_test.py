from __future__ import annotations

import httpx
import multifutures
import pandas as pd
import pytest

from searvey._common import _fetch_url
from searvey._common import _fetch_url_main
from searvey._common import _resolve_end_date
from searvey._common import _resolve_http_client
from searvey._common import _resolve_rate_limit
from searvey._common import _resolve_start_date
from searvey._common import _to_utc


def test_fetch_url():
    url = "https://google.com"
    response = _fetch_url_main(url, client=httpx.Client())
    assert "The document has moved" in response


def test_fetch_url_failure():
    url = "http://localhost"
    with pytest.raises(httpx.ConnectError) as exc:
        _fetch_url_main(url, client=httpx.Client(timeout=0))
    assert "in progress" in str(exc)


def test_fetch_url_full():
    url = "https://google.com"
    response = _fetch_url(url, client=httpx.Client(), rate_limit=multifutures.RateLimit())
    assert "The document has moved" in response


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
