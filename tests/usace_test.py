from unittest.mock import patch

import httpx
import pandas as pd
import pytest

from searvey._usace_api import _fetch_usace
from searvey._usace_api import _generate_urls
from searvey._usace_api import fetch_usace_station


def test_generate_urls():
    station_id = "01300"
    start_date = pd.Timestamp("2020-04-05")
    end_date = pd.Timestamp("2020-04-10")

    urls = _generate_urls(station_id, start_date, end_date)

    assert len(urls) == 1
    assert station_id in urls[0]
    assert "2020-04-05" in urls[0]
    assert "2020-04-10" in urls[0]


def test_fetch_usace():
    result = _fetch_usace(
        station_ids=["01300"],
        start_dates=["2020-04-05"],
        end_dates=["2020-04-10"],
        rate_limit=None,
        http_client=httpx.Client(verify=False),
        multiprocessing_executor=None,
        multithreading_executor=None,
    )
    assert "01300" in result
    assert isinstance(result["01300"], pd.DataFrame)
    assert len(result) == 1


@patch("searvey._usace_api._fetch_usace")
def test_fetch_usace_station(mock_fetch):
    mock_df = pd.DataFrame(
        {"value": [10.5, 11.2, 10.8]}, index=pd.date_range("2020-04-05", periods=3, freq="D")
    )
    mock_df.index.name = "time"
    mock_df.attrs["station_id"] = "USACE-01300"

    mock_fetch.return_value = {"01300": mock_df}

    result = fetch_usace_station(
        "01300", start_date="2020-04-05", end_date="2020-04-10", http_client=httpx.Client(verify=False)
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert result.index.name == "time"
    assert "value" in result.columns
    assert result.attrs["station_id"] == "USACE-01300"


def test_fetch_usace_station_error_handling():
    with patch("searvey._usace_api._fetch_usace", side_effect=Exception("API Error")):
        result = fetch_usace_station(
            "01300", start_date="2020-04-05", end_date="2020-04-10", http_client=httpx.Client(verify=False)
        )
        assert result.empty


if __name__ == "__main__":
    pytest.main()
