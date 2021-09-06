import pandas as pd
import pytest

from searvey import uhslc


def test_that_negative_longitude_raises() -> None:
    with pytest.raises(ValueError) as exc:
        uhslc.get_uhslc_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lon_min=-1000)
    assert "ensure this value is greater than or equal to 0" in str(exc)


def test_that_longitude_greater_than_360_raises() -> None:
    with pytest.raises(ValueError) as exc:
        uhslc.get_uhslc_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lon_min=1000)
    assert "ensure this value is less than or equal to 360" in str(exc)


def test_that_latitude_less_than_minus_90_raises() -> None:
    with pytest.raises(ValueError) as exc:
        uhslc.get_uhslc_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lat_min=-1000)
    assert "ensure this value is greater than or equal to -90" in str(exc)


def test_that_latitude_greater_than_90_raises() -> None:
    with pytest.raises(ValueError) as exc:
        uhslc.get_uhslc_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lat_min=1000)
    assert "ensure this value is less than or equal to 90" in str(exc)


@pytest.mark.vcr
def test_get_uhslc_data() -> None:
    df = uhslc.get_uhslc_data(
        start_date=pd.to_datetime("2021-08-01 00:00:00"),
        end_date=pd.to_datetime("2021-08-01 12:00:00"),
        lon_min=300,
        lon_max=360,
        lat_min=-15,
        lat_max=15,
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 26
    assert "lon" in df.columns
    assert "lat" in df.columns
    assert "time" in df.columns
    assert "sea_level" in df.columns
    assert df.lon.min() >= -60
    assert df.lon.max() <= 0
    assert df.lat.min() >= -15
    assert df.lat.max() <= 15
