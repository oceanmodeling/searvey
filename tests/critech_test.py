import pandas as pd
import pytest

from searvey import critech


def test_that_longitude_less_than_minus_180_raises() -> None:
    with pytest.raises(ValueError) as exc:
        critech.get_critech_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lon_min=-1000)
    assert "ensure this value is greater than or equal to -180" in str(exc)


def test_that_longitude_greater_than_180_raises() -> None:
    with pytest.raises(ValueError) as exc:
        critech.get_critech_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lon_min=1000)
    assert "ensure this value is less than or equal to 180" in str(exc)


def test_that_latitude_less_than_minus_90_raises() -> None:
    with pytest.raises(ValueError) as exc:
        critech.get_critech_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lat_min=-1000)
    assert "ensure this value is greater than or equal to -90" in str(exc)


def test_that_latitude_greater_than_90_raises() -> None:
    with pytest.raises(ValueError) as exc:
        critech.get_critech_data(start_date=pd.to_datetime("2000-01-01 00:00:00"), lat_min=1000)
    assert "ensure this value is less than or equal to 90" in str(exc)


@pytest.mark.vcr
def test_get_critech_data() -> None:
    df = critech.get_critech_data(
        start_date=pd.to_datetime("2021-08-01 00:00:00"),
        end_date=pd.to_datetime("2021-08-01 00:30:00"),
        lon_min=-3,
        lon_max=3,
        lat_min=35,
        lat_max=36,
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 155
    assert "lon" in df.columns
    assert "lat" in df.columns
    assert "time" in df.columns
    assert "sea_level" in df.columns
    assert df.lon.min() >= -3
    assert df.lon.max() <= 3
    assert df.lat.min() >= 35
    assert df.lat.max() <= 36
