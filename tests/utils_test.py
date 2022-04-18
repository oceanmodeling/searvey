from typing import Any

import numpy as np
import numpy.typing as npt
import pytest

from searvey import utils


@pytest.mark.parametrize(
    'lon1,expected_lon3', [(-180, 180), (-90, 270), (0, 0), (90, 90), (180, 180)],
)
def test_lon1_to_lon3_scalars(lon1: float, expected_lon3: float) -> None:
    assert utils.lon1_to_lon3(lon1) == expected_lon3


@pytest.mark.parametrize(
    'lon1,expected_lon3',
    [
        (np.array([180]), np.array([180])),
        (np.array([-180, -90, 0, 90, 180]), np.array([180, 270, 0, 90, 180])),
    ],
)
def test_lon1_to_lon3_numpy(lon1: npt.NDArray[Any], expected_lon3: npt.NDArray[Any]) -> None:
    assert np.array_equal(utils.lon1_to_lon3(lon1), expected_lon3)


@pytest.mark.parametrize(
    'lon3,expected_lon1', [(0, 0), (90, 90), (180, -180), (270, -90), (360, 0)],
)
def test_lon3_to_lon1_scalars(lon3: float, expected_lon1: float) -> None:
    assert utils.lon3_to_lon1(lon3) == expected_lon1


@pytest.mark.parametrize(
    'lon3,expected_lon1',
    [
        (np.array([180]), np.array([-180])),
        (np.array([0, 90, 181, 270, 359]), np.array([0, 90, -179, -90, -1])),
    ],
)
def test_lon3_to_lon1_numpy(lon3: float, expected_lon1: float) -> None:
    lon1 = utils.lon3_to_lon1(lon3)
    assert np.array_equal(lon1, expected_lon1)


@pytest.mark.parametrize('lon1', [-162.3, -32.3, -0.02, 0.01, 45.23, 163.2])
def test_lon1_to_lon3_roundtrip(lon1: float) -> None:
    assert lon1 == pytest.approx(utils.lon3_to_lon1(utils.lon1_to_lon3(lon1)))


@pytest.mark.parametrize('lon3', [0.01, 45.23, 163.2, 181.1, 273.2, 332.1])
def test_lon3_to_lon1_roundtrip(lon3: float) -> None:
    assert lon3 == pytest.approx(utils.lon1_to_lon3(utils.lon3_to_lon1(lon3)))
