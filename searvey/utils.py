from typing import Any
from typing import overload
from typing import Union

import numpy as np
import numpy.typing as npt

ScalarOrArray = Union[float, npt.NDArray[Any]]

# https://gis.stackexchange.com/questions/201789/verifying-formula-that-will-convert-longitude-0-360-to-180-to-180
# lon1 is the longitude varying from -180 to 180 or 180W-180E
# lon3 is the longitude variable from 0 to 360 (all positive)


# fmt: off
@overload
def lon1_to_lon3(lon1: npt.NDArray[Any]) -> npt.NDArray[Any]: ...  # noqa: E704
@overload
def lon1_to_lon3(lon1: float) -> float: ...  # noqa: E704


def lon1_to_lon3(lon1: ScalarOrArray) -> ScalarOrArray:
    return lon1 % 360


@overload
def lon3_to_lon1(lon3: float) -> float: ...  # noqa: E704
@overload
def lon3_to_lon1(lon3: npt.NDArray[np.float_]) -> npt.NDArray[np.float_]: ...  # noqa: E704


def lon3_to_lon1(lon3: ScalarOrArray) -> ScalarOrArray:
    return ((lon3 + 180) % 360) - 180
# fmt: on
