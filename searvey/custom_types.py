from __future__ import annotations

from typing import Any
from typing import TypeVar

import numpy.typing as npt


StrDict = dict[str, Any]
ScalarOrArray = TypeVar("ScalarOrArray", int, float, npt.NDArray[Any])
