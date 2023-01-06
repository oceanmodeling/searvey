from __future__ import annotations

from typing import Any
from typing import Dict
from typing import TypeVar

import numpy.typing as npt


StrDict = Dict[str, Any]
ScalarOrArray = TypeVar("ScalarOrArray", int, float, npt.NDArray[Any])
