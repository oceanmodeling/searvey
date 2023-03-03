from __future__ import annotations

import datetime
from typing import Any
from typing import Dict
from typing import TypeVar
from typing import Union

import numpy.typing as npt
import pandas as pd
from typing_extensions import TypeAlias  # "from typing" in Python 3.9+


StrDict: TypeAlias = Dict[str, Any]
DateLike: TypeAlias = Union[str, datetime.date, datetime.datetime, pd.Timestamp]

ScalarOrArray = TypeVar("ScalarOrArray", int, float, npt.NDArray[Any])
