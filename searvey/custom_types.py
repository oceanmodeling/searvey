from __future__ import annotations

import datetime
import typing as T
from typing import TypeVar

import numpy as np
import numpy.typing as npt
import pandas as pd
from typing_extensions import TypeAlias


StrDict: TypeAlias = T.Dict[str, T.Any]
ScalarOrArray = TypeVar("ScalarOrArray", int, float, npt.NDArray[T.Any])
DateTimeLike: TypeAlias = T.Union[str, datetime.date, datetime.datetime, pd.Timestamp]
DatetimeLike: TypeAlias = T.Union[str, datetime.date, pd.Timestamp, datetime.datetime, np.datetime64]
