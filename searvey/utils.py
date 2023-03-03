from __future__ import annotations

import datetime
import itertools
from typing import Dict
from typing import Final
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import Union

import pandas as pd
import xarray as xr
from shapely.geometry import box
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from . import models
from .custom_types import DateLike
from .custom_types import ScalarOrArray

_T = TypeVar("_T")
_U = TypeVar("_U")

TODAY: Final = "TODAY"


# https://gis.stackexchange.com/questions/201789/verifying-formula-that-will-convert-longitude-0-360-to-180-to-180
# lon1 is the longitude varying from -180 to 180 or 180W-180E
# lon3 is the longitude variable from 0 to 360 (all positive)


def lon1_to_lon3(lon1: ScalarOrArray) -> ScalarOrArray:
    return lon1 % 360


def lon3_to_lon1(lon3: ScalarOrArray) -> ScalarOrArray:
    return ((lon3 + 180) % 360) - 180


def resolve_date(date: DateLike) -> datetime.date:
    if date == TODAY:
        date = datetime.date.today()
    else:
        date = pd.Timestamp(date).date()
    return date


def get_region_from_bbox_corners(
    lon_min: Union[float, None] = None,
    lon_max: Union[float, None] = None,
    lat_min: Union[float, None] = None,
    lat_max: Union[float, None] = None,
    symmetric: bool = True,
) -> Polygon:
    """Return a ``shapely.geometry.Polygon`` from the corners of a Bounding Box."""
    bbox_kwargs = {
        "lon_min": lon_min,
        "lon_max": lon_max,
        "lat_min": lat_min,
        "lat_max": lat_max,
    }
    if symmetric:
        klass = models.SymmetricBBox
    else:
        klass = models.AsymmetricBBox

    # Create the BBox (i.e. validate input values)
    # Not the most beautiful code in the world, but Pydantic needs keyword arguments
    # and we want to only pass arguments that have actual values and not None
    # moreover, we create a new object because mypy was complaining about redefining
    filtered_bbox_kwargs: Dict[str, float] = {k: v for (k, v) in bbox_kwargs.items() if v}
    bbox = klass(**filtered_bbox_kwargs)

    # Create the region
    region = box(
        minx=bbox.lon_min,
        miny=bbox.lat_min,
        maxx=bbox.lon_max,
        maxy=bbox.lat_max,
    )
    return region


def get_region(
    region: Union[Union[Polygon, MultiPolygon], None] = None,
    lon_min: Union[float, None] = None,
    lon_max: Union[float, None] = None,
    lat_min: Union[float, None] = None,
    lat_max: Union[float, None] = None,
    symmetric: bool = True,
) -> Union[Polygon, None]:
    """
    Return a shapely Region

    The region can either be a ``shapely.geometry`` object or it can be defined
    from the corners of a Bounding Box.

    If both a ``region`` and the corners are specified then an Exception is raised.

    - If ``symmetric`` is ``False``, then it is assumed that Longitude ∈ [0, 360).
    - If ``symmetric`` is ``True``, then it is assumed that Longitude ∈ [-180, 180).
    """
    if any((lon_min, lon_max, lat_min, lat_max)):
        # Ensure that only one of region and bbox have been specified, not both
        if region:
            msg = "You must specify either `region` or the `BBox` corners, not both"
            raise ValueError(msg)

        region = get_region_from_bbox_corners(
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
            symmetric=symmetric,
        )
    return region


# https://docs.python.org/3/library/itertools.html#itertools-recipes
# https://github.com/more-itertools/more-itertools/blob/2ff5943d76afa4591b5b4ae8cb4524578d365f67/more_itertools/recipes.pyi#L42-L48
def grouper(
    iterable: Iterable[_T],
    n: int,
    *,
    incomplete: str = "fill",
    fillvalue: Union[_U, None] = None,
) -> Iterator[Tuple[Union[_T, _U], ...]]:
    """Collect data into non-overlapping fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, fillvalue='x') --> ABC DEF Gxx
    # grouper('ABCDEFG', 3, incomplete='strict') --> ABC DEF ValueError
    # grouper('ABCDEFG', 3, incomplete='ignore') --> ABC DEF
    args = [iter(iterable)] * n
    if incomplete == "fill":
        return itertools.zip_longest(*args, fillvalue=fillvalue)
    if incomplete == "strict":
        return zip(*args, strict=True)  # type: ignore[call-overload]
    if incomplete == "ignore":
        return zip(*args)
    else:
        raise ValueError("Expected fill, strict, or ignore")


def merge_datasets(datasets: List[xr.Dataset], size: int = 5) -> List[xr.Dataset]:
    datasets = [xr.merge(g for g in group if g) for group in grouper(datasets, size)]
    return datasets
