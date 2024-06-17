from __future__ import annotations

import importlib.metadata

from searvey._coops_api import fetch_coops_station
from searvey._ioc_api import fetch_ioc_station
from searvey.coops import get_coops_stations
from searvey.ioc import get_ioc_data
from searvey.ioc import get_ioc_stations
from searvey.stations import get_stations
from searvey.stations import Provider
from searvey.usgs import get_usgs_stations
from searvey._ndbc_api import get_ndbc_stations
from searvey._ndbc_api import fetch_ndbc_stations_data

__version__ = importlib.metadata.version(__name__)


__all__: list[str] = [
    "fetch_coops_station",
    "fetch_ioc_station",
    "fetch_ndbc_stations_data",
    "get_coops_stations",
    "get_ioc_data",
    "get_ioc_stations",
    "get_stations",
    "get_usgs_stations",
    "get_ndbc_stations",
    "Provider",
    "__version__",
]
