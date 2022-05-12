from abc import ABC
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any
from typing import Dict

from pandas import DataFrame
from shapely.geometry import Point
from xarray import Dataset


class StationDataProduct(Enum):
    pass


class StationDataInterval(Enum):
    pass


class StationDatum(Enum):
    pass


class StationStatus(Enum):
    ACTIVE = "active"
    DISCONTINUED = "discontinued"


class Station(ABC):
    """
    abstraction of a specific data station
    """

    id: str
    location: Point

    def __init__(self, id: str, location: Point):  # pylint: disable=redefined-builtin
        self.id = id
        self.location = location

    @abstractmethod
    def product(
        self,
        product: StationDataProduct,
        start_date: datetime,
        end_date: datetime = None,
        interval: StationDataInterval = None,
        datum: StationDatum = None,
    ) -> Dataset:
        """
        retrieve data for the current station within the specified parameters

        :param product: name of data product
        :param start_date: start date
        :param end_date: end date
        :param interval: time interval of data
        :param datum: vertical datum
        :return: data for the current station within the specified parameters
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        return f'{self.__class__.__name__} - "{self.id}" {self.location}'


class StationQuery(ABC):
    """
    abstraction of an individual station data query
    """

    station_id: str
    product: StationDataProduct
    start_date: datetime
    end_date: datetime
    interval: StationDataInterval
    datum: StationDatum

    def __init__(
        self,
        station_id: str,
        product: StationDataProduct,
        start_date: datetime,
        end_date: datetime = None,
        interval: StationDataInterval = None,
        datum: StationDatum = None,
    ):
        self.station_id = station_id
        self.product = product
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.datum = datum

    @property
    @abstractmethod
    def query(self) -> Dict[str, Any]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def data(self) -> DataFrame:
        """
        :return: data for the current query parameters
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        return f'{self.__class__.__name__} - {self.product} at station "{self.station_id}" between {self.start_date} and {self.end_date} over {self.interval} in {self.datum}'
