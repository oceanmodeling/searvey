from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict

from pandas import DataFrame
from pyproj import CRS
from shapely.geometry import Point
from xarray import Dataset


class Station(ABC):
    """
    abstraction of a specific data station
    """

    id: str
    location: Point

    @abstractmethod
    def product(
            self,
            product: str,
            start_date: datetime,
            end_date: datetime = None,
            datum: CRS = None,
            interval: timedelta = None,
    ) -> Dataset:
        """
        retrieve data for the current station within the specified parameters

        :param product: name of data product
        :param start_date: start date
        :param end_date: end date
        :param datum: vertical datum
        :param interval: time interval of data
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
    start_date: datetime
    end_date: datetime
    product: str
    datum: CRS
    interval: timedelta

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
