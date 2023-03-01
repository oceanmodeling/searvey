"""
interface with the U.S. National Oceanic and Atmospheric Administration (NOAA) Center for Operational Oceanographic Products and Services (CO-OPS) API
https://api.tidesandcurrents.noaa.gov/api/prod/
"""
from __future__ import annotations

import json
import logging
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Union

import geopandas
import numpy
import pandas
import requests
import shapely
import xarray
from bs4 import BeautifulSoup
from bs4 import element
from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import Series
from shapely.geometry import MultiPolygon
from shapely.geometry import Point
from shapely.geometry import Polygon
from xarray import Dataset


logger = logging.getLogger(__name__)


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

    def __init__(self, id: str, location: Point):
        self.id = id
        self.location = location

    @abstractmethod
    def product(
        self,
        product: StationDataProduct,
        start_date: datetime,
        end_date: datetime | None = None,
        interval: StationDataInterval | None = None,
        datum: StationDatum | None = None,
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
        end_date: datetime | None = None,
        interval: StationDataInterval | None = None,
        datum: StationDatum | None = None,
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


class COOPS_Product(StationDataProduct):  # noqa: N801
    WATER_LEVEL = (
        "water_level"
        # Preliminary or verified water levels, depending on availability.
    )
    AIR_TEMPERATURE = "air_temperature"  # Air temperature as measured at the station.
    WATER_TEMPERATURE = "water_temperature"  # Water temperature as measured at the station.
    WIND = "wind"  # Wind speed, direction, and gusts as measured at the station.
    AIR_PRESSURE = "air_pressure"  # Barometric pressure as measured at the station.
    AIR_GAP = "air_gap"  # Air Gap (distance between a bridge and the water's surface) at the station.
    CONDUCTIVITY = "conductivity"  # The water's conductivity as measured at the station.
    VISIBILITY = (
        "visibility"  # Visibility from the station's visibility sensor. A measure of atmospheric clarity.
    )
    HUMIDITY = "humidity"  # Relative humidity as measured at the station.
    SALINITY = "salinity"  # Salinity and specific gravity data for the station.
    HOURLY_HEIGHT = "hourly_height"  # Verified hourly height water level data for the station.
    HIGH_LOW = "high_low"  # Verified high/low water level data for the station.
    DAILY_MEAN = "daily_mean"  # Verified daily mean water level data for the station.
    MONTHLY_MEAN = "monthly_mean"  # Verified monthly mean water level data for the station.
    ONE_MINUTE_WATER_LEVEL = (
        "one_minute_water_level"
        # One minute water level data for the station.
    )
    PREDICTIONS = "predictions"  # 6 minute predictions water level data for the station.*
    DATUMS = "datums"  # datums data for the stations.
    CURRENTS = "currents"  # Currents data for currents stations.
    CURRENTS_PREDICTIONS = (
        "currents_predictions"
        # Currents predictions data for currents predictions stations.
    )


class COOPS_Interval(StationDataInterval):  # noqa: N801
    H = "h"  # Hourly Met data and harmonic predictions will be returned
    HILO = "hilo"  # High/Low tide predictions for all stations.


class COOPS_TidalDatum(StationDatum):  # noqa: N801
    CRD = "CRD"  # Columbia River Datum
    IGLD = "IGLD"  # International Great Lakes Datum
    LWD = "LWD"  # Great Lakes Low Water Datum (Chart Datum)
    MHHW = "MHHW"  # Mean Higher High Water
    MHW = "MHW"  # Mean High Water
    MTL = "MTL"  # Mean Tide Level
    MSL = "MSL"  # Mean Sea Level
    MLW = "MLW"  # Mean Low Water
    MLLW = "MLLW"  # Mean Lower Low Water
    NAVD = "NAVD"  # North American Vertical Datum
    STND = "STND"  # Station Datum


class COOPS_VelocityType(Enum):  # noqa: N801
    SPEED_DIR = "speed_dir"  # Return results for speed and dirction
    DEFAULT = "default"  # Return results for velocity major, mean flood direction and mean ebb dirction


class COOPS_Units(Enum):  # noqa: N801
    METRIC = "metric"
    ENGLISH = "english"


class COOPS_TimeZone(Enum):  # noqa: N801
    GMT = "gmt"  # Greenwich Mean Time
    LST = "lst"  # Local Standard Time. The time local to the requested station.
    LST_LDT = "lst_ldt"  # Local Standard/Local Daylight Time. The time local to the requested station.


class COOPS_Station(Station):  # noqa: N801
    """
    a specific CO-OPS station
    """

    def __init__(self, id: Union[int, str]):
        """
        :param id: NOS ID, NWS ID, or station name

        from NOS ID:

        >>> COOPS_Station(1612480)
        COOPS_Station(1612480)

        from NWS ID:

        >>> COOPS_Station('OOUH1')
        COOPS_Station(1612340)

        from station name:

        >>> COOPS_Station('San Mateo Bridge')
        COOPS_Station(9414458)
        """

        stations = coops_stations()
        if id in stations.index:
            metadata = stations.loc[id]
        elif id in stations["nws_id"].values:
            metadata = stations[stations["nws_id"] == id]
        elif id in stations["name"].values:
            metadata = stations[stations["name"] == id]
        else:
            metadata = None

        if metadata is None or len(metadata) == 0:
            raise ValueError(f'station with "{id}" not found')

        removed = metadata["removed"]
        if isinstance(removed, Series):
            self.__active = pandas.isna(removed).any()
            removed = pandas.unique(removed.dropna().sort_values(ascending=False))
        else:
            self.__active = pandas.isna(removed)
        self.__removed = removed

        if isinstance(metadata, DataFrame):
            metadata = metadata.iloc[0]

        Station.__init__(self, id=str(metadata.name), location=metadata.geometry)

        self.nws_id = metadata["nws_id"]
        self.state = metadata["state"]
        self.name = metadata["name"]

        self.__query = None

    @property
    def current(self) -> bool:
        return self.__active

    @property
    def removed(self) -> Series:
        return self.__removed

    @property
    @lru_cache(maxsize=1)
    def constituents(self) -> DataFrame:
        """
        :return: table of tidal constituents for the current station
        """

        url = f"https://tidesandcurrents.noaa.gov/harcon.html?id={self.id}"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, features="html.parser")
        table = soup.find_all("table", {"class": "table table-striped"})
        if len(table) > 0:
            table = table[0]
            columns = [field.text for field in table.find("thead").find("tr").find_all("th")]
            constituents = []
            for row in table.find_all("tr")[1:]:
                constituents.append([entry.text for entry in row.find_all("td")])
            constituents = DataFrame(constituents, columns=columns)
            constituents.rename(columns={"Constituent #": "#"}, inplace=True)
            constituents = constituents.astype(
                {
                    "#": numpy.int32,
                    "Amplitude": numpy.float64,
                    "Phase": numpy.float64,
                    "Speed": numpy.float64,
                }
            )
        else:
            constituents = DataFrame(columns=["#", "Amplitude", "Phase", "Speed"])

        constituents.set_index("#", inplace=True)
        return constituents

    def product(
        self,
        product: COOPS_Product,
        start_date: datetime,
        end_date: datetime | None = None,
        interval: COOPS_Interval | None = None,
        datum: COOPS_TidalDatum | None = None,
    ) -> Dataset:
        """
        retrieve data for the current station within the specified parameters

        :param product: CO-OPS product
        :param start_date: start date
        :param end_date: end date
        :param interval: time interval of data
        :param datum: tidal datum
        :return: data for the current station within the specified parameters

        >>> station = COOPS_Station(8632200)
        >>> station.product('water_level', start_date=datetime(2018, 9, 13), end_date=datetime(2018, 9, 16, 12))
        <xarray.Dataset>
        Dimensions:  (nos_id: 1, t: 841)
        Coordinates:
          * nos_id   (nos_id) int64 8632200
          * t        (t) datetime64[ns] 2018-09-13 ... 2018-09-16T12:00:00
            nws_id   (nos_id) <U5 'KPTV2'
            x        (nos_id) float64 -76.0
            y        (nos_id) float64 37.16
        Data variables:
            v        (nos_id, t) float32 1.67 1.694 1.73 1.751 ... 1.597 1.607 1.605
            s        (nos_id, t) float32 0.026 0.027 0.034 0.03 ... 0.018 0.019 0.021
            f        (nos_id, t) object '0,0,0,0' '0,0,0,0' ... '0,0,0,0' '0,0,0,0'
            q        (nos_id, t) object 'v' 'v' 'v' 'v' 'v' 'v' ... 'v' 'v' 'v' 'v' 'v'
        """

        logger.debug("Downloading: %s", self)
        if self.__query is None:
            self.__query = COOPS_Query(
                station=int(self.id),
                product=product,
                start_date=start_date,
                end_date=end_date,
                datum=datum,
                interval=interval,
            )
        else:
            if start_date is not None:
                self.__query.start_date = start_date
            if end_date is not None:
                self.__query.end_date = end_date
            if product is not None:
                self.__query.product = product
            if datum is not None:
                self.__query.datum = datum
            if interval is not None:
                self.__query.interval = interval

        data = self.__query.data

        data["nos_id"] = self.id
        data.set_index(["nos_id", data.index], inplace=True)

        data = data.to_xarray()

        if len(data["t"]) > 0:
            data = data.assign_coords(
                nws_id=("nos_id", [self.nws_id]),
                x=("nos_id", [self.location.x]),
                y=("nos_id", [self.location.y]),
            )
        else:
            data = data.assign_coords(
                nws_id=("nos_id", []),
                x=("nos_id", []),
                y=("nos_id", []),
            )

        return data

    def __str__(self) -> str:
        return f"{self.__class__.__name__} - {self.id} ({self.name}) - {self.location}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.id})"


class COOPS_Query(StationQuery):  # noqa: N801
    """
    abstraction of an individual query to the CO-OPS API
    https://api.tidesandcurrents.noaa.gov/api/prod/
    """

    URL = "https://tidesandcurrents.noaa.gov/api/datagetter?"

    def __init__(
        self,
        station: int,
        product: COOPS_Product,
        start_date: datetime,
        end_date: datetime | None = None,
        datum: COOPS_TidalDatum | None = None,
        units: COOPS_Units | None = None,
        time_zone: COOPS_TimeZone | None = None,
        interval: COOPS_Interval | None = None,
    ):
        """
        instantiate a new query with the specified parameters

        :param station: NOS station ID
        :param product: CO-OPS product
        :param start_date: start date
        :param end_date: end date
        :param datum: station datum
        :param units: one of `metric`, `english`
        :param time_zone: time zone of data
        :param interval: time interval of data
        :return: data for the current station within the specified parameters

        >>> COOPS_Query(1612480, start_date='2022-01-01', end_date='2022-01-03')
        COOPS_Query(1612480, datetime.datetime(2022, 1, 1, 0, 0), datetime.datetime(2022, 1, 3, 0, 0), 'water_level', 'MLLW', 'metric', 'gmt', 'h')
        """

        if isinstance(station, COOPS_Station):
            station = station.id
        if end_date is None:
            end_date = datetime.today()
        if datum is None:
            datum = COOPS_TidalDatum.STND
        if units is None:
            units = COOPS_Units.METRIC
        if time_zone is None:
            time_zone = COOPS_TimeZone.GMT
        if interval is None:
            interval = COOPS_Interval.H

        StationQuery.__init__(
            self, station_id=station, product=product, start_date=start_date, end_date=end_date  # type: ignore[arg-type]
        )

        self.station_id = station
        self.product = product
        self.start_date = start_date
        self.end_date = end_date
        self.datum = datum
        self.units = units
        self.time_zone = time_zone
        self.interval = interval

        self.__previous_query = None
        self.__error = None
        self.__data = None

    @property
    def start_date(self) -> datetime:
        return self.__start_date

    @start_date.setter
    def start_date(self, start_date: datetime) -> None:
        self.__start_date = pandas.to_datetime(start_date)

    @property
    def end_date(self) -> datetime:
        return self.__end_date

    @end_date.setter
    def end_date(self, end_date: datetime) -> None:
        self.__end_date = pandas.to_datetime(end_date)

    @property
    def product(self) -> COOPS_Product | None:
        return self.__product

    @product.setter
    def product(self, product: COOPS_Product | None) -> None:
        if product is None:
            self.__product = None
        elif isinstance(product, COOPS_Product):
            self.__product = product
        else:
            self.__product = COOPS_Product[product.upper()]

    @property
    def datum(self) -> COOPS_TidalDatum | None:
        return self.__datum

    @datum.setter
    def datum(self, datum: COOPS_TidalDatum | None) -> None:
        if datum is None:
            self.__datum = None
        elif isinstance(datum, COOPS_TidalDatum):
            self.__datum = datum
        else:
            self.__datum = COOPS_TidalDatum[datum.upper()]

    @property
    def units(self) -> COOPS_Units | None:
        return self.__units

    @units.setter
    def units(self, units: COOPS_Units | None) -> None:
        if units is None:
            self.__units = None
        elif isinstance(units, COOPS_Units):
            self.__units = units
        else:
            self.__units = COOPS_Units[units.upper()]

    @property
    def time_zone(self) -> COOPS_TimeZone | None:
        return self.__time_zone

    @time_zone.setter
    def time_zone(self, time_zone: COOPS_TimeZone | None) -> None:
        if time_zone is None:
            self.__time_zone = None
        elif isinstance(time_zone, COOPS_TimeZone):
            self.__time_zone = time_zone
        else:
            self.__time_zone = COOPS_TimeZone[time_zone.upper()]

    @property
    def interval(self) -> COOPS_Interval | None:
        return self.__interval

    @interval.setter
    def interval(self, interval: COOPS_Interval | None) -> None:
        if interval is None:
            self.__interval = None
        elif isinstance(interval, COOPS_Interval):
            self.__interval = interval
        else:
            self.__interval = COOPS_Interval[interval.upper()]

    @property
    def query(self) -> Dict[str, Any]:
        self.__error = None

        product = self.product
        if isinstance(product, Enum):
            product = product.value
        start_date = self.start_date
        if start_date is not None and not isinstance(start_date, str):
            start_date = f"{self.start_date:%Y%m%d %H:%M}"
        datum = self.datum
        if isinstance(datum, Enum):
            datum = datum.value
        units = self.units
        if isinstance(units, Enum):
            units = units.value
        time_zone = self.time_zone
        if isinstance(time_zone, Enum):
            time_zone = time_zone.value
        interval = self.interval
        if isinstance(interval, Enum):
            interval = interval.value

        return {
            "station": self.station_id,
            "product": product,
            "begin_date": start_date,
            "end_date": f"{self.end_date:%Y%m%d %H:%M}",
            "datum": datum,
            "units": units,
            "time_zone": time_zone,
            "interval": interval,
            "format": "json",
            "application": "noaa/nos/csdl/stormevents",
        }

    @property
    def data(self) -> DataFrame:
        """
        :return: data for the current query parameters

        >>> query = COOPS_Query(1612480, 'water_level', start_date='2022-01-01', end_date='2022-01-03')
        >>> query.data
                                 v      s        f  q
        t
        2022-01-01 00:00:00  1.193  0.002  0,0,0,0  v
        2022-01-01 00:06:00  1.180  0.002  0,0,0,0  v
        2022-01-01 00:12:00  1.167  0.002  0,0,0,0  v
        2022-01-01 00:18:00  1.156  0.003  0,0,0,0  v
        2022-01-01 00:24:00  1.147  0.003  0,0,0,0  v
                            ...    ...      ... ..
        2022-01-02 23:36:00  1.229  0.004  0,0,0,0  v
        2022-01-02 23:42:00  1.219  0.003  0,0,0,0  v
        2022-01-02 23:48:00  1.223  0.004  0,0,0,0  v
        2022-01-02 23:54:00  1.217  0.004  0,0,0,0  v
        2022-01-03 00:00:00  1.207  0.002  0,0,0,0  v
        [481 rows x 4 columns]
        """

        if self.__previous_query is None or self.query != self.__previous_query:
            response = requests.get(self.URL, params=self.query)
            data = response.json()
            fields = ["t", "v", "s", "f", "q"]
            if "error" in data or "data" not in data:
                if "error" in data:
                    self.__error = data["error"]["message"]
                data = DataFrame(columns=fields)
            else:
                data = DataFrame(data["data"], columns=fields)
                data[data == ""] = numpy.nan
                data = data.astype(
                    {"v": numpy.float32, "s": numpy.float32, "f": "string", "q": "string"},
                    errors="ignore",
                )
                data["t"] = pandas.to_datetime(data["t"])

            data.set_index("t", inplace=True)
            self.__data = data
        return self.__data

    def __repr__(self) -> str:
        attributes = (
            self.station_id,
            self.start_date,
            self.end_date,
            self.product.value,  # type: ignore[union-attr]
            self.datum.value,  # type: ignore[union-attr]
            self.units.value,  # type: ignore[union-attr]
            self.time_zone.value,  # type: ignore[union-attr]
            self.interval.value,  # type: ignore[union-attr]
        )
        return f'{self.__class__.__name__}({", ".join(repr(value) for value in attributes)})'


@lru_cache(maxsize=1)
def __coops_stations_html_tables() -> element.ResultSet:
    url = "https://access.co-ops.nos.noaa.gov/nwsproducts.html?type=current"
    logger.debug("Downloading: %s", url)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, features="html.parser")
    return soup.find_all("div", {"class": "table-responsive"})


@lru_cache(maxsize=1)
def coops_stations(station_status: StationStatus | None = None) -> GeoDataFrame:
    """
    retrieve a list of CO-OPS stations with associated metadata

    :param station_status: one of ``active`` or ``discontinued``
    :return: data frame of stations

    >>> coops_stations()
            nws_id                              name state        status                                            removed                     geometry
    nos_id
    1600012  46125                         QREB buoy              active                                               <NA>   POINT (122.62500 37.75000)
    8735180  DILA1                    Dauphin Island    AL        active  2019-07-18 10:00:00,2018-07-30 16:40:00,2017-0...   POINT (-88.06250 30.25000)
    8557380  LWSD1                             Lewes    DE        active  2019-08-01 00:00:00,2018-06-18 00:00:00,2017-0...   POINT (-75.12500 38.78125)
    8465705  NWHC3                         New Haven    CT        active  2019-08-18 14:55:00,2019-08-18 14:54:00,2018-0...   POINT (-72.93750 41.28125)
    9439099  WAUO3                             Wauna    OR        active  2019-08-19 22:59:00,2014-06-20 21:30:00,2013-0...  POINT (-123.43750 46.15625)
    ...        ...                               ...   ...           ...                                                ...                          ...
    8448725  MSHM3               Menemsha Harbor, MA    MA  discontinued  2013-09-26 23:59:00,2013-09-26 00:00:00,2012-0...   POINT (-70.75000 41.34375)
    8538886  TPBN4             Tacony-Palmyra Bridge    NJ  discontinued  2013-11-11 00:01:00,2013-11-11 00:00:00,2012-0...   POINT (-75.06250 40.00000)
    9439011  HMDO3                           Hammond    OR  discontinued  2014-08-13 00:00:00,2011-04-12 23:59:00,2011-0...  POINT (-123.93750 46.18750)
    8762372  LABL1  East Bank 1, Norco, B. LaBranche    LA  discontinued  2012-11-05 10:38:00,2012-11-05 10:37:00,2012-1...   POINT (-90.37500 30.04688)
    8530528  CARN4       CARLSTADT, HACKENSACK RIVER    NJ  discontinued            1994-11-12 23:59:00,1994-11-12 00:00:00   POINT (-74.06250 40.81250)
    [436 rows x 6 columns]
    >>> coops_stations(station_status='active')
            nws_id                          name state  status removed                     geometry
    nos_id
    1600012  46125                     QREB buoy        active    <NA>   POINT (122.62500 37.75000)
    1611400  NWWH1                    Nawiliwili    HI  active    <NA>  POINT (-159.37500 21.95312)
    1612340  OOUH1                      Honolulu    HI  active    <NA>  POINT (-157.87500 21.31250)
    1612480  MOKH1                      Mokuoloe    HI  active    <NA>  POINT (-157.75000 21.43750)
    1615680  KLIH1       Kahului, Kahului Harbor    HI  active    <NA>  POINT (-156.50000 20.89062)
    ...        ...                           ...   ...     ...     ...                          ...
    9759394  MGZP4                      Mayaguez    PR  active    <NA>   POINT (-67.18750 18.21875)
    9759938  MISP4                   Mona Island        active    <NA>   POINT (-67.93750 18.09375)
    9761115  BARA9                       Barbuda        active    <NA>   POINT (-61.81250 17.59375)
    9999530  FRCB6  Bermuda, Ferry Reach Channel        active    <NA>   POINT (-64.68750 32.37500)
    9999531               Calcasieu Test Station    LA  active    <NA>   POINT (-93.31250 29.76562)
    [365 rows x 6 columns]
    >>> coops_stations(station_status='discontinued')
            nws_id                               name state        status                                            removed                     geometry
    nos_id
    8530528  CARN4        CARLSTADT, HACKENSACK RIVER    NJ  discontinued            1994-11-12 23:59:00,1994-11-12 00:00:00   POINT (-74.06250 40.81250)
    9415064  NCHC1         ANTIOCH, SAN JOAQUIN RIVER    CA  discontinued            1997-03-03 23:59:00,1997-03-03 00:00:00  POINT (-121.81250 38.03125)
    9415316  RVXC1                          Rio Vista    CA  discontinued            1997-03-04 23:59:00,1997-03-04 00:00:00  POINT (-121.68750 38.15625)
    9440572  ILWW1            JETTY A, COLUMBIA RIVER    WA  discontinued                                1997-04-11 23:00:00  POINT (-124.06250 46.28125)
    8760551  SPSL1                         South Pass    LA  discontinued  2000-09-26 23:59:00,2000-09-26 00:00:00,1998-1...   POINT (-89.12500 28.98438)
    ...        ...                                ...   ...           ...                                                ...                          ...
    8726667  MCYF1                 Mckay Bay Entrance    FL  discontinued  2020-05-20 00:00:00,2019-03-08 00:00:00,2017-0...   POINT (-82.43750 27.90625)
    8772447  FCGT2                           Freeport    TX  discontinued  2020-05-24 18:45:00,2018-10-10 21:50:00,2018-1...   POINT (-95.31250 28.93750)
    9087079  GBWW3                          Green Bay    WI  discontinued  2020-10-28 13:00:00,2007-08-06 23:59:00,2007-0...   POINT (-88.00000 44.53125)
    8770570  SBPT2                  Sabine Pass North    TX  discontinued  2021-01-18 00:00:00,2020-09-30 15:45:00,2020-0...   POINT (-93.87500 29.73438)
    8740166  GBRM6  Grand Bay NERR, Mississippi Sound    MS  discontinued  2022-04-07 00:00:00,2022-03-30 23:58:00,2015-1...   POINT (-88.37500 30.40625)
    [71 rows x 6 columns]
    """

    tables = __coops_stations_html_tables()

    status_tables = {
        StationStatus.ACTIVE: (0, "NWSTable"),
        StationStatus.DISCONTINUED: (1, "HistNWSTable"),
    }

    dataframes = {}
    for status, (table_index, table_id) in status_tables.items():
        table = tables[table_index]
        table = table.find("table", {"id": table_id}).find_all("tr")
        stations_columns = [field.text for field in table[0].find_all("th")]
        stations = DataFrame(
            [[value.text.strip() for value in station.find_all("td")] for station in table[1:]],
            columns=stations_columns,
        )
        stations.rename(
            columns={
                "NOS ID": "nos_id",
                "NWS ID": "nws_id",
                "Latitude": "y",
                "Longitude": "x",
                "State": "state",
                "Station Name": "name",
            },
            inplace=True,
        )
        stations = stations.astype(
            {
                "nos_id": numpy.int32,
                "nws_id": "string",
                "x": numpy.float32,
                "y": numpy.float32,
                "state": "string",
                "name": "string",
            },
            copy=False,
        )
        stations.set_index("nos_id", inplace=True)

        if status == StationStatus.DISCONTINUED:
            with (Path(__file__).parent / "us_states.json").open() as us_states_file:
                us_states = json.load(us_states_file)

            for name, abbreviation in us_states.items():
                stations.loc[stations["state"] == name, "state"] = abbreviation

            stations.rename(columns={"Removed Date/Time": "removed"}, inplace=True)

            stations["removed"] = pandas.to_datetime(stations["removed"]).astype("string")

            stations = (
                stations[~pandas.isna(stations["removed"])]
                .sort_values("removed", ascending=False)
                .groupby("nos_id")
                .aggregate(
                    {
                        "nws_id": "first",
                        "removed": ",".join,
                        "y": "first",
                        "x": "first",
                        "state": "first",
                        "name": "first",
                    }
                )
            )
        else:
            stations["removed"] = pandas.NA

        stations["status"] = status.value
        dataframes[status] = stations

    active_stations = dataframes[StationStatus.ACTIVE]
    discontinued_stations = dataframes[StationStatus.DISCONTINUED]
    discontinued_stations.loc[
        discontinued_stations.index.isin(active_stations.index), "status"
    ] = StationStatus.ACTIVE.value

    stations = pandas.concat(
        (
            active_stations[~active_stations.index.isin(discontinued_stations.index)],
            discontinued_stations,
        )
    )
    stations.loc[pandas.isna(stations["status"]), "status"] = StationStatus.ACTIVE.value
    stations.sort_values(["status", "removed"], na_position="first", inplace=True)

    if station_status is not None:
        if isinstance(station_status, StationStatus):
            station_status = station_status.value
        stations = stations[stations["status"] == station_status]

    return GeoDataFrame(
        stations[["nws_id", "name", "state", "status", "removed"]],
        geometry=geopandas.points_from_xy(stations["x"], stations["y"]),
    )


def coops_stations_within_region(
    region: Polygon | None = None,
    station_status: StationStatus | None = None,
) -> GeoDataFrame:
    """
    retrieve all stations within the specified region of interest

    :param region: polygon or multipolygon denoting region of interest
    :param station_status: one of ``active`` or ``discontinued``
    :return: data frame of stations within the specified region

    >>> import shapely
    >>> east_coast = shapely.geometry.box(-85, 25, -65, 45)
    >>> coops_stations_within_region(east_coast)
            nws_id                           name state        status                                            removed                    geometry
    nos_id
    8726412  MTBF1               Middle Tampa Bay              active                                               <NA>  POINT (-82.62500 27.65625)
    8726679  TSHF1              East Bay Causeway    FL        active                                               <NA>  POINT (-82.43750 27.92188)
    8726694  TPAF1          TPA Cruise Terminal 2    FL        active                                               <NA>  POINT (-82.43750 27.93750)
    9044036  FWNM4                     Fort Wayne    MI        active  2005-04-29 23:59:00,2005-04-29 00:00:00,2001-1...  POINT (-83.06250 42.31250)
    9075035  ESVM4                     Essexville    MI        active  2007-03-28 23:59:00,2007-03-28 00:00:00,2007-0...  POINT (-83.87500 43.62500)
    ...        ...                            ...   ...           ...                                                ...                         ...
    8720503  GCVF1  Red Bay Point, St Johns River    FL  discontinued  2017-10-07 20:54:00,2017-10-07 10:54:00,2017-1...  POINT (-81.62500 29.98438)
    8654400  CFPN7     Cape Hatteras Fishing Pier    NC  discontinued  2018-09-19 23:59:00,2003-09-18 23:59:00,2003-0...  POINT (-75.62500 35.21875)
    8720625  RCYF1     Racy Point, St Johns River    FL  discontinued  2019-08-05 14:00:00,2017-06-14 15:36:00,2017-0...  POINT (-81.56250 29.79688)
    8423898  FTPN3                     Fort Point    NH  discontinued  2020-04-13 00:00:00,2014-08-05 00:00:00,2012-0...  POINT (-70.68750 43.06250)
    8726667  MCYF1             Mckay Bay Entrance    FL  discontinued  2020-05-20 00:00:00,2019-03-08 00:00:00,2017-0...  POINT (-82.43750 27.90625)
    [164 rows x 6 columns]
    """

    stations = coops_stations(station_status=station_status)
    if region:
        return stations[stations.within(region)]
    else:
        return stations


def coops_stations_within_bounds(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    station_status: StationStatus | None = None,
) -> GeoDataFrame:
    return coops_stations_within_region(
        region=shapely.geometry.box(minx=minx, miny=miny, maxx=maxx, maxy=maxy),
        station_status=station_status,
    )


def coops_product_within_region(
    product: COOPS_Product,
    region: Union[Polygon, MultiPolygon],
    start_date: datetime,
    end_date: datetime | None = None,
    datum: COOPS_TidalDatum | None = None,
    interval: COOPS_Interval | None = None,
    station_status: StationStatus | None = None,
) -> Dataset:
    """
    retrieve CO-OPS data from within the specified region of interest

    :param product: CO-OPS product; one of ``water_level``, ``air_temperature``, ``water_temperature``, ``wind``, ``air_pressure``, ``air_gap``, ``conductivity``, ``visibility``, ``humidity``, ``salinity``, ``hourly_height``, ``high_low``, ``daily_mean``, ``monthly_mean``, ``one_minute_water_level``, ``predictions``, ``datums``, ``currents``, ``currents_predictions``
    :param region: polygon or multipolygon denoting region of interest
    :param start_date: start date of CO-OPS query
    :param end_date: start date of CO-OPS query
    :param datum: tidal datum
    :param interval: data time interval
    :param station_status: either ``active`` or ``discontinued``
    :return: array of data within the specified region

    >>> from datetime import datetime, timedelta
    >>> import shapely
    >>> east_coast = shapely.geometry.box(-85, 25, -65, 45)
    >>> coops_product_within_region('water_level', region=east_coast, start_date=datetime(2022, 4, 2, 12), end_date=datetime(2022, 4, 2, 12, 30))
    <xarray.Dataset>
    Dimensions:  (t: 6, nos_id: 111)
    Coordinates:
      * t        (t) datetime64[ns] 2022-04-02T12:00:00 ... 2022-04-02T12:30:00
      * nos_id   (nos_id) int64 9044036 9075035 9052076 ... 8516945 9052000 8638901
        nws_id   (nos_id) <U5 'FWNM4' 'ESVM4' 'OCTN6' ... 'KPTN6' 'CAVN6' 'CHBV2'
        x        (nos_id) float64 -83.06 -83.88 -78.75 ... -73.75 -76.31 -76.06
        y        (nos_id) float64 42.31 43.62 43.34 42.09 ... 40.81 44.12 37.03
    Data variables:
        v        (nos_id, t) float32 175.1 175.1 175.1 175.1 ... 2.084 2.089 2.096
        s        (nos_id, t) float32 0.0 0.0 0.0 0.0 0.0 ... 0.059 0.076 0.088 0.08
        f        (nos_id, t) object '0,0,0,0' '0,0,0,0' ... '1,0,0,0' '1,0,0,0'
        q        (nos_id, t) object 'p' 'p' 'p' 'p' 'p' 'p' ... 'p' 'p' 'p' 'p' 'p'
    """

    stations = coops_stations_within_region(region=region, station_status=station_status)
    station_data = [
        COOPS_Station(station).product(
            product=product,
            start_date=start_date,
            end_date=end_date,
            datum=datum,
            interval=interval,
        )
        for station in stations.index
    ]
    station_data = [station for station in station_data if len(station["t"]) > 0]
    return xarray.combine_nested(station_data, concat_dim="nos_id")


get_coops_stations = coops_stations_within_region
