import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import abc
from typing import List
from typing import Union
from searvey.custom_types import DatetimeLike
import httpx
import multifutures
import pandas as pd

from ._common import _fetch_url, _resolve_end_date, _resolve_http_client, _resolve_rate_limit, _resolve_start_date, _to_utc
from .custom_types import DatetimeLike

logger = logging.getLogger(__name__)

BASE_URL = "https://rivergages.mvr.usace.army.mil/watercontrol/webservices/rest/webserviceWaterML.cfc?method=RGWML&meth=getValues&location={location}&site={site}&variable={variable}&beginDate={begin_date}&endDate={end_date}&authToken=RiverGages"

def _parse_xml_data(content: str, station_id: str) -> pd.DataFrame:
    try:
        namespace = {'wml': 'http://www.cuahsi.org/waterML/1.0/'}
        root = ET.fromstring(content)
        values_element = root.find(".//wml:values", namespaces=namespace)

        if values_element is None:
            logger.warning(f"{station_id}: No 'values' element found in the XML.")
            return pd.DataFrame()

        data = []
        for value_element in values_element.findall("wml:value", namespaces=namespace):
            date_time = value_element.get("dateTime")
            value = value_element.text
            date_time_obj = datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%S")
            data.append({'time': date_time_obj, 'value': float(value)})

        df = pd.DataFrame(data)
        df.set_index('time', inplace=True)
        df.index = pd.to_datetime(df.index, utc=True)
        df.attrs["station_id"] = f"USACE-{station_id}"
        return df
    except ET.ParseError:
        logger.error(f"{station_id}: Failed to parse XML data.")
        return pd.DataFrame()

def _generate_urls(
    station_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[str]:
    if end_date < start_date:
        raise ValueError(f"'end_date' must be after 'start_date': {end_date} vs {start_date}")
    if end_date == start_date:
        return []

    url = BASE_URL.format(
        location=station_id,
        site=station_id,
        variable="HG",
        begin_date=start_date.strftime("%Y-%m-%dT%H:%M"),
        end_date=end_date.strftime("%Y-%m-%dT%H:%M")
    )
    return [url]

def _retrieve_usace_data(
    station_ids: abc.Collection[str],
    start_dates: abc.Collection[pd.Timestamp],
    end_dates: abc.Collection[pd.Timestamp],
    rate_limit: multifutures.RateLimit,
    http_client: httpx.Client,
    executor: multifutures.ExecutorProtocol | None,
) -> list[multifutures.FutureResult]:
    kwargs = []
    for station_id, start_date, end_date in zip(station_ids, start_dates, end_dates):
        for url in _generate_urls(station_id=station_id, start_date=start_date, end_date=end_date):
            logger.info("USACE-%s: Starting scraping: %s - %s", station_id, start_date, end_date)
            if url:
                kwargs.append(
                    dict(
                        station_id=station_id,
                        url=url,
                        client=http_client,
                        rate_limit=rate_limit,
                    ),
                )
    with http_client:
        logger.debug("Starting data retrieval")
        results = multifutures.multithread(
            func=_fetch_url, func_kwargs=kwargs, check=False, executor=executor
        )
        logger.debug("Finished data retrieval")
    return results


def _fetch_usace(
    station_ids: abc.Collection[str],
    start_dates: Union[DatetimeLike, List[DatetimeLike]] = None,
    end_dates: Union[DatetimeLike, List[DatetimeLike]] = None,
    *,
    rate_limit: multifutures.RateLimit | None,
    http_client: httpx.Client | None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None,
    multithreading_executor: multifutures.ExecutorProtocol | None,
) -> dict[str, pd.DataFrame]:
    rate_limit = _resolve_rate_limit(rate_limit)
    http_client = _resolve_http_client(http_client)

    now = pd.Timestamp.now("utc")

    start_dates = [start_dates] if not isinstance(start_dates, list) else start_dates
    end_dates = [end_dates] if not isinstance(end_dates, list) else end_dates

    #we get the first index because the output is (DatetimeIndex(['2020-04-05'], dtype='datetime64[ns]', freq=None)
    start_dates = [_resolve_start_date(now, date)[0] for date in start_dates]
    end_dates = [_resolve_end_date(now, date)[0] for date in end_dates]

    usace_responses = _retrieve_usace_data(
        station_ids=station_ids,
        start_dates=start_dates,
        end_dates=end_dates,
        rate_limit=rate_limit,
        http_client=http_client,
        executor=multithreading_executor,
    )

    dataframes = {}
    for response in usace_responses:
        station_id = response.kwargs["station_id"]
        if response.exception:
            logger.error(f"USACE-{station_id}: Failed to retrieve data. Error: {response.exception}")
            continue
        df = _parse_xml_data(response.result, station_id)
        if not df.empty:
            dataframes[station_id] = df
        else:
            logger.warning(f"USACE-{station_id}: No data retrieved or parsed.")

    return dataframes

def fetch_usace_station(
    station_id: str,
    start_date: DatetimeLike | None = None,
    end_date: DatetimeLike | None = None,
    *,
    rate_limit: multifutures.RateLimit | None = None,
    http_client: httpx.Client | None = None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None = None,
    multithreading_executor: multifutures.ExecutorProtocol | None = None,
) -> pd.DataFrame:
    """
    Make a query to the USACE API for river gauge data for ``station_id``
    and return the results as a ``pandas.DataFrame``.

    :param station_id: The station identifier.
    :param start_date: The starting date of the query.
    :param end_date: The finishing date of the query.
    :param rate_limit: The rate limit for making requests to the USACE servers.
    :param http_client: The ``httpx.Client``, this should have the parameter verify=False.
    :param multiprocessing_executor
    :param multithreading_executor
    """
    logger.info("USACE-%s: Starting scraping: %s - %s", station_id, start_date, end_date)
    try:
        df = _fetch_usace(
            station_ids=[station_id],
            start_dates=start_date,
            end_dates=end_date,
            rate_limit=rate_limit,
            http_client=http_client,
            multiprocessing_executor=multiprocessing_executor,
            multithreading_executor=multithreading_executor,
        ).get(station_id, pd.DataFrame())
    except Exception as e:
        logger.error(f"USACE-{station_id}: An error occurred while fetching data: {str(e)}")
        df = pd.DataFrame()

    if df.empty:
        logger.warning(f"USACE-{station_id}: No data retrieved for the specified period.")
    else:
        logger.info("USACE-%s: Finished scraping: %s - %s", station_id, start_date, end_date)

    return df