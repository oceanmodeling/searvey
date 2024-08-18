import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import abc

import httpx
import multifutures
import pandas as pd

from ._common import _fetch_url, _resolve_end_date, _resolve_http_client, _resolve_rate_limit, _resolve_start_date, _to_utc
from .custom_types import DatetimeLike

logger = logging.getLogger(__name__)

BASE_URL = "https://rivergages.mvr.usace.army.mil/watercontrol/webservices/rest/webserviceWaterML.cfc?method=RGWML&meth=getValues&location={location}&site={site}&variable={variable}&beginDate={begin_date}&endDate={end_date}&authToken=RiverGages"

def _fetch_usace(
    station_ids: abc.Collection[str],
    start_dates: pd.DatetimeIndex,
    end_dates: pd.DatetimeIndex,
    *,
    rate_limit: multifutures.RateLimit | None,
    http_client: httpx.Client | None,
    multiprocessing_executor: multifutures.ExecutorProtocol | None,
    multithreading_executor: multifutures.ExecutorProtocol | None,
) -> dict[str, pd.DataFrame]:
    rate_limit = _resolve_rate_limit(rate_limit)
    http_client = _resolve_http_client(http_client)
    start_dates = _to_utc(start_dates)
    end_dates = _to_utc(end_dates)

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
    :param start_date: The starting date of the query. Defaults to 7 days ago.
    :param end_date: The finishing date of the query. Defaults to "now".
    :param variable: The variable to fetch. Defaults to "HG" (gauge height).
    :param rate_limit: The rate limit for making requests to the USACE servers.
    :param http_client: The ``httpx.Client``.
    :param multiprocessing_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
    :param multithreading_executor: An instance of a class implementing the ``concurrent.futures.Executor`` API.
    """
    logger.info("USACE-%s: Starting scraping: %s - %s", station_id, start_date, end_date)
    now = pd.Timestamp.now("utc")
    try:
        df = _fetch_usace(
            station_ids=[station_id],
            start_dates=_resolve_start_date(now, start_date),
            end_dates=_resolve_end_date(now, end_date),
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