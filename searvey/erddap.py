import datetime
import io
import logging
from typing import cast
from typing import Optional

import erddapy
import pandas as pd
import requests

from searvey.custom_types import StrDict
from searvey.models import Constraints
from searvey.models import ERDDAPDataset


logger = logging.getLogger(__name__)

DEFAULT_SEARVEY_SESSION = requests.Session()


def ts_to_erddap(ts: datetime.datetime) -> str:
    return ts.strftime("%Y/%m/%dT%H:%M:%SZ")


# Adapted from:
# https://github.com/ioos/erddapy/blob/524783242c8b973ffd9bf5ab9f20f70516752f07/erddapy/url_handling.py#L14-L33
# @functools.lru_cache(maxsize=256)
def urlopen(
    url: str,
    session: requests.Session,
    requests_kwargs: StrDict,
    timeout: float = 10,
) -> io.BytesIO:
    # logger.debug("Making a GET request to: %s", url)
    response = session.get(url, allow_redirects=True, timeout=timeout, **requests_kwargs)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise requests.exceptions.HTTPError(f"{response.content.decode()}") from err
    data = io.BytesIO(response.content)
    data.seek(0)
    return data


def get_erddap_url(
    dataset: ERDDAPDataset,
    constraints: Constraints,
    response: str = "csvp",
) -> str:
    erddap = erddapy.ERDDAP(
        server=dataset.server_url,
        protocol=dataset.protocol.value,
        response=response,
    )
    erddap.dataset_id = dataset.dataset_id
    erddap.constraints = {
        "time>=": ts_to_erddap(constraints.start_date),
        "time<=": ts_to_erddap(constraints.end_date),
        "latitude>=": constraints.bbox.lat_min,
        "latitude<=": constraints.bbox.lat_max,
        "longitude>=": constraints.bbox.lon_min,
        "longitude<=": constraints.bbox.lon_max,
    }
    url = erddap.get_download_url()
    return cast(str, url)


def query_erddap(
    dataset: ERDDAPDataset,
    constraints: Constraints,
    session: requests.Session = DEFAULT_SEARVEY_SESSION,
    timeout: int = 10,
    requests_kwargs: Optional[StrDict] = None,
    read_csv_kwargs: Optional[StrDict] = None,
) -> pd.DataFrame:
    if requests_kwargs is None:
        requests_kwargs = {}
    if read_csv_kwargs is None:
        read_csv_kwargs = {}
    # Make the query and parse the result
    url = get_erddap_url(dataset=dataset, constraints=constraints)
    logger.debug(url)
    data = urlopen(url, session=session, requests_kwargs=requests_kwargs, timeout=timeout)
    df = pd.read_csv(data, **read_csv_kwargs)
    return df
