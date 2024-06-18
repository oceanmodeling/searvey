from __future__ import annotations

import logging
import typing as T
import warnings
from datetime import timedelta

import httpx
import limits
import multifutures
import pandas as pd
import tenacity

from .custom_types import DatetimeLike

logger = logging.getLogger(__name__)


def _to_utc(
    index: pd.DatetimeIndex | pd.Timestamp,
    *,
    warn: bool = False,
) -> pd.DatetimeIndex:
    if index.tz:
        ref = index
        if isinstance(ref, pd.Timestamp):
            ref = pd.DatetimeIndex([ref])
        if warn and index.tz.utcoffset(ref[0]) != timedelta():
            warnings.warn("Converting to UTC!\nData is retrieved and stored in UTC time")
        index = index.tz_convert("utc")
    else:
        if warn:
            warnings.warn("Assuming UTC!\nData is retrieved and stored in UTC time")
        index = index.tz_localize("utc")
    return index


def _to_datetime_index(ts: pd.Timestamp) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex([ts])
    return index


def _resolve_start_date(now: pd.Timestamp, start_date: DatetimeLike | None) -> pd.DatetimeIndex:
    if start_date is None:
        resolved_start_date = T.cast(pd.Timestamp, now - pd.Timedelta(days=7))
    else:
        resolved_start_date = pd.to_datetime(start_date)
    index = _to_datetime_index(resolved_start_date)
    return index


def _resolve_end_date(now: pd.Timestamp, end_date: DatetimeLike | None) -> pd.DatetimeIndex:
    if end_date is None:
        resolved_end_date = now
    else:
        resolved_end_date = pd.to_datetime(end_date)
    index = _to_datetime_index(resolved_end_date)
    return index


def _resolve_rate_limit(rate_limit: multifutures.RateLimit | None) -> multifutures.RateLimit:
    if rate_limit is None:
        rate_limit = multifutures.RateLimit(rate_limit=limits.parse("5/second"))
    return rate_limit


def _resolve_http_client(http_client: httpx.Client | None) -> httpx.Client:
    if http_client is None:
        timeout = httpx.Timeout(timeout=10, read=30)
        http_client = httpx.Client(timeout=timeout)
    return http_client


def _before_sleep(retry_state: T.Any) -> None:  # pragma: no cover
    logger.warning(
        "Retrying %s: attempt %s ended with: %s",
        retry_state.fn,
        retry_state.attempt_number,
        retry_state.outcome,
    )


RETRY: T.Callable[..., T.Any] = tenacity.retry(
    stop=(tenacity.stop_after_delay(90) | tenacity.stop_after_attempt(10)),
    wait=tenacity.wait_random(min=2, max=10),
    retry=tenacity.retry_if_exception_type(httpx.TransportError),
    before_sleep=_before_sleep,
)


def _fetch_url_main(url: str, client: httpx.Client, redirect: bool = False) -> str:
    try:
        response = client.get(url, follow_redirects=redirect)
    except Exception:
        logger.warning("Failed to retrieve: %s", url)
        raise
    data = response.text
    return data


@RETRY
def _fetch_url(
    url: str,
    client: httpx.Client,
    rate_limit: multifutures.RateLimit | None = None,
    redirect: bool = False,
    **kwargs: T.Any,
) -> str:
    if rate_limit is not None:  # pragma: no cover
        while rate_limit.reached():
            multifutures.wait()  # pragma: no cover
    return _fetch_url_main(
        url=url,
        client=client,
        redirect=redirect,
    )
