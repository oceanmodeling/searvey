from __future__ import annotations

import datetime
from enum import Enum
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon

from . import coops
from . import ioc


STATIONS_COLUMNS: list[str] = [
    "provider",
    "provider_id",
    "country",
    "location",
    "lon",
    "lat",
    "is_active",
    "start_date",
    "last_observation",
    "geometry",
]


class Provider(str, Enum):
    COOPS: str = "COOPS"
    IOC: str = "IOC"


ALL: Literal["all"] = "all"


def get_stations(
    activity_threshold: datetime.timedelta = datetime.timedelta(days=3),
    providers: list[Provider] | str = ALL,
    region: Polygon | MultiPolygon | None = None,
) -> gpd.GeoDataFrame:

    # Retrieve the metadata
    ioc_gdf = ioc.get_ioc_stations(region=region)
    coops_gdf = coops.coops_stations_within_region(region=region)

    # Convert activity threshold to a Timezone Aware Datetime object
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    activity_threshold_ts = now_utc - activity_threshold

    # Normalize IOC
    # Convert delay to minutes
    ioc_gdf = ioc_gdf.assign(
        delay=pd.concat(
            (
                ioc_gdf.delay[ioc_gdf.delay.str.endswith("'")].str[:-1].astype(int),
                ioc_gdf.delay[ioc_gdf.delay.str.endswith("h")].str[:-1].astype(int) * 60,
                ioc_gdf.delay[ioc_gdf.delay.str.endswith("d")].str[:-1].astype(int) * 24 * 60,
            )
        )
    )

    # Some IOC stations appear to have negative delay due to server
    # [time drift](https://www.bluematador.com/docs/troubleshooting/time-drift-ntp)
    # or for other Provider-specific reasons. IOC suggests to ignore the negative
    # delay and consider the stations as active.
    # https://github.com/oceanmodeling/searvey/issues/40#issuecomment-1219509512
    ioc_gdf.loc[(ioc_gdf.delay < 0), "delay"] = 0

    # Calculate the timestamp of the last observation
    ioc_gdf = ioc_gdf.assign(
        last_observation=now_utc - ioc_gdf.delay.apply(lambda x: datetime.timedelta(minutes=x))
    )

    ioc_gdf = ioc_gdf.assign(
        provider=Provider.IOC.value,
        provider_id=ioc_gdf.ioc_code,
        start_date=ioc_gdf.added_to_system.apply(pd.to_datetime).dt.tz_localize("UTC"),
        is_active=ioc_gdf.last_observation > activity_threshold_ts,
    )

    ioc_gdf = ioc_gdf[STATIONS_COLUMNS]

    coops_gdf = coops_gdf.assign(
        provider=Provider.COOPS.value,
        provider_id=coops_gdf.index,
        country=np.where(coops_gdf.state.str.len() > 0, "USA", None),  # type: ignore[call-overload]
        location=coops_gdf[["name", "state"]].agg(", ".join, axis=1),
        lon=coops_gdf.geometry.x,
        lat=coops_gdf.geometry.y,
        is_active=coops_gdf.status == "active",
        start_date=pd.NaT,
        last_observation=coops_gdf[coops_gdf.status == "discontinued"]
        .removed.str[:18]
        .apply(pd.to_datetime)
        .dt.tz_localize("UTC"),
    )[STATIONS_COLUMNS]

    return pd.concat((ioc_gdf, coops_gdf)).reset_index(drop=True)
