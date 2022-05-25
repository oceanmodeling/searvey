# searvey

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/oceanmodeling/searvey/master.svg)](https://results.pre-commit.ci/latest/github/oceanmodeling/searvey/master)
[![tests](https://github.com/oceanmodeling/searvey/actions/workflows/run_tests.yml/badge.svg)](https://github.com/oceanmodeling/searvey/actions/workflows/run_tests.yml)

Searvey aims to provide the following functionality:

- Unified catalogue of observational data including near real time.

- Real time data analysis/clean up to facilitate comparison with numerical
  models.

- On demand data retrieval from multiple sources.

## Usage

#### retrieve data products from the U.S. Center for Operational Oceanographic Products and Services (CO-OPS)

The [Center for Operational Oceanographic Products and Services (CO-OPS)](https://tidesandcurrents.noaa.gov)
maintains and operates a large array of tidal buoys and oceanic weather stations
that measure water and atmospheric variables
across the coastal United States. CO-OPS provides
several [data products](https://tidesandcurrents.noaa.gov/products.html)
including hourly water levels, tidal datums and predictions, and trends in sea
level over time.

A list of CO-OPS stations can be retrieved with the `coops_stations()` function.

```python
from searvey.coops import coops_stations

coops_stations()
```

```
        nws_id                         name state        status                                            removed                      geometry
nos_id
1600012  46125                    QREB buoy              active                                               <NA>    POINT (122.62500 37.75000)
1619910  SNDP5  Sand Island, Midway Islands              active                                               <NA>   POINT (-177.37500 28.21875)
1630000  APRP7            Apra Harbor, Guam              active                                               <NA>    POINT (144.62500 13.44531)
1631428  PGBP7               Pago Bay, Guam              active                                               <NA>    POINT (144.75000 13.42969)
1770000  NSTP6    Pago Pago, American Samoa              active                                               <NA>  POINT (-170.75000 -14.27344)
...        ...                          ...   ...           ...                                                ...                           ...
8423898  FTPN3                   Fort Point    NH  discontinued  2020-04-13 00:00:00,2014-08-05 00:00:00,2012-0...    POINT (-70.68750 43.06250)
8726667  MCYF1           Mckay Bay Entrance    FL  discontinued  2020-05-20 00:00:00,2019-03-08 00:00:00,2017-0...    POINT (-82.43750 27.90625)
8772447  FCGT2                     Freeport    TX  discontinued  2020-05-24 18:45:00,2018-10-10 21:50:00,2018-1...    POINT (-95.31250 28.93750)
9087079  GBWW3                    Green Bay    WI  discontinued  2020-10-28 13:00:00,2007-08-06 23:59:00,2007-0...    POINT (-88.00000 44.53125)
8770570  SBPT2            Sabine Pass North    TX  discontinued  2021-01-18 00:00:00,2020-09-30 15:45:00,2020-0...    POINT (-93.87500 29.73438)

[435 rows x 6 columns]
```

Additionally, you can use a Shapely `Polygon` or `MultiPolygon` to constrain the
stations query to a specific region:

```python
from shapely.geometry import Polygon
from searvey.coops import coops_stations_within_region

region = Polygon(...)

coops_stations_within_region(region=region)
```

```
        nws_id                               name state removed                    geometry
nos_id                                                                                     
8651370  DUKN7                               Duck    NC     NaT  POINT (-75.75000 36.18750)
8652587  ORIN7                Oregon Inlet Marina    NC     NaT  POINT (-75.56250 35.78125)
8654467  HCGN7              USCG Station Hatteras    NC     NaT  POINT (-75.68750 35.21875)
8656483  BFTN7          Beaufort, Duke Marine Lab    NC     NaT  POINT (-76.68750 34.71875)
8658120  WLON7                         Wilmington    NC     NaT  POINT (-77.93750 34.21875)
8658163  JMPN7                 Wrightsville Beach    NC     NaT  POINT (-77.81250 34.21875)
8661070  MROS1                    Springmaid Pier    SC     NaT  POINT (-78.93750 33.65625)
8662245  NITS1   Oyster Landing (N Inlet Estuary)    SC     NaT  POINT (-79.18750 33.34375)
8665530  CHTS1  Charleston, Cooper River Entrance    SC     NaT  POINT (-79.93750 32.78125)
8670870  FPKG1                       Fort Pulaski    GA     NaT  POINT (-80.87500 32.03125)
```

##### retrieve CO-OPS data product for a specific station

```python
from datetime import datetime
from searvey.coops import COOPS_Station

station = COOPS_Station(8632200)
station.product('water_level', start_date=datetime(2018, 9, 13),
                end_date=datetime(2018, 9, 16, 12))
```

```
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
```

##### retrieve CO-OPS data product from within a region and time interval

To retrieve data, you must provide three things:

1. the **data product** of interest; one of
    - `water_level` - Preliminary or verified water levels, depending on
      availability.
    - `air_temperature` - Air temperature as measured at the station.
    - `water_temperature` - Water temperature as measured at the station.
    - `wind` - Wind speed, direction, and gusts as measured at the station.
    - `air_pressure` - Barometric pressure as measured at the station.
    - `air_gap` - Air Gap (distance between a bridge and the water's surface) at
      the station.
    - `conductivity` - The water's conductivity as measured at the station.
    - `visibility` - Visibility from the station's visibility sensor. A measure
      of atmospheric clarity.
    - `humidity` - Relative humidity as measured at the station.
    - `salinity` - Salinity and specific gravity data for the station.
    - `hourly_height` - Verified hourly height water level data for the station.
    - `high_low` - Verified high/low water level data for the station.
    - `daily_mean` - Verified daily mean water level data for the station.
    - `monthly_mean` - Verified monthly mean water level data for the station.
    - `one_minute_water_level`  One minute water level data for the station.
    - `predictions` - 6 minute predictions water level data for the station.*
    - `datums` - datums data for the stations.
    - `currents` - Currents data for currents stations.
    - `currents_predictions` - Currents predictions data for currents
      predictions stations.
2. a **region** within which to retrieve the data product
3. a **time interval** within which to retrieve the data product

```python
from datetime import datetime, timedelta

from shapely.geometry import Polygon
from searvey.coops import coops_product_within_region

polygon = Polygon(...)

coops_product_within_region(
    'water_level',
    region=polygon,
    start_date=datetime.now() - timedelta(hours=1),
)
```

```
<xarray.Dataset>
Dimensions:  (nos_id: 10, t: 11)
Coordinates:
  * nos_id   (nos_id) int64 8651370 8652587 8654467 ... 8662245 8665530 8670870
  * t        (t) datetime64[ns] 2022-03-08T14:48:00 ... 2022-03-08T15:48:00
    nws_id   (nos_id) <U5 'DUKN7' 'ORIN7' 'HCGN7' ... 'NITS1' 'CHTS1' 'FPKG1'
    x        (nos_id) float64 -75.75 -75.56 -75.69 ... -79.19 -79.94 -80.88
    y        (nos_id) float64 36.19 35.78 35.22 34.72 ... 33.34 32.78 32.03
Data variables:
    v        (nos_id, t) float32 6.256 6.304 6.361 6.375 ... 2.633 2.659 2.686
    s        (nos_id, t) float32 0.107 0.097 0.127 0.122 ... 0.005 0.004 0.004
    f        (nos_id, t) object '1,0,0,0' '1,0,0,0' ... '1,0,0,0' '1,0,0,0'
    q        (nos_id, t) object 'p' 'p' 'p' 'p' 'p' 'p' ... 'p' 'p' 'p' 'p' 'p'
```

## Development

```
python3 -mvenv .venv
source .venv/bin/activate
poetry install
pre-commit install
```

If you wish to use jupyterlab to test searvey, then, assuming you have an
existing jupyterlab
installation, you should be able to add a kernel to it with:

```bash
python -mipykernel install --user --name searvey
```
