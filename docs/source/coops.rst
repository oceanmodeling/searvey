CO-OPS tidal station data
=========================

The `Center for Operational Oceanographic Products and Services (CO-OPS) <https://tidesandcurrents.noaa.gov>`_
maintains and operates a large array of tidal buoys and oceanic weather stations that measure water and atmospheric variables
across the coastal United States. CO-OPS provides several `data products <https://tidesandcurrents.noaa.gov/products.html>`_
including hourly water levels, tidal datums and predictions, and trends in sea level over time.

A list of CO-OPS stations can be retrieved with the ``coops_stations()`` function.

.. autofunction:: searvey.coops.coops_stations

Additionally, you can use a Shapely ``Polygon`` or ``MultiPolygon`` to constrain the stations query to a specific region:

.. autofunction:: searvey.coops.coops_stations_within_region

CO-OPS station class
--------------------

.. autoclass:: searvey.coops.COOPS_Station

retrieve CO-OPS data product from within a region and time interval
-------------------------------------------------------------------

To retrieve data, you must provide three things:

1. the data product of interest; one of
    - ``water_level`` - Preliminary or verified water levels, depending on availability.
    - ``air_temperature`` - Air temperature as measured at the station.
    - ``water_temperature`` - Water temperature as measured at the station.
    - ``wind`` - Wind speed, direction, and gusts as measured at the station.
    - ``air_pressure`` - Barometric pressure as measured at the station.
    - ``air_gap`` - Air Gap (distance between a bridge and the water's surface) at the station.
    - ``conductivity`` - The water's conductivity as measured at the station.
    - ``visibility`` - Visibility from the station's visibility sensor. A measure of atmospheric clarity.
    - ``humidity`` - Relative humidity as measured at the station.
    - ``salinity`` - Salinity and specific gravity data for the station.
    - ``hourly_height`` - Verified hourly height water level data for the station.
    - ``high_low`` - Verified high/low water level data for the station.
    - ``daily_mean`` - Verified daily mean water level data for the station.
    - ``monthly_mean`` - Verified monthly mean water level data for the station.
    - ``one_minute_water_level``  One minute water level data for the station.
    - ``predictions`` - 6 minute predictions water level data for the station.*
    - ``datums`` - datums data for the stations.
    - ``currents`` - Currents data for currents stations.
    - ``currents_predictions`` - Currents predictions data for currents predictions stations.
2. a region within which to retrieve the data product
3. a time interval within which to retrieve the data product

.. autofunction:: searvey.coops.coops_product_within_region

CO-OPS query class
""""""""""""""""""

The ``COOPS_Query`` class lets you send an individual query to the CO-OPS API by specifying a station, data product, and time interval.

.. autoclass:: searvey.coops.COOPS_Query

New API
-------

.. autofunction:: searvey.get_coops_stations
.. autofunction:: searvey.fetch_coops_station
