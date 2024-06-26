NDBC: National Data Buoy Center
===

The `National Data Buoy Center<https://www.ndbc.noaa.gov/>`_ (NDBC) is a part of the National Oceanic and Atmospheric Administration (NOAA)'s National Weather Service (NWS). They operate a network of data buoys and coastal stations that measure various oceanographic and atmospheric conditions. This data is essential for weather forecasting, marine operations, and scientific research.

NDBC's different modes:

• adcp: Acoustic Doppler Current Profiler Data contains depth, direction and speed
• cwind: Continuous Winds data
• ocean: oceanographic data
• spec: Spectral Wave data
• stdmet: Standard Meteorological data
• supl: Supplemental Measurements data
• swden: Spectral Wave Density data
• swdir: Spectral Wave data (alpha1)
• swdir2: Spectral Wave data (alpha2)
• swr1: Spectral Wave data(r1)
• swr2: Spectral Wave data(r2)

A list of NDBC stations is provided with the ``get_ndbc_stations`` functions
.. autofunction:: searvey._ndbc_api.get_ndbc_stations

The data from a specific station/stations can be retrieved with the ``fetch_ndbc_stations_data`` function
.. autofunction:: searvey._ndbc_api.fetch_ndbc_stations_data
