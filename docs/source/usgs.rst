USGS
====

The United States Geological Survey's (USGS) `National Water Information
System (NWIS) <https://waterdata.usgs.gov/nwis>`_ provides different
categories of water data for sites all across the US. This includes information
about both surface and ground water, and for physical, chemical, and pollution
variables. `searvey` uses NWIS REST API through `dataretrieval`
package to access this data. Currently only data about elevation and
flow rate are exposed in `searvey`.

A list of USGS stations is provided with the ``get_usgs_stations()`` function with various subsetting options.

.. autofunction:: searvey.usgs.get_usgs_stations

The station data can be retrieved with

.. autofunction:: searvey.usgs.get_usgs_data
