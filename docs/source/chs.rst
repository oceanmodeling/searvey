CHS
===

The `Canadian Hydrographic Service (CHS) <https://www.charts.gc.ca/index-eng.html>`_
provides water level and forecast data for Canadian waters. This module offers functions
to interact with the CHS API and retrieve station information and water level data.

A DataFrame with the CHS station metadata can be retrieved with ``get_chs_stations()``,
while station data can be fetched with ``fetch_chs_station()`` and ``_fetch_chs()``.

.. autofunction:: searvey.get_chs_stations

.. autofunction:: searvey.fetch_chs_station