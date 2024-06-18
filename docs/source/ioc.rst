IOC
===

The `Sea level station monitoring facility <http://www.ioc-sealevelmonitoring.org>`_
website is focused on operational monitoring of sea level measuring stations across the globe on behalf of the
`Intergovernmental Oceanographic Commission (IOC) <https://ioc.unesco.org>`_ aggregating data from more than 170 providers.


A DataFrame with the IOC station metadata can be retrieved with ``get_ioc_stations()``
while the station data can be fetched with ``fetch_ioc_station()``:

.. autofunction:: searvey.get_ioc_stations


.. autofunction:: searvey.fetch_ioc_station

Deprecated API
``````````````

.. autofunction:: searvey.get_ioc_data
