High Level API
~~~~~~~~~~~~~~

The high level API provides station metadata from all the available providers.
Since each provider is returning different metadata, only the (small) subset of common metadata is provided.

.. autoclass:: searvey.stations.Provider
  :members:
  :undoc-members:
  :no-private-members:
  :member-order: bysource

.. autofunction:: searvey.stations.get_stations
