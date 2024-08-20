USACE RiverGages
==============
The U.S. Army Corps of Engineers RiverGages <https://rivergages.mvr.usace.army.mil/>_
system provides water level data for rivers and waterways across the United States.
searvey uses the RiverGages REST API to access this data. Currently, water level
data is exposed in searvey.

The data from an individual station can be retrieved with:
.. autofunction:: searvey.usace.get_usace_station

You can fetch data from multiple stations and multiple different dates with:
.. autofunction:: searvey.usace.fetch_usace

Note: The verify=False parameter in the httpx.Client() is used here to bypass
SSL verification, which is the only way to access the USACE RiverGages API.