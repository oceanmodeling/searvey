# Coastal Emergency Risks Assessment (CERA)
# =============================================================================
# Copyright(c) 2006-2022 Carola Kaiser (ckaiser <at> cct.lsu.edu)
# script to filter the NOAA Searvey data for CERA STOFS
import datetime
import os
import re
import sys

import pandas as pd

import CERA_get_active_stations

################################################################################
def get_csv():

    # read searvey data and return geopandas geodataframe object

    # COOPS stations, status = active
    #  stations_coops = CERA_get_active_stations.get_coops_stations_activity()
    # IOC stations, actve within the last 30 days
    stations_ioc = CERA_get_active_stations.get_ioc_stations_activity(
        activity_threshold=datetime.timedelta(days=30)
    )

    # convert to geojson
    #  coops = stations_coops.to_json()
    #  f = open("CERA_searvey_coops_all.json", "w")
    #  f.write(coops)
    #  f.close()

    ioc = stations_ioc.to_csv()
    f = open("CERA_searvey_ioc_all.csv", "w")
    f.write(ioc)
    f.close()

    # filter active stations
    #  coops_active = stations_coops[stations_coops['is_active']]   # comes back as True/False
    ioc_active = stations_ioc[stations_ioc["is_active"]]

    # convert to geojson
    #  coops_active = coops_active.to_json()
    #  f = open("CERA_searvey_coops_active.json", "w")
    #  f.write(coops_active)
    #  f.close()

    ################################################
    # write original geopandas into csv
    f = open("CERA_searvey_ioc_active_all.csv", "w", encoding="latin1")
    # original fieldnames = ['', 'provider','provider_id','country','location','lon','lat','is_active','start_date','last_observation','geometry']
    f.write(ioc_active.to_csv())
    f.close()

    ##################################################
    # remove US stations for IOC (assume they are duplicates from COOPS)
    df = ioc_active[ioc_active.country != "USA"]
    f1 = open("CERA_searvey_ioc_active_no_USA.csv", "w", encoding="latin1")
    f1.write(df.to_csv())
    f1.close()

    ##################################################
    # make pandas dataframe from csv
    df = pd.read_csv("CERA_searvey_ioc_active_no_USA.csv", encoding="latin1")

    # df["stationid"] = df.apply(lambda x:'STOFS_%s' % (x['provider_id']),axis=1)  # keep provider_id, STOFS_ will be added in CERA DB script
    # combine 'location' and 'country' to 'stationname'
    df["stationname"] = df.apply(lambda x: "%s (%s)" % (x["location"], x["country"]), axis=1)
    # keep only specific columns
    keep_col = ["provider_id", "stationname", "lon", "lat"]
    f2 = df[keep_col]

    f2.to_csv("CERA_searvey_ioc_active_no USA_for_CERA_DB.csv", encoding="latin1", index=False)

    # filter inactive stations


#  coops_inactive = stations_coops[~stations_coops['is_active']]   # comes back as True/False
#  ioc_inactive = stations_ioc[~stations_ioc['is_active']]

# convert to geojson
#  coops_inactive = coops_inactive.to_json()
#  f = open("CERA_searvey_coops_inactive.json", "w")
#  f.write(coops_inactive)
#  f.close()

#  ioc_inactive = ioc_inactive.to_json()
#  f = open("CERA_searvey_ioc_inactive.json", "w")
#  f.write(ioc_inactive)
#  f.close()

# do main work
if __name__ == "__main__":
    try:
        get_csv()

    except Exception as e:
        import traceback, sys

        tb = sys.exc_info()[2]
        tbstr = traceback.format_tb(tb)
        print("An error occurred: %s\n%s\n" % (str(e), tbstr))
