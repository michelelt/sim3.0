import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")


import pandas as pd
import time
import datetime

def readConfigFile():
    cityAreas = pd.read_csv(p+"/../input/car2go_oper_areas_limits.csv", header=0)
    availableCities = cityAreas["city"]
    with open(p+"/../input/config.txt", "r") as f:
        content = f.readlines()

    d={}
    for x in content:
        if len(x) == 0 :
            pass
        else:
            x = x.rstrip()
            line = x.split("=")
            d[line[0]] = line[1]

    # d["city"] = d["city"].
    # for city in d["city"]:
    #     if city not in availableCities:
    #         print ("No entries for", "city")
    #
    # d["provider"] = d["provider"].split(",")

    d["initdate"] = int(time.mktime(datetime.datetime.strptime(d["initdate"], "%Y-%m-%dT%H:%M:%S").timetuple()))
    d["finaldate"] = int(time.mktime(datetime.datetime.strptime(d["finaldate"], "%Y-%m-%dT%H:%M:%S").timetuple()))
    cityAreas = cityAreas.set_index("city")
    d["limits"] = cityAreas.loc[d["city"]]


    return d

