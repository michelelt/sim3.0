# -*- coding: utf-8 -*-

import pandas as pd
import pickle
import ast
from math import radians, cos, sin, asin, sqrt

'''
flotta torino
booking milano 
flotta milano
mediane rentals (magari caricare tutta la cdf)
consumo smart
dati preliminar trips
'''

#TO = pd.read_pickle("./input/bookings_vancouver")
#print ("Not filtered TO:", len(TO))
#
def extractCoordinates(myDict, what):
    coord = myDict["coordinates"]
    start = coord[0]
    end = coord[1]
    if what == "longStart" :
        return  float(start[0])
    
    elif what == "latStart":
        return  float(start[1])
    
    elif what == "longEnd":
        return  float(end[0])
    
    elif what == "latEnd":
        return float(end[1])
    else:
        return "errore"
    
    
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return int(km*1000)
    
#df = TO
##
#df["longStart"] = df.apply(lambda x : extractCoordinates(x.origin_destination, "longStart"), axis = 1)
#df["latStart"]= df.apply(lambda x : extractCoordinates(x.origin_destination, "latStart"), axis = 1)
#df["longEnd"]= df.apply(lambda x : extractCoordinates(x.origin_destination, "longEnd"), axis = 1)
#df["latEnd"]= df.apply(lambda x : extractCoordinates(x.origin_destination, "latEnd"), axis = 1)  
#df["Duration"] = df["final_time"] - df["init_time"]
#df["Distance"] = df.apply(lambda x : haversine(x.longStart, x.latStart, x.longEnd, x.latEnd), axis=1)
#df.to_pickle("../bookings_torino_good.p")
    
df = pd.read_pickle("../bookings_torino_good.p")
df = df[(df.Duration >= 120)& (df.Duration <= 3600)]
df = df[df.Distance >= 500]
print("Before filtering caselle", df.Distance.median()*1.4)
df = df[(df.latStart < 45.17)]
df = df[(df.latEnd < 45.17)]
print("After filtering caselle", df.Distance.median()*1.4)



#caselleTirps = len(df[(df.latStart >= 45.18) | (df.latEnd >= 45.18)])

#
#MI = pd.read_pickle("../Milano_sim3.0/input/bookings_vancouver")
##print ("Not filtered MI:", len(MI))
#
#df = MI
##
#df["longStart"] = df.apply(lambda x : extractCoordinates(x.origin_destination, "longStart"), axis = 1)
#df["latStart"]= df.apply(lambda x : extractCoordinates(x.origin_destination, "latStart"), axis = 1)
#df["longEnd"]= df.apply(lambda x : extractCoordinates(x.origin_destination, "longEnd"), axis = 1)
#df["latEnd"]= df.apply(lambda x : extractCoordinates(x.origin_destination, "latEnd"), axis = 1)  
#df["Duration"] = df["final_time"] - df["init_time"]
#df["Distance"] = df.apply(lambda x : haversine(x.longStart, x.latStart, x.longEnd, x.latEnd), axis=1)
#df.to_pickle("../bookings_Milano_good.p")

df = pd.read_pickle("../bookings_Milano_good.p")
df = df[(df.Duration >= 120)& (df.Duration <= 3600)]
df = df[df.Distance >= 500]
print("Milano median", df.Distance.median()*1.4)
##
########################
##
#f = open( "./events/car2go_sorted_dict_events_obj.pkl", "rb" )
#tot = 0
#eventsTO = pickle.load(f)
#for ts in eventsTO.keys():
#    tot += len(eventsTO[ts])
#tot /= 2
#
#print("Torino bookings:", tot)
#
#f = open( "../Milano_sim3.0/events/car2go_sorted_dict_events_obj.pkl", "rb" )
#tot = 0
#eventsMI = pickle.load(f)
#for ts in eventsMI.keys():
#    tot += len(eventsMI[ts])
#tot /= 2
#print("Milano bookings:", tot)
#
#######################
#
ZoneCarsTO = pickle.load( open( "./input/car2go_ZoneCars.p", "rb" ) )
fleetTo = 0
for k in ZoneCarsTO.keys():
    fleetTo += len(ZoneCarsTO[k])
print ("Fllet Torino", fleetTo)

ZoneCarsMI = pickle.load( open( "../Milano_sim3.0/input/car2go_ZoneCars.p", "rb" ) )
fleetMi = 0
for k in ZoneCarsMI.keys():
    fleetMi += len(ZoneCarsMI[k])
print ("Fllet Milano", fleetMi)


######################
#TO_rental_cdf = pd.read_csv("../TOrentalCDF.csv")
#print ("median TO", TO_rental_cdf.describe())
#
#MI_rental_cdf = pd.read_csv("../MIrentalCDF.csv")
#print ("median MI", MI_rental_cdf.describe())




