# -*- coding: utf-8 -*-

import pandas as pd
import datetime
import sys
from math import radians, cos, sin, asin, sqrt
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


city = sys.argv[1]
df = pd.read_pickle("bookings_vancouver")

df["duration"] = df["final_time"] - df["init_time"]
df = df[(df["duration"] >= 120) & (df["duration"] <= 3600)]

df['coordinates'] = df.origin_destination.apply(lambda x : x['coordinates'])
df = df.drop('origin_destination',1)

df['start'] = df.coordinates.apply(lambda x : x[0])
df['end'] = df.coordinates.apply(lambda x : x[1])
df = df.drop('coordinates',1)

df['start_lon'] = df.start.apply(lambda x : float(x[0]) )
df['start_lat'] = df.start.apply(lambda x : float(x[1]) )
df = df.drop('start',1)

df['end_lon'] = df.end.apply(lambda x : float(x[0]) )
df['end_lat'] = df.end.apply(lambda x : float(x[1]) )
df = df.drop('end', 1)

df['distance'] = df.apply(lambda x : haversine(
        float(x['start_lon']),float(x['start_lat']),
        float(x['end_lon']), float(x['end_lat'])), axis=1
)
    
df = df[df["distance"] >= 700]
if city == 'Vancouver':
    df["final_time"] = df["final_time"].sub(25200)
    df["init_time"]  = df["init_time"].sub(25200)


zzz = df
zzz["dayHour"] = zzz.apply(lambda x :datetime.datetime.fromtimestamp(x.init_time).strftime('%H'), axis=1 )
zzz["dayNumb"] = zzz.apply(lambda x :datetime.datetime.fromtimestamp(x.init_time).weekday(), axis=1 )
zzz.to_csv("./%s_completeDataset.csv"% city)


zzz = pd.read_csv("./%s_completeDataset.csv"% city)
wd_df = zzz[zzz['dayNumb'] <  5]
we_df = zzz[zzz['dayNumb'] >= 5]

wd_df = wd_df.groupby('dayHour').count()['_id'].rename(columns={"_id":"BPD"})
we_df = we_df.groupby('dayHour').count()['_id'].rename(columns={"_id":"BPD"})

final_df =pd.DataFrame()
final_df["WD_BPD"] = wd_df
final_df["WE_BPD"] = we_df
final_df["city"] = city

final_df.to_csv("bookings_per_hour_"+city+".csv")






