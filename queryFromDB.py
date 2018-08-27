#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pickle
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

import time
import datetime
import pandas as pd

import matplotlib.pyplot as plt
import pymongo
import Simulator
import Simulator.Globals.SupportFunctions as sf

def queryToCollection(typology):
    
    city="Torino"
    provider="car2go"
    initdate="2017-9-5T00:00:00"
    finaldate="2017-11-2T00:00:00"
    
    collections = {}    
    d1 = {}
    d1["enjoy"] = "enjoy_PermanentBookings"
    d1["car2go"] = "PermanentBookings"
    collections["bookings"] = d1
    
    d2 = {}
    d2["enjoy"] = "enjoy_PermanentParkings"
    d2["car2go"] = "PermanentParkings"
    collections["parkings"] = d2

    collection = collections[typology][provider]
    
    initdate = int(time.mktime(datetime.datetime.strptime(initdate, "%Y-%m-%dT%H:%M:%S").timetuple()))
    finaldate = int(time.mktime(datetime.datetime.strptime(finaldate, "%Y-%m-%dT%H:%M:%S").timetuple()))
    mongo_collection = sf.setup_mongodb(collection)
    data = mongo_collection.find({"city": city, "init_time": {"$gt":initdate, "$lt":finaldate}})
    return data

def queryParkings(typology):
    parks_cursor = queryToCollection(typology)
    if (parks_cursor == "err" or parks_cursor.count() == 0):
        return  parks_cursor.count()
    else :
        parkings_df = pd.DataFrame(list(parks_cursor))
    
        parkings_df['type'] = parkings_df['loc'].apply(lambda x : x['type'])
        parkings_df['coordinates'] = parkings_df['loc'].apply(lambda x : x['coordinates'])
        parkings_df = parkings_df.drop('loc',1)
        
        parkings_df['lon'] = parkings_df.coordinates.apply(lambda x : float(x[0]))
        parkings_df['lat'] = parkings_df.coordinates.apply(lambda x : float(x[1]))
        parkings_df = parkings_df.drop('coordinates',1)
        
        parkings_df['duration'] =parkings_df.final_date - parkings_df.init_date 
        parkings_df['duration'] = parkings_df['duration'].apply(lambda x: x.days*24*60 + x.seconds/60)
        parkings_df = parkings_df[parkings_df["lon"] <= 7.8] 
        return parkings_df
        
def queryBookings(typology):
    books_cursor = queryToCollection(typology)
    if (books_cursor == "err from cursor" or books_cursor.count() == 0):
        return "err"
    else :
#            print books_cursor.count()
#            bookings_df = pd.DataFrame(columns = pd.Series(books_cursor.next()).index)
        bookings_df = pd.DataFrame(list(books_cursor))
        
        bookings_df['duration_dr'] = bookings_df.driving.apply(lambda x: float(x['duration']/60))
        bookings_df['distance_dr'] = bookings_df.driving.apply(lambda x: x['distance'])
        bookings_df = bookings_df.drop('driving',1)
        
        bookings_df['type'] = bookings_df.origin_destination.apply(lambda x : x['type'])
        bookings_df['coordinates'] = bookings_df.origin_destination.apply(lambda x : x['coordinates'])
        bookings_df = bookings_df.drop('origin_destination',1)
        
        bookings_df['start'] = bookings_df.coordinates.apply(lambda x : x[0])
        bookings_df['end'] = bookings_df.coordinates.apply(lambda x : x[1])
        bookings_df = bookings_df.drop('coordinates',1)
        
        bookings_df['start_lon'] = bookings_df.start.apply(lambda x : float(x[0]) )
        bookings_df['start_lat'] = bookings_df.start.apply(lambda x : float(x[1]) )
        bookings_df = bookings_df.drop('start',1)
                  
        bookings_df['end_lon'] = bookings_df.end.apply(lambda x : float(x[0]) )
        bookings_df['end_lat'] = bookings_df.end.apply(lambda x : float(x[1]) )
        bookings_df = bookings_df.drop('end', 1)
        
        bookings_df['distance'] = bookings_df.apply(lambda x : sf.haversine(
                float(x['start_lon']),float(x['start_lat']),
                float(x['end_lon']), float(x['end_lat'])), axis=1
        )

        bookings_df['duration'] = bookings_df.final_date - bookings_df.init_date 
        bookings_df['duration'] = bookings_df['duration'].apply(lambda x: x.days*24*60 + x.seconds/60)
        
        bookings_df['duration_pt'] = bookings_df.public_transport.apply(lambda x : x['duration'] )
        bookings_df['distance_pt'] = bookings_df.public_transport.apply(lambda x : x['distance'] )
        bookings_df['arrival_date_pt'] = bookings_df.public_transport.apply(lambda x : x['arrival_date'] )
        bookings_df['arrival_time_pt'] = bookings_df.public_transport.apply(lambda x : x['arrival_time'] )
        bookings_df = bookings_df.drop('public_transport',1)
        
        bookings_df = bookings_df[ bookings_df["start_lon"] <= 7.8]  

        return bookings_df

def collection2df(typology):
    if typology == "bookings":
        df = queryBookings(typology)
        df = df[df["distance"] >= 500]
        df = df[df["duration"] >= 120 ]
        df = df[df["duration"] <= 3600]
        df.to_pickle(p+"/sim3.0/input/bookings_pickle")
    else :
        df = queryParkings(typology)
        df.to_pickle(p+"/sim3.0/input/parkings_pickle")
    return df


#collection2df("bookings")
#collection2df("parkings")
