import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

import pandas as pd
import pickle
import collections
from geopy.geocoders import Nominatim
import datetime
#import tzlocal  # $ pip install tzlocal
import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv
import pprint
import datetime

city = sys.argv[1]
gv.init()
sf.assingVariables(city)
pp = pprint.PrettyPrinter(indent=4)

dataset_bookings=[]
dict_bookings={} #dictionary keys (timestamp), inside is a list of objects events. (events without timestamp)_
dict_bookings_short = {}
id_events = {}

'''
def main():

    collection="enjoy_PermanentBookings"
    if gv.provider == "car2go":
        collection = "PermanentBookings"
    enjoy_bookings = sf.setup_mongodb(collection)
    bookings = enjoy_bookings.find({"city": gv.city,
                                    "init_time": {"$gt": gv.initDate , "$lt": gv.finalDate}})


    matrix = {}
    Discarded=0
    for booking in bookings:

        #if(i>1000): break
        initt =  booking['init_time']
        finalt= booking['final_time']
        duration = finalt - initt
        coords = booking['origin_destination']['coordinates']
        lon1 = coords[0][0]
        lat1 = coords[0][1]
        lon2 = coords[1][0]
        lat2 = coords[1][1]

        d2 = sf.haversine(lon1, lat1, lon2, lat2)

        if duration > 120 and duration < 3600 and d2 > 500 :
            if sf.checkPerimeter(lat1, lon1) and sf.checkPerimeter(lat2, lon2):
                ind = sf.coordinates_to_index(coords[1])
                matrix_coords = sf.zoneIDtoMatrixCoordinates(ind)
                if matrix_coords not in matrix:
                    matrix[matrix_coords] = [0, 0, 0]
                matrix[matrix_coords][0] += 1
                matrix[matrix_coords][1] += int(finalt) - int(initt)
            else:
                Discarded += 1

    validzones = open("../input/" + gv.provider + "_ValidZones.csv", "w")
    validzones.write("id,Lon,Lat,NParkings,SumTime,AvgTime\n")

    Zone_TotalParkingTime = {}
    Zone_NParkings = {}
    Zone_AvgTime = {}
    for val in matrix:
        c0 = val[0]
        c3 = val[3]
        c4 = val[4]

        coords2 = "%d,%.4f,%.4f,"%(c0,c3,c4)
        avgpark = float(matrix[val][1])/float(matrix[val][0])
        validzones.write(coords2 + "%d,%d,%d\n" %(matrix[val][0], matrix[val][1], avgpark))

        Zone_NParkings[(c0,c3,c4)] = matrix[val][0]
        Zone_TotalParkingTime[(c0,c3,c4)] = matrix[val][1]
        Zone_AvgTime[(c0,c3,c4)] = avgpark


    sorted_Zone_NParkings = sorted(Zone_NParkings.items(), key=lambda x:x[1], reverse=True)
    fout = open("../input/"+gv.provider+"_max-parking500.csv", "w")
    fout.write("id,lat,lon,n_parkings\n")
    for val in sorted_Zone_NParkings:
        strout = "%d,%.6f,%.6f,%d\n"%(val[0][0],val[0][1],val[0][2],val[1])
        fout.write(strout)

    sorted_Zone_TotalParkingTime = sorted(Zone_TotalParkingTime.items(), key=lambda x:x[1], reverse=True)
    fout = open("../input/"+gv.provider+"_max-time500.csv", "w")
    fout.write("id,lat,lon,totParkingTime\n")
    for val in sorted_Zone_TotalParkingTime:
        strout = "%d,%.6f,%.6f,%d\n"%(val[0][0],val[0][1],val[0][2],val[1])
        fout.write(strout)

    sorted_Zone_AvgTime = sorted(Zone_AvgTime.items(), key=lambda x:x[1], reverse=True)
    fout = open("../input/"+gv.provider+"_avg-time500.csv", "w")
    fout.write("id,lat,lon,avgTime\n")
    for val in sorted_Zone_AvgTime:
        strout = "%d,%.6f,%.6f,%d\n"%(val[0][0],val[0][1],val[0][2],val[1])
        fout.write(strout)


    print ("CVZ, discarded:", Discarded)
    print("CVZ, End")
    
'''

def formatBookings():
    collection="enjoy_PermanentBookings"
    if gv.provider == "car2go":
        collection = "PermanentBookings"
    enjoy_bookings = sf.setup_mongodb(collection)

    print("***********************")
    print("city", gv.city)
    print("initDate ",datetime.datetime.fromtimestamp(
        int(gv.initDate)
    ).strftime('%Y-%m-%d %H:%M:%S'))
    print("fianlDate",datetime.datetime.fromtimestamp(
        int(gv.finalDate)
    ).strftime('%Y-%m-%d %H:%M:%S'))
    print("***********************")

    bookings = enjoy_bookings.find({"city": gv.city,
                                    "init_time": {"$gt": gv.initDate ,
                                                  "$lt": gv.finalDate}
                                    })

    bookings_df = pd.DataFrame(list(bookings))


    if gv.city == "Vancouver":
        bookings_df["init_time"] = bookings_df["init_time"].sub(25200)
        bookings_df["final_time"] = bookings_df["final_time"].sub(25200)

    bookings_df.to_pickle('../input/bookings_' + gv.city)

    bookings_df["duration"] = bookings_df["final_time"] - bookings_df["init_time"]
    bookings_df["duration"] = bookings_df["duration"].astype(int)
    bookings_df = bookings_df.drop('driving',1)

    bookings_df['type'] = bookings_df.origin_destination.apply(lambda x : x['type'])
    bookings_df['coordinates'] = bookings_df.origin_destination.apply(lambda x : x['coordinates'])
    bookings_df = bookings_df.drop('origin_destination',1)

    bookings_df['end'] = bookings_df.coordinates.apply(lambda x : x[0])
    bookings_df['start'] = bookings_df.coordinates.apply(lambda x : x[1])
    bookings_df = bookings_df.drop('coordinates',1)

    bookings_df['start_lon'] = bookings_df.start.apply(lambda x : float(x[0]) )
    bookings_df['start_lat'] = bookings_df.start.apply(lambda x : float(x[1]) )
    bookings_df = bookings_df.drop('start',1)

    bookings_df['end_lon'] = bookings_df.end.apply(lambda x : float(x[0]) )
    bookings_df['end_lat'] = bookings_df.end.apply(lambda x : float(x[1]) )
    bookings_df = bookings_df.drop('end', 1)

    bookings_df['distance'] = bookings_df.apply(lambda x : sf.haversine(
            float(x['start_lon']),float(x['start_lat']),
            float(x['end_lon']), float(x['end_lat'])), axis=1)

    bookings_df = bookings_df[bookings_df["distance"] >= 700]
    bookings_df = bookings_df[bookings_df["duration"] >= 120]
    bookings_df = bookings_df[bookings_df["duration"] <= 3600]

    if gv.city == "Torino":
        bookings_df = bookings_df[ bookings_df["start_lon"] <= 7.8]



    return bookings_df

def createParkingsFromBookings(df):
#    parkings = pd.DataFrame(columns=["duration", "init_time", "final_time", "plate", "lat", "lon"])
    parkings = pd.DataFrame()

    i = 0
    s = 0

    def coord2id (lon, lat):

        ind = int((lat - gv.minLat) / gv.ShiftLat) * \
              gv.NColumns + \
              int((lon - gv.minLon) / gv.ShiftLon)
#        print (int(ind))
#        print()
        if(ind<=gv.MaxIndex):

            return int(ind)

        # if(checkCasellePerimeter(lat,lon)):
        #     print("Caselle!!!")
        #     return GlobalVar.MaxIndex+1

        return -1


    for plate in df.plate.unique():
        res = pd.DataFrame()
        tmp = df[df["plate"] == plate].reset_index()
        tmp["zone"] = tmp.apply(lambda x: coord2id(x["end_lon"], x["end_lat"]), axis=1)
        tmp.init_time = tmp.init_time.shift(-1)
        tmp = tmp.dropna()
        if len(tmp) == 1 : continue
#        print (i, plate, int(len(tmp)))
        i=i+1

        res["duration"] = tmp["final_time"].astype(int) - tmp["init_time"].astype(int)
        res["init_time"] = tmp["init_time"].astype(int)
        res["final_time"] = tmp["final_time"].astype(int)
        res["plate"] = tmp["plate"]
        res["lat"] = tmp["end_lat"]
        res["lon"] = tmp["end_lon"]
        res["zone"] = tmp["zone"]

        if res.isnull().any().any() : print (i, plate)

        parkings = parkings.append(res, ignore_index=True)


    return parkings

def printCSV(parkings):
    parkings["plate"] = parkings["plate"].astype(str)
    parkings["duration"] = parkings["duration"].astype(int)
    parkings["duration"] = parkings["duration"].div(60)

    def wrapperIDtoCoords(zoneID, coord):
        out = sf.zoneIDtoCoordinates(zoneID)

        if coord == "lon":
            return out[0]
        else :
            return out[1]
    q = parkings.groupby("zone").count()["duration"]
    parkings_stats =  pd.DataFrame(q)
    parkings_stats = parkings_stats.rename(columns={'duration':"NParkings"})
    parkings_stats["SumTime"] = parkings.groupby("zone").sum()["duration"]
    parkings_stats["AvgTime"] = parkings_stats["SumTime"]/ parkings_stats["NParkings"]

    parkings_stats = parkings_stats.reset_index()
    parkings_stats["Lon"] = parkings_stats.apply(lambda row : wrapperIDtoCoords(row.zone, "lon"), axis=1)
    parkings_stats["Lat"] = parkings_stats.apply(lambda row : wrapperIDtoCoords(row.zone, "lat"), axis=1)

    parkings_stats = parkings_stats.set_index("zone")
    parkings_stats.to_csv("../input/"+ gv.city + "_" + gv.provider + "_ValidZones.csv", index_label="id")

    a = parkings_stats[["Lon", "Lat", "NParkings"]].sort_values(by="NParkings", ascending=False)\
    .to_csv("../input/"+ gv.city + "_" + gv.provider + "_max-parking500.csv", index_label="id")

    parkings_stats[["Lon", "Lat", "SumTime"]].sort_values("SumTime", ascending=False)\
    .to_csv("../input/"+ gv.city + "_" + gv.provider + "_max-time500.csv", index_label="id")

    parkings_stats[["Lon", "Lat", "AvgTime"]].sort_values("AvgTime", ascending=False)\
    .to_csv("../input/"+ gv.city + "_" + gv.provider + "_avg-time500.csv", index_label="id")




bookings_df = formatBookings()
print("CVZ, all data queried")
parkings_df = createParkingsFromBookings(bookings_df)
print("CVZ, all parkings are created")
parkings_stats = printCSV(parkings_df)
print("CVZ, input files generated")





