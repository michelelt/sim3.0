import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")
from Simulator.Globals.SupportFunctions import *
from Simulator.Globals.GlobalVar import *


import pickle
import collections
from geopy.geocoders import Nominatim

import datetime
#import tzlocal  # $ pip install tzlocal

      
dataset_bookings=[]
dict_bookings={} #dictionary keys (timestamp), inside is a list of objects events. (events without timestamp)_
dict_bookings_short = {}
id_events = {}

def make_fusion_string(c1,c2):

  
    mys = "<Polygon><outerBoundaryIs><LinearRing><coordinates>"  \
             + "%.4f, %.4f, 17.0 "%(c1-ShiftLon/2,c2-ShiftLat/2) \
             + "%.4f, %.4f, 17.0 "%(c1+ShiftLon/2,c2-ShiftLat/2) \
             + "%.4f, %.4f, 17.0 "%(c1+ShiftLon/2,c2+ShiftLat/2) \
             + "%.4f, %.4f, 17.0 "%(c1-ShiftLon/2,c2+ShiftLat/2) \
          + "</coordinates></LinearRing></outerBoundaryIs></Polygon>"
    
    return mys


def main():
    


    collection="enjoy_PermanentBookings"
    if(provider == "car2go"):
        collection = "PermanentBookings"
    enjoy_bookings = setup_mongodb(collection)
    bookings = enjoy_bookings.find({"city": "Torino", 
                                    "init_time" :{"$gt" : 1504648800 , "$lt" : 1509577200}});
    geolocator = Nominatim()    
    location = geolocator.geocode("Torino")
    #baselon = location.longitude
    #baselat = location.latitude

    
    i=0 #id del booking, numero progressivo
    
    NumEvents=0
    NumEventsFiltered=0
    Discarted=0
    #local_timezone = tzlocal.get_localzone() # get pytz timezone
    
    day_stats = {}
    
    
    matrix = {}
    i=0
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

        d2 = haversine(lon1, lat1, lon2, lat2)

        
        if(duration > 120 and duration<3600 and d2>500):
            if( checkPerimeter(lat1, lon1) and checkPerimeter(lat2, lon2) or
               (provider == "car2go" and  ((checkPerimeter(lat1, lon1) and checkCasellePerimeter(lat2, lon2)) or  (checkCasellePerimeter(lat1, lon1) and checkPerimeter(lat2, lon2))))): 
                    ind = coordinates_to_index(coords[1])
                    matrix_coords = zoneIDtoMatrixCoordinates(ind)
                    if(matrix_coords not in matrix):
                        matrix[matrix_coords] = [0,0,0]
                    matrix[matrix_coords][0] += 1 
                    matrix[matrix_coords][1] += int(finalt) - int(initt)
                        
                        
                    d1 = datetime.datetime.fromtimestamp(initt)
                    d = datetime.date(d1.year,d1.month,d1.day)        
                    daystamp = d.strftime("%s")
                    
                    if(daystamp not in day_stats):
                        day_stats[daystamp] = [0,0]
                    day_stats[daystamp][0] += 1
                    day_stats[daystamp][1] += int(finalt) - int(initt)

            else:
                Discarted+=1   
                        
    foutdays = open("../output/output_bookings_stats.txt","w")    
    foutzones = open("../output/matrix.txt","w")
    
    fusionout = open("../output/fusion.txt","w")
    fusionout.write("ID ; Area; NParkings; SumTime; AvgTime\n")
    
    validzones = open("../input/"+provider+"_ValidZones.txt", "w")
    validzones.write("lat long ParkingsNo\n")

    for val in matrix:
        c0 = val[0]
        c1 = val[1]
        c2 = val[2]
        c3 = val[3]
        c4 = val[4]
        #print(val)
        coords = "%d %d %d %.4f %.4f"%(c0,c1,c2,c3,c4)

        coords2 = "%d %.4f %.4f"%(c0,c3,c4)
        
        validzones.write(coords2+ " %d\n"%matrix[val][0])
        
        avgpark = float(matrix[val][1])/float(matrix[val][0])
        foutzones.write(coords + " "+str(matrix[val][0]) + " "+ str(matrix[val][1])+ " %.2f"%avgpark +"\n")
        area_string = make_fusion_string(c3,c4)

        i+=1
        #if(i<10):
        fusionout.write(str(c0) + "; "+area_string + "; "+ " "+str(matrix[val][0]) +"; " + str(matrix[val][1])+ "; %.2f"%avgpark +"\n")
        
        

    for val in day_stats:
        d1 = datetime.datetime.fromtimestamp(int(val))
        strdate = str(d1.day)+"-"+str(d1.month)+"-"+str(d1.year)
        
        foutdays.write(str(val) + " "+strdate + " "+ str(day_stats[val][0])+ " "+ str(day_stats[val][1])+"\n")
        
        
main()

'''
if(d<30000 and d1<30000 and d2<30000):
    min30+=1
'''
