'''
Created on 13/nov/2017

@author: dgiordan
'''

import pymongo
import ssl
from math import *
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

import pandas as pd
import numpy as np
import random
import csv
import time
import datetime
import subprocess

from pathlib import Path

import Simulator.Globals.GlobalVar as GlobalVar
import Simulator.Classes.Distance


def readConfigFile(city):

    proc = subprocess.Popen(["ls -t ../input/"],  stdout=subprocess.PIPE, shell=True)

    (out, err) = proc.communicate()
    q = out.decode("utf-8").split("\n")

    there_are_config_files = False
    for el in q:
        if city+"_config" in el:
            there_are_config_files = True


    if not there_are_config_files:
        city = str(input('Insert city: '))
        if city == 'default':
            city = str(input('DEFAULT MODE, Insert city: '))
            config = open(p+"/input/" + city + "_config.txt", "w")

            config.write("city="+city.lower().title()+"\n")

            provider = 'car2go'
            config.write("provider=" + provider + "\n")

            initdate = "2017-09-05T00:00:00"
            config.write("initdate=" + initdate + "\n")

            finaldate = "2017-09-13T00:00:00"
            config.write("finaldate=" + finaldate + "\n")

            fleetSize = 'mean'
            config.write("fleetSize=" + fleetSize + "\n")

            car2goKey = 'polito'
            config.write("car2goKey=" + car2goKey + "\n")

        else:
            config = open(p+"/input/" + city + "_config.txt", "w")
            config.write("city="+city.lower().title()+"\n")

            provider = str(input('Insert Provider: '))
            config.write("provider=" + provider + "\n")

            initdate = str(input('Insert initial date in this format \"%Y-%m-%dT%H:%M:%S\":'))
            config.write("initdate=" + initdate + "\n")

            finaldate = str(input('Insert final date in this format \"%Y-%m-%dT%H:%M:%S\":'))
            config.write("finaldate=" + finaldate + "\n")

            fleetSize = str(input('Insert fleetSize:'))
            config.write("fleetSize=" + fleetSize + "\n")

            car2goKey = str(input('Insert Car2go API key:'))
            config.write("car2goKey=" + car2goKey + "\n")

        config.close()

    # else:
    #
    #     proc = subprocess.Popen(["ls -t ../input/"],  stdout=subprocess.PIPE, shell=True)
    #
    #     (out, err) = proc.communicate()
    #     q = out.decode("utf-8").split("\n")
    #     city = q[0].split("_")
    #     city = city[0]


    with open(p+"/input/" + city + "_config.txt", "r") as f:
        content = f.readlines()

    d={}
    for x in content:
        if len(x) > 0:
            x = x.rstrip()
            line = x.split("=")
            if len(line[1]) > 0:
                d[line[0]] = line[1]
            else :
                print (line[0], "not present")
                exit(666)


    d["city"] = d["city"].lower().title()
    d["initdate"] = int(time.mktime(datetime.datetime.strptime(d["initdate"], "%Y-%m-%dT%H:%M:%S").timetuple()))
    d["finaldate"] = int(time.mktime(datetime.datetime.strptime(d["finaldate"], "%Y-%m-%dT%H:%M:%S").timetuple()))

    try:
        pathCityAreas = Path(p+"/input/car2go_oper_areas_limits.csv")
        if pathCityAreas.is_file():
            cityAreas = pd.read_csv(pathCityAreas, header=0)
            cityAreas = cityAreas.set_index("city")
            d["limits"] = cityAreas.loc[d["city"]]
    except:
        print ("Missing ", d["city"])


    return d



def assingVariables(city):

    d = readConfigFile(city)

    # GlobalVar.MaxLat = d["limits"]["maxLat"]
    # GlobalVar.MaxLon = d["limits"]["maxLon"]
    # GlobalVar.minLat = d["limits"]["minLat"]
    # GlobalVar.minLon = d["limits"]["minLon"]
    GlobalVar.city = d["city"]
    GlobalVar.provider = d["provider"]
    GlobalVar.initDate = int(d["initdate"])
    GlobalVar.finalDate = int(d["finaldate"])
    GlobalVar.fleetSize = d["fleetSize"]
    GlobalVar.car2goKey = d["car2goKey"]

    GlobalVar.CaselleCentralLat = 45.18843
    GlobalVar.CaselleCentralLon = 7.6435

    GlobalVar.CorrectiveFactor = 1.4  # .88

    # GlobalVar.shiftLat500m = 0.0045
    # GlobalVar.shiftLon500m = 0.00637

    '''
    add /2 in order to have a zonization 250x250
    '''

    if "limits" in d.keys():
        GlobalVar.MaxLat = d["limits"]["maxLat"]
        GlobalVar.MaxLon = d["limits"]["maxLon"]
        GlobalVar.minLat = d["limits"]["minLat"]
        GlobalVar.minLon = d["limits"]["minLon"]
        GlobalVar.NColumns = int((GlobalVar.MaxLon - GlobalVar.minLon) / GlobalVar.shiftLon500m)
        GlobalVar.NRows = int((GlobalVar.MaxLat - GlobalVar.minLat) / GlobalVar.shiftLat500m)
        GlobalVar.MaxIndex = GlobalVar.NRows * GlobalVar.NColumns - 1

        GlobalVar.ShiftLon = (GlobalVar.MaxLon - GlobalVar.minLon) / GlobalVar.NColumns
        GlobalVar.ShiftLat = (GlobalVar.MaxLat - GlobalVar.minLat) / GlobalVar.NRows


    return

###############################################################################

def setup_mongodb(CollectionName):
    """"Setup mongodb session """
    try:
        client = pymongo.MongoClient('bigdatadb.polito.it',
                                     27017,
                                     ssl=True,
                                     ssl_cert_reqs=ssl.CERT_NONE) # server.local_bind_port is assigned local port                #client = pymongo.MongoClient()
        client.server_info()
        db = client['carsharing'] #Choose the DB to use
        db.authenticate('ictts', 'Ictts16!')#, mechanism='MONGODB-CR') #authentication         #car2go_debug_info = db['DebugInfo'] #Collection for Car2Go watch
        Collection = db[CollectionName] #Collection for Enjoy watch
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
    return Collection

###############################################################################




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

###############################################################################

def numberOfZones(city):
    c2id={"Vancouver":510, "Torino":251, "Berlino":833, "Milano":549}
    if city in c2id.keys():
        return c2id[city]


    command = 'ssh -t d046373@polito.it@tlcdocker1.polito.it wc -l %s_sim3.0/input/%s_car2go_ValidZones.csv' % (city,city)
    zones = int(str(subprocess.check_output(command, shell=True)).split(" ")[0][2:5]) - 1

    return zones

###############################################################################

def validSimulation(BestEffort, tankThreshold_valid, pThresholdCheck) :

   '''

   :param BestEffort: True -> car goes to park if ends trip in a CS
   :param tankThreshold_valid: percentage of battery below with a car can recharge
   :param pThresholdCheck: 0-> people charge only needed, 1 -> charge every time
   :return:
   '''

  #Station Based and IMP2
   if tankThreshold_valid == 100:
       return False

   #IMP1
   if BestEffort==False and tankThreshold_valid==-1 :
       return False

   #Needed only p=0
   if BestEffort == False \
       and tankThreshold_valid >= 0 \
       and tankThreshold_valid < 100 \
       and pThresholdCheck != 0 :
       #print(BestEffort, tankThreshold, p)
       return False

   ##free Floating only and p=100
   if BestEffort == True \
       and tankThreshold_valid == -1 \
       and pThresholdCheck != 100 :
       #print(BestEffort, tankThreshold, p)
       return False

   return True


###############################################################################


def coordinates_to_index(coords):

    lon = coords[0]
    lat = coords[1]

    ind = int((lat - GlobalVar.minLat) / GlobalVar.ShiftLat) * \
          GlobalVar.NColumns + \
          int((lon - GlobalVar.minLon) / GlobalVar.ShiftLon)
    if(ind<=GlobalVar.MaxIndex): return int(ind)

    # if(checkCasellePerimeter(lat,lon)):
    #     print("Caselle!!!")
    #     return GlobalVar.MaxIndex+1

    return -1

###############################################################################




def checkPerimeter(lat,lon):

    if(lon > GlobalVar.minLon  and  lon < GlobalVar.MaxLon and lat > GlobalVar.minLat  and  lat< GlobalVar.MaxLat): return True

    return False

def checkCasellePerimeter(lon,lat):

    '''
    print("Var",lat,lon)

    print("Lon",GlobalVar.CaselleminLon,GlobalVar.CaselleMaxLon)

    print("Lat",GlobalVar.CaselleminLat,GlobalVar.CaselleMaxLat)
    print("\n")

    CaselleCentralLat = 45.18843

    CaselleCentralLon = 7.6435

                        7,64987
                        7.645130
                        7,63713

                        45,19293
                        45.067790
                        45,18393


                    7.643500,45.188430
    shiftLat500m = 0.0045
    shiftLon500m = 0.00637


    CaselleMaxLat = CaselleCentralLat + shiftLat500m
    CaselleMaxLon = CaselleCentralLon + shiftLon500m
    CaselleminLat = CaselleCentralLat - shiftLat500m
    CaselleminLon = CaselleCentralLon + shiftLon500m
    print("caselle "+str(lat)+" "+str(lon))
    '''

    if(lon > GlobalVar.CaselleminLon
       and lon < GlobalVar.CaselleMaxLon
       and lat > GlobalVar.CaselleminLat
       and lat < GlobalVar.CaselleMaxLat):

        return True
    return False

def checkBerlinZone(lon,lat):
    zone_id = coordinates_to_index([lon, lat])
    if(zone_id in GlobalVar.BerlinCriticalZone):
        return True
    return False


def zoneIDtoCoordinates(ID):

    Xi = ID % GlobalVar.NColumns
    Yi = int(ID / GlobalVar.NColumns)


    CentalLoni = (Xi + 0.5) * GlobalVar.ShiftLon + GlobalVar.minLon
    CentalLati = (Yi + 0.5) * GlobalVar.ShiftLat + GlobalVar.minLat

    return [CentalLoni, CentalLati]

def MatrixCoordinatesToID(Xi,Yi):

    ID = Yi * GlobalVar.NColumns + Xi

    return ID

def zoneIDtoMatrixCoordinates(ID):

    Xi = ID % GlobalVar.NColumns
    Yi = int(ID / GlobalVar.NColumns)

    CentalLoni = (Xi + 0.5) * GlobalVar.ShiftLon + GlobalVar.minLon
    CentalLati = (Yi + 0.5) * GlobalVar.ShiftLat + GlobalVar.minLat

    return (ID, Xi, Yi, CentalLoni, CentalLati)


def ReloadZonesCars(ZoneCars, ZoneID_Zone, AvaiableChargingStations):

    for ZoneI_ID in ZoneCars:
        ZoneI = Zone(ZoneI_ID,AvaiableChargingStations)
        ZoneID_Zone[ZoneI_ID] = ZoneI
        ZoneI.setCars(ZoneCars[ZoneI.ID])

    return


def loadRecharing(method, numberOfStations, city):
    Stations = []
    csvfilePath = p+"/input/"+ city + "_" + GlobalVar.provider + "_" + method + "500.csv"
    if (method == "rnd"):

        zones = pd.read_csv(p+"/input/"+ city + "_"  + GlobalVar.provider + "_ValidZones.csv", sep=" ", header=0)
        zones_list = list(zones.index)
        # while len(Stations)<=numberOfStations:
            # rn = np.random.randint(NColumns*Nrows, size = 1)
            # if(rn not in Stations): Stations.append(rn)
        Stations2 = random.sample(zones_list, numberOfStations)
        for i in range(0, len(Stations2)):
            Stations.append(Stations2[i])


    else :
        #print ("Not in random")
        coords = []
        with open(csvfilePath, 'rt') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',')
                next(csvreader) # jump header
                for row in csvreader:
                    coords.insert(0, float(row[2])) #lon
                    coords.insert(1, float(row[1])) #lat
#                    index = np.array(coordinates_to_index(coords))
                    index = int(row[0])
                    Stations.append(index)
                    if len(Stations) == numberOfStations:
                    #     Stations.pop(0)
                        break

    return Stations

def foutname(BestEffort,algorithm,AvaiableChargingStations,numberOfStations,
             tankThreshold,walkingTreshold, pThreshold, kwh):

    foutname = ""

    policy = ""

    if(tankThreshold==100):
        policy = "StationBased"
    if(tankThreshold>-1 and tankThreshold<100):
        if(BestEffort == True):
            policy="Hybrid"
        else:
            policy="Needed"

    if(tankThreshold<1):
        policy="FreeFloating"

    fileid = GlobalVar.provider+"_"+\
             policy +"_"+\
             algorithm+"_"+\
             str(numberOfStations)+"_"+\
             str(AvaiableChargingStations)+"_"+\
             str(tankThreshold) +"_"+\
             str(walkingTreshold) + "_" +\
             str(int(pThreshold*100)) + "_" +\
             str(int(kwh))

    return policy, fileid,fileid+".txt"


def FillDistancesFrom_Recharging_Zone_Ordered(DistancesFrom_Zone_Ordered,\
                                              DistancesFrom_Recharging_Zone_Ordered,\
                                              RechargingStation_Zones):
    
    for zoneI in DistancesFrom_Zone_Ordered:
        DistancesFrom_Recharging_Zone_Ordered[zoneI] = []
        for DistanceI in DistancesFrom_Zone_Ordered[zoneI]:
            RandomZones = DistanceI[1].getZones()
            DistanceValid=""
            for RandomZonesI in RandomZones:
                if(RandomZonesI in RechargingStation_Zones):
                    if(DistanceValid==""):
                        DistanceValid = (DistanceI[0],Simulator.Classes.Distance.Distance(DistanceI[0]))
                    DistanceValid[1].appendZone(RandomZonesI)
            if(DistanceValid!=""):
                DistancesFrom_Recharging_Zone_Ordered[zoneI].append(DistanceValid)
    
    return




from Simulator.Classes.Zone import *
