 #!/usr/bin/env python3

import numpy as np
import pickle
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

from Simulator.Globals.SupportFunctions import *
import datetime
import click
import random

import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv




def SearchAvailableCar(RechargingStation_Zones,ZoneI,Stamp):

    SelectedCar = "" 
    
    SelectedCar = ZoneI.getBestGlobalCars(Stamp)
    '''if(ZoneI.ID in RechargingStation_Zones):
        SelectedCar = ZoneI.getBestRechargedCars(Stamp)
    if(SelectedCar == ""):
        SelectedCar = ZoneI.getBestCars()'''
    
    
    return SelectedCar


def SearchNearestBestCar(RechargingStation_Zones,DistancesFrom_Zone_Ordered,ZoneID_Zone,BookingStarting_Position,Stamp):
       
    SelectedCar = ""
    Distance = -1
    Iter = 0
    for DistanceI in DistancesFrom_Zone_Ordered[BookingStarting_Position]:        
        Iter +=1
        RandomZones = DistanceI[1].getZones()
        for ZoneI_ID in RandomZones:
            ZoneI = ZoneID_Zone[ZoneI_ID]                    
            SelectedCar = SearchAvailableCar(RechargingStation_Zones,ZoneI,Stamp)
            if(SelectedCar != ""):
                Distance = DistanceI[1].getDistance()
                return SelectedCar, Distance, ZoneI.ID, Iter
    
    print("erroreeeee")
    return -1, -1

def ParkCar(RechargingStation_Zones, DistancesFrom_Zone_Ordered, ZoneID_Zone, BookingEndPosition,
            BookedCar, tankThreshold, walkingTreshold, BestEffort, upperTankThreshold, pThreshold):
    
    ToRecharge = False
    Recharged = False
    Distance =-1   
    Iter = 0
    Lvl =BookedCar.getBatteryLvl()

    p = random.SystemRandom().random()

    #Park in CS, and if policy is H, with the system P and UTT check
    #if the policy is FreeFloating, HybridParCondition is True -> parkings only in CS when hitted
    if(BestEffort
       and BookingEndPosition in RechargingStation_Zones
       and Lvl <= upperTankThreshold
       and p >= pThreshold):

        DistanceI = DistancesFrom_Zone_Ordered[BookingEndPosition][0]        
        Distance = DistanceI[1].getDistance()
        ZoneI_ID = DistanceI[1].getZones()[0]
        ZoneI = ZoneID_Zone[ZoneI_ID]        
        Found = ZoneI.getParkingAtRechargingStations(BookedCar)
        if(Found): 
            Recharged = True
            BookedCar.setInStation()
            return Lvl, ToRecharge, Recharged, Distance, ZoneI.ID, 1, p


    #Park only if N config and under TT in CS
    if(Lvl < tankThreshold):
        #print("PROBLEMA: %d"%BookedCar.getBatteryLvl())
        ToRecharge = True
        for DistanceI in DistancesFrom_Zone_Ordered[BookingEndPosition]:    
            Iter +=1    
            Distance = DistanceI[1].getDistance()
            if(Distance > walkingTreshold): break            
            RandomZones = DistanceI[1].getZones()
            for ZoneI_ID in RandomZones:     
                ZoneI = ZoneID_Zone[ZoneI_ID]
                if(ZoneI.ID in RechargingStation_Zones):    
                    Found = ZoneI.getParkingAtRechargingStations(BookedCar)
                    if(Found): 
                        Recharged = True
                        BookedCar.setInStation()
                        return Lvl, ToRecharge, Recharged, Distance, ZoneI.ID, Iter, p


    #lascia la macchina senza attaccarla
    for DistanceI in DistancesFrom_Zone_Ordered[BookingEndPosition]:        
        RandomZones = DistanceI[1].getZones()
        for ZoneI_ID in RandomZones:       
            ZoneI = ZoneID_Zone[ZoneI_ID]             
            ZoneI.getAnyParking(BookedCar)
            return Lvl, ToRecharge, Recharged, 0, ZoneI.ID, 1, p




def WriteOutHeader(file, parametersDict):
    
    HeaderOreder = ["Provider", "Policy", "Algorithm", "ChargingStations", 
     "AvaiableChargingStations", "TankThreshold", "WalkingTreshold", "upperTankThreshold",
     "pThreshold"]
    
    for key in HeaderOreder:
        file.write(key + ":" + str(parametersDict[key])+"\n")
    file.write("####"+"\n")

    return

def dict_to_string(myDict):
    
    mykeys = ["Type", "ToRecharge", "Recharged","ID","Lvl","Distance",
    "Iter","Recharge", "StartRecharge", "Stamp","EventCoords",
    "ZoneC", "Discharge", "TripDistance","FileID", "extractedP", "ZoneID", "OccupiedCS"]
    
    
    outputString =""
    for k in mykeys:
        if(type(myDict[k]) is int):
            outputString +="%d;"%myDict[k]
        elif(type(myDict[k]) is str):
            outputString +="%s;"%myDict[k]
        elif(type(myDict[k]) is float):
            outputString +="%.6f;"%myDict[k]
        elif(type(myDict[k]) is bool):
            outputString +=str(myDict[k])+";"
        else:
            outputString +="[%.6f,%.6f];"%(myDict[k][0],myDict[k][1])
            
        
    outputString = outputString[:-1]
    outputString+="\n"

    return outputString


def RunSim(BestEffort,
    algorithmName,
    algorithm,
    AvaiableChargingStations,
    tankThreshold,
    walkingTreshold,
    ZoneCars,
    RechargingStation_Zones,
    Stamps_Events,
    DistancesFrom_Zone_Ordered,
    lastS,
    upperTankThreshold,
    pThreshold,
    kwh,
    randomStrtingLevel,
    return_dict,
    processID,
    direction,
    city):

    gv.init()
    sf.assingVariables(city)

    numberOfStations = len(RechargingStation_Zones)

    policy, fileID, fname = foutname(BestEffort,algorithmName,AvaiableChargingStations,numberOfStations,tankThreshold,
                                     walkingTreshold, upperTankThreshold, pThreshold, kwh)



    

    NRecharge = 0
    NStart = 0
    NEnd = 0
    MeterRerouteStart = []
    MeterRerouteEnd = []
    NDeath = 0
    
    ActualBooking = 0

    BookingID_Car = {}

    ZoneID_Zone = {}
    
    ReloadZonesCars(ZoneCars, ZoneID_Zone, AvaiableChargingStations)


    if randomStrtingLevel == True :
        for zone in ZoneCars:
            if len(ZoneCars[zone]) > 0 :
                for car in ZoneCars[zone]:
                    car.BatteryCurrentCapacity = round(random.SystemRandom().random(),2) * car.BatteryMaxCapacity

    
    output_directory ="../output/Simulation_"+str(lastS)+"/"         
    
    fout = open(output_directory+fname,"w")
                
    fout2 = open(output_directory+"debugproblem.txt","w")
    a = datetime.datetime.now()
    WriteOutHeader(fout, {
    "Provider": gv.provider,
    "Policy": policy,                          
    "Algorithm": algorithm,
    "ChargingStations":numberOfStations,
    "AvaiableChargingStations":AvaiableChargingStations, 
    "TankThreshold":tankThreshold,
    "WalkingTreshold":  walkingTreshold,
    "upperTankThreshold": upperTankThreshold,
    "pThreshold": pThreshold,
    "kwh": kwh})
    
    
    fout.write("Type;ToRecharge;Recharged;ID;Lvl;Distance;Iter;Recharge;StartRecharge;Stamp;EventCoords;ZoneC;Discharge;TripDistance;FileID;extractedP;ZoneID;OccupiedCS\n")

    i=0
    occupiedCS = 0
    #with click.progressbar(Stamps_Events, length=len(Stamps_Events)) as bar:

    for inutile in range(0,1):
        for Stamp in Stamps_Events:
            for Event in Stamps_Events[Stamp]:
                i+=1
                if(Event.type == "s"):
                    fout2.write("%d %d \n"%(Stamp,ActualBooking))#,TotalCar1,TotalCar2))
                    ActualBooking +=1
                    BookingStarting_Position = sf.coordinates_to_index(Event.coordinates)
                    BookingID = Event.id_booking
                    NearestCar, Distance, ZoneID, Iter = SearchNearestBestCar(RechargingStation_Zones,DistancesFrom_Zone_Ordered,ZoneID_Zone,\
                                                                       BookingStarting_Position, Stamp)


                    if NearestCar.WasInRecharge == True :
                        occupiedCS -= 1

                    Recharge, StartRecharge = NearestCar.Recharge(Stamp)
                    NearestCar.setStartPosition(Event.coordinates)
                    BookingID_Car[BookingID] = NearestCar
                    Lvl = NearestCar.getBatteryLvl()
                    ID = NearestCar.getID()
                    ZoneC = zoneIDtoCoordinates(ZoneID)

                    d={"Type":"s",
                    "ToRecharge":np.NaN,
                    "Recharged":np.NaN,
                    "ID":ID,
                    "Lvl":Lvl,
                    "Distance":Distance,
                    "Iter":Iter,
                    "Recharge":Recharge,
                    "StartRecharge":StartRecharge,
                    "Stamp":Stamp,
                    "EventCoords":Event.coordinates,
                    "ZoneC":ZoneC,
                    "Discharge":np.NaN,
                    "TripDistance":np.NaN,
                    "FileID": fileID,
                    "extractedP" : np.NaN,
                    "ZoneID":ZoneID,
                    "OccupiedCS":occupiedCS}


                    fout.write(dict_to_string(d))

                    if(Distance> 0):
                        MeterRerouteStart.append(Distance)
                    NStart+=1
                else:
                    BookingEndPosition = sf.coordinates_to_index(Event.coordinates)
                    if(BookingEndPosition<0): print(Event.coordinates)
                    ActualBooking -=1
                    BookedCar = BookingID_Car[Event.id_booking]
                    Discarge, TripDistance = BookedCar.Discharge(Event.coordinates)
                    Lvl, ToRecharge, Recharged, Distance, ZoneID, Iter, extractedP = ParkCar(RechargingStation_Zones,DistancesFrom_Zone_Ordered,ZoneID_Zone,\
                                                                           BookingEndPosition, BookedCar, tankThreshold, walkingTreshold, BestEffort,\
                                                                           upperTankThreshold, pThreshold)
                    BookedCar.setStartRecharge(Stamp)
                    ID = BookedCar.getID()
                    del BookingID_Car[Event.id_booking]
                    ZoneC = zoneIDtoCoordinates(ZoneID)

                    if Recharged == True :
                        occupiedCS += 1

                    d={"Type":"e",
                    "ToRecharge":ToRecharge,
                    "Recharged":Recharged,
                    "ID":ID,
                    "Lvl":Lvl,
                    "Distance":Distance,
                    "Iter":Iter,
                    "Recharge":np.NaN,
                    "StartRecharge":np.NaN,
                    "Stamp":Stamp,
                    "EventCoords":Event.coordinates,
                    "ZoneC":ZoneC,
                    "Discharge":Discarge,
                    "TripDistance":TripDistance,
                    "FileID": fileID,
                    "extractedP":extractedP,
                    "ZoneID":ZoneID,
                    "OccupiedCS":occupiedCS}

                    fout.write(dict_to_string(d))

                    if(Distance > 0):
                        MeterRerouteEnd.append(Distance)

                    if(Recharged == True):
                        NRecharge +=1

                    if(BookedCar.getBatterCurrentCapacity()<0):
                        NDeath +=1

                    NEnd+=1


                # print (i, Event.type, occupiedCS)
                # if occupiedCS > AvaiableChargingStations * len(RechargingStation_Zones):
                #     print ("Noooooo")
                #     break

    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    #print("End Simulation: "+str(int(c)))


    fout.close()
    fout2.close()

    if return_dict == None :
        os.system('scp %s bigdatadb:/data/03/Carsharing_data/output/Simulation_%d/%s'%(output_directory+fname,lastS,fname))
        os.system('cat %s | ssh bigdatadb hdfs dfs -put -f - Simulator/output/Simulation_%s/%s' %(output_directory+fname,lastS,fname))
        os.system('rm %s'%(output_directory+fname))
        return

    if return_dict != None:

        PercRerouteEnd = len(MeterRerouteEnd)/NEnd*100
        PercRerouteStart = len(MeterRerouteStart)/NStart*100
        PercRecharge = NRecharge/NEnd*100
        PercDeath = NDeath/NEnd*100

        MedianMeterEnd = np.median(np.array(MeterRerouteEnd))
        MeanMeterEnd = np.mean(np.array(MeterRerouteEnd))

        MedianMeterStart = np.median(np.array(MeterRerouteStart))
        MeanMeterStart = np.mean(np.array(MeterRerouteStart))

        RetValues = {}
        RetValues["ProcessID"] = processID
        RetValues["Direction"] = direction
        RetValues["PercRerouteEnd"] = PercRerouteEnd
        RetValues["PercRerouteStart"] = PercRerouteStart
        RetValues["PercRecharge"] = PercRecharge
        RetValues["PercDeath"] = PercDeath
        RetValues["MedianMeterEnd"] = MedianMeterEnd
        RetValues["MeanMeterEnd"] = MeanMeterEnd
        RetValues["MedianMeterStart"] = MedianMeterStart
        RetValues["MeanMeterStart"] = MeanMeterStart
        RetValues["NEnd"] = NEnd
        RetValues["NStart"] = NStart
        return_dict[processID] = RetValues

    current_folder = os.getcwd().split("/")
    output_folder = ""
    for i in range(0,len(current_folder)-1):
        output_folder += current_folder[i]+"/"
    output_folder+="output/"


    #do not use
	#os.system('ssh bigdatadb hdfs dfs -put /data/03/Carsharing_data/output/Simulation_%d/%s Simulator/output/Simulation_%d/%s &' %(lastS,fname,lastS,fname))
    #os.system('ssh bigdatadb cat /data/03/Carsharing_data/output/Simulation_%d/%s | hdfs dfs -put -f - Simulator/output/Simulation_%s/%s &' %(lastS,fname,lastS,fname))
    
    return
