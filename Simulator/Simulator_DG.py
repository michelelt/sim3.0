 #!/usr/bin/env python3


import Simulator.Classes.Distance 

from random import shuffle
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


DistancesFrom_Recharging_Zone_Ordered = {}


def SearchAvailableCar(ZoneI,Stamp):

    if(ZoneI.getNumCars()>0):
        return ZoneI.getBestGlobalCars(Stamp)        
    return ""


def SearchNearestBestCar(DistancesFrom_Zone_Ordered, ZoneID_Zone, BookingStarting_Position,Stamp):
       
    SelectedCar = ""
    DistanceV = -1
    Iter = 0
    for DistanceI in DistancesFrom_Zone_Ordered[BookingStarting_Position]:        
        Iter +=1
        RandomZones = DistanceI[1].getRandomZones()
        for ZoneI_ID in RandomZones:        
            ZoneI = ZoneID_Zone[ZoneI_ID]                    
            SelectedCar = SearchAvailableCar(ZoneI,Stamp)
            if(SelectedCar != ""):
                DistanceV = DistanceI[1].getDistance()
                return SelectedCar, DistanceV, ZoneI.ID, Iter
    
    print("erroreeeee")
    return -1, -1



def ParkCar(RechargingStation_Zones, DistancesFrom_Zone_Ordered, ZoneID_Zone, BookingEndPosition,
            BookedCar, tankThreshold, walkingTreshold, BestEffort, pThreshold):
    
    ToRecharge = False
    Recharged = False
    DistanceV =-1   
    Iter = 0
    Lvl =BookedCar.getBatteryLvl()

    p = random.SystemRandom().random()

    #Park in CS, and if policy is H, with the system P and UTT check
    #if the policy is FreeFloating, HybridParCondition is True -> parkings only in CS when hitted
    #pThreshold == 0 -> park only Needed
    #pThreshold == 100 -> hybrid
    if(BestEffort
       and BookingEndPosition in RechargingStation_Zones
       and pThreshold >= p):

        DistanceI = DistancesFrom_Zone_Ordered[BookingEndPosition][0]        
        DistanceV = DistanceI[1].getDistance()
        ZoneI_ID = DistanceI[1].getZones()[0]
        ZoneI = ZoneID_Zone[ZoneI_ID]        
        Found = ZoneI.getParkingAtRechargingStations(BookedCar)
        if(Found): 
            Recharged = True
            BookedCar.setInStation()
            return Lvl, ToRecharge, Recharged, DistanceV, ZoneI.ID, 1, p
            
    #Park only if N config and under TT in CS
    if(Lvl < tankThreshold):
        #print("PROBLEMA: %d"%BookedCar.getBatteryLvl())
        Iter=0
        ToRecharge = True
        for DistanceI in  DistancesFrom_Recharging_Zone_Ordered[BookingEndPosition]:
            Iter +=1    
            Distance = DistanceI[1].getDistance()
            if(Distance > walkingTreshold): break            
            RandomZones = DistanceI[1].getRandomZones()
        

            for ZoneI_ID in RandomZones:     
                ZoneI = ZoneID_Zone[ZoneI_ID]
                if(ZoneI.ID in RechargingStation_Zones):    
                    Found = ZoneI.getParkingAtRechargingStations(BookedCar)
                    if(Found): 
                        Recharged = True
                        BookedCar.setInStation()
                        return Lvl, ToRecharge, Recharged, Distance, ZoneI.ID, Iter, p         



    #Park the car withouht plug it
    for DistanceI in DistancesFrom_Zone_Ordered[BookingEndPosition]:        
        RandomZones = DistanceI[1].getZones()
        for ZoneI_ID in RandomZones:       
            ZoneI = ZoneID_Zone[ZoneI_ID]             
            ZoneI.getAnyParking(BookedCar)
            return Lvl, ToRecharge, Recharged, 0, ZoneI.ID, 1, p




def WriteOutHeader(file, parametersDict):
    
    HeaderOreder = ["Provider", "Policy", "Algorithm", "ChargingStations", 
     "AvaiableChargingStations", "TankThreshold", "WalkingTreshold",
     "pThreshold", "kwh", "gamma"]
    
    for key in HeaderOreder:
        file.write(key + ":" + str(parametersDict[key])+"\n")
    file.write("####"+"\n")

    return


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
    pThreshold,
    kwh,
    gamma,
    randomStrtingLevel,
    return_dict,
    processID,
    direction,
    city):


    gv.init()
    sf.assingVariables(city)

    time_init = time.time()

    numberOfStations = len(RechargingStation_Zones)

    policy, fileID, fname = foutname(BestEffort,algorithmName,AvaiableChargingStations,numberOfStations,tankThreshold,
                                     walkingTreshold, pThreshold, kwh, gamma)



    
    NRecharge = 0
    NStart = 0
    NEnd = 0
    MeterRerouteStart = []
    MeterRerouteEnd = []
    NDeath = 0
    
    ActualBooking = 0

    BookingID_Car = {}

    ZoneID_Zone = {}
    
    sf.ReloadZonesCars(ZoneCars, ZoneID_Zone, AvaiableChargingStations)
    sf.FillDistancesFrom_Recharging_Zone_Ordered(DistancesFrom_Zone_Ordered,\
                                              DistancesFrom_Recharging_Zone_Ordered,\
                                              RechargingStation_Zones)
    cars = 0
    for z in ZoneCars.keys():
        if len(ZoneCars[z]) > 0:
            for i in range (len(ZoneCars[z])):
                ZoneCars[z][i].setRechKwh(kwh)
                ZoneCars[z][i].setGamma(gamma)
                cars+=1
    print ('cars', cars)

    # cars = 0
    # for z in ZoneCars.keys():
    #     if len(ZoneCars[z]) > 0:
    #         for i in range (len(ZoneCars[z])):
    #             print(ZoneCars[z][i].getGamma())
    #             cars +=1

    # print('cars', cars)

            
    if randomStrtingLevel == True :
        for zone in ZoneCars:
            if len(ZoneCars[zone]) > 0 :
                for car in ZoneCars[zone]:
                    car.BatteryCurrentCapacity = round(random.SystemRandom().random(),2) * car.BatteryMaxCapacity


    # for k in DistancesFrom_Recharging_Zone_Ordered.keys():
    #     print(k)
    #
    # for el in DistancesFrom_Recharging_Zone_Ordered[1]:
    #     print('eucle', el[0])
    #     print('zones', el[1].getZones())
    #     print(el[1].getDistance())
    #     print()

    

    output_directory ="../output/Simulation_"+str(lastS)+"/"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
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
    "pThreshold": pThreshold,
    "kwh": kwh,
    "gamma": gamma})
    
    
    fout.write("Type;ToRecharge;Recharged;ID;Lvl;Distance;Iter;Recharge;StartRecharge;Stamp;EventCoords;ZoneC;Discharge;TripDistance;FileID;extractedP;ZoneID;OccupiedCS\n")

    i=0
    occupiedCS = 0
    #with click.progressbar(Stamps_Events, length=len(Stamps_Events)) as bar:
    for Stamp in Stamps_Events:
        for Event in Stamps_Events[Stamp]:
            i+=1
            if(Event.type == "s"):
                fout2.write("%d %d \n"%(Stamp,ActualBooking))#,TotalCar1,TotalCar2))
                ActualBooking +=1
                BookingStarting_Position = sf.coordinates_to_index(Event.coordinates)
                BookingID = Event.id_booking
                NearestCar, DistanceV, ZoneID, Iter = SearchNearestBestCar(DistancesFrom_Zone_Ordered,ZoneID_Zone,\
                                                                   BookingStarting_Position, Stamp)



                if NearestCar.WasInRecharge == True :
                    occupiedCS -= 1

                Recharge, StartRecharge = NearestCar.Recharge(Stamp)
                NearestCar.setStartPosition(Event.coordinates)
                NearestCar.setGamma(gamma)
                NearestCar.setRechKwh(kwh)
                BookingID_Car[BookingID] = NearestCar
                Lvl = NearestCar.getBatteryLvl()
                ID = NearestCar.getID()
                ZoneC = zoneIDtoCoordinates(ZoneID)


                EventCoords = Event.coordinates
                #Loop Unrooling
                outputString  = "s;"
                outputString += "nan;"
                outputString += "nan;"
                outputString += "%d;"% ID
                outputString += "%.2f;"% Lvl
                outputString += "%d;"% DistanceV
                outputString += "%d;"% Iter
                outputString += "%.2f;"%Recharge
                outputString += "%d;"%StartRecharge
                outputString += "%d;"% Stamp
                outputString += "[%.6f,%.6f];"%(EventCoords[0],EventCoords[1])
                outputString += "[%.6f,%.6f];"%(ZoneC[0],ZoneC[1])
                outputString += "nan;" 
                outputString += "nan;"
                outputString += "%s;"%fileID
                outputString += "%nan;"
                outputString += "%d;"% ZoneID
                outputString += "%d\n"% occupiedCS

                fout.write(outputString)

                if(DistanceV> 0):
                    MeterRerouteStart.append(DistanceV)
                NStart+=1
            else:
                BookingEndPosition = sf.coordinates_to_index(Event.coordinates)
                if(BookingEndPosition<0): print(Event.coordinates)
                ActualBooking -=1
                BookedCar = BookingID_Car[Event.id_booking]
                Discarge, TripDistance = BookedCar.Discharge(Event.coordinates)

                Lvl, ToRecharge, Recharged, DistanceV, ZoneID, Iter, extractedP = ParkCar(RechargingStation_Zones,DistancesFrom_Zone_Ordered,ZoneID_Zone,\
                                                                       BookingEndPosition, BookedCar, tankThreshold, walkingTreshold, BestEffort,\
                                                                       pThreshold)



                #extra consuption if there is rerouting
                # if DistanceV > 0:
                #     BookedCar.setStartPosition(Event.coordinates)
                #     DiscargeR, TripDistanceR = BookedCar.Discharge(sf.zoneIDtoCoordinates(ZoneID))
                #     Discarge += DiscargeR
                #     TripDistance += TripDistanceR
                    
                    #Please notice that TripDistanceR > Distance because TripDistanceR keeps in account the corr. fact
                    #Distance is dist(centre_arrival_Zone, centre_leaving_zone), so in this distance is biased by an error of 150m
                    # print("Distnace", Distance)
                    # print("Discharge", Discarge)
                    # print("DischargeR", DiscargeR)
                    # print("TripDistance", TripDistance)
                    # print("TripDistanceR", TripDistanceR)
                    # print("check", sf.haversine(Event.coordinates[0],
                    #                             Event.coordinates[1],
                    #                             sf.zoneIDtoCoordinates(ZoneID)[0],
                    #                             sf.zoneIDtoCoordinates(ZoneID)[1]
                    #                             )*gv.CorrectiveFactor
                    # )
                    # print("-------------------------")





                BookedCar.setStartRecharge(Stamp)
                ID = BookedCar.getID()
                del BookingID_Car[Event.id_booking]
                ZoneC = zoneIDtoCoordinates(ZoneID)

                if Recharged == True :
                    occupiedCS += 1

                EventCoords = Event.coordinates
                #Loop Unrooling
                outputString  = "e;"
                outputString += "%s;"%ToRecharge
                outputString += "%s;"%Recharged
                outputString += "%d;"% ID
                outputString += "%.2f;"% Lvl
                outputString += "%d;"% DistanceV
                outputString += "%d;"% Iter
                outputString += "nan;"
                outputString += "nan;"
                outputString += "%d;"% Stamp
                outputString += "[%.6f,%.6f];"%(EventCoords[0],EventCoords[1])
                outputString += "[%.6f,%.6f];"%(ZoneC[0],ZoneC[1])
                outputString += "%.2f;"%Discarge 
                outputString += "%.2f;"%TripDistance
                outputString += "%s;"%fileID
                outputString += "%.2f;"%extractedP
                outputString += "%d;"% ZoneID
                outputString += "%d\n"% occupiedCS
                
                fout.write(outputString)


                #fout.write(dict_to_string(d))

                if(DistanceV > 0):
                    MeterRerouteEnd.append(DistanceV)

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
        RetValues['WeightedWalkedDistance'] = (MeanMeterEnd * PercRerouteEnd + ((PercRecharge - PercRerouteEnd) * 150))/100
        return_dict[processID] = RetValues

    current_folder = os.getcwd().split("/")
    output_folder = ""
    for i in range(0,len(current_folder)-1):
        output_folder += current_folder[i]+"/"
    output_folder+="output/"

    print('PID %d, time: %.3f'%(processID, time.time()-time_init))
    #do not use
    #os.system('ssh bigdatadb hdfs dfs -put /data/03/Carsharing_data/output/Simulation_%d/%s Simulator/output/Simulation_%d/%s &' %(lastS,fname,lastS,fname))
    #os.system('ssh bigdatadb cat /data/03/Carsharing_data/output/Simulation_%d/%s | hdfs dfs -put -f - Simulator/output/Simulation_%s/%s &' %(lastS,fname,lastS,fname))
    
    return RetValues
