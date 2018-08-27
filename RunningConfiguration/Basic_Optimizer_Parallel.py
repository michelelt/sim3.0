import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

from Simulator.Simulator import *
from Simulator.Globals.GlobalVar import * 
import datetime as datetime
import pickle
import numpy as np
import pandas as pd

from multiprocessing import Process
import multiprocessing
import subprocess

import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv
gv.init()
sf.assingVariables()
import pprint
pp = pprint.PrettyPrinter(indent=4)

c2id = {"Vancouver":6, "Torino":7, "Berlino":10, "Milano":9}
zonesMetrics = pd.read_csv("../input/"+gv.provider+"_ValidZones.csv")


def printMatrix(xynew):

    print ("R:", gv.NRows, "C:", gv.NColumns)
    matrix={}
    for row in range(gv.NRows):
        matrix[row] ={}
        for col in range (gv.NColumns, 0, -1):
            matrix[row][col] = " "

    for id in zonesMetrics["id"]:
        # if id == 689:
        #     print ("caselle beccata")
        #     couple = sf.zoneIDtoMatrixCoordinates(id)
        #     print(couple)
        #     matrix[couple[2]][couple[1]] = 'C'
        couple = sf.zoneIDtoMatrixCoordinates(id)
        matrix[couple[2]][couple[1]] = '\''
    for k in xynew.keys():
        couple = xynew[k]
        matrix[couple[1]][couple[0]] = str(k)

    print("\t")
    for i in range(gv.NColumns+1, 0, -1):
        if i == gv.NColumns+1:
            print("\t", end='')
        else:
            print(str(i) + "\t", end='')
    print()

    for row in range(gv.NRows-1, -1, -1):
        print(str(row) + "\t", end='')
        for col in range (gv.NColumns, 0, -1):
            print(matrix[row][col] + "\t", end='')
        print()
    return

def exploreNeighbours():
    xynew = {}
    direction = {}
    myindex=np.random.randint(len(RechargingStation_Zones), size = 1)[0]
    ID=RechargingStation_Zones[myindex]
    retv = sf.zoneIDtoMatrixCoordinates(ID)
    xy= [retv[1],retv[2]]

    max=2
    i=0
    for dst in range(1,max):
        xynew[i]=[xy[0]-dst, xy[1]]
        direction[i] = [0, xy[0]-dst, xy[1]]

        xynew[i+1]=[xy[0], xy[1]-dst]
        direction[i+1] = [1, xy[0], xy[1]-dst]

        xynew[i+2]=[xy[0]+dst, xy[1]]
        direction[i+2] = [2, xy[0]+dst, xy[1]]

        xynew[i+3]=[xy[0], xy[1]+dst]
        direction[i+3] = [3, xy[0], xy[1]+dst]
        i=i+1
    # print("expNeigh - dir", direction)
    # print()
    return xynew, direction, myindex



def exploreDirection(directionToFollow):
    center = [directionToFollow[1], directionToFollow[2]]
    # print(center)
    i = 0
    xynew = {}
    direction = {}
    if directionToFollow[0] == 0: ##East
        # print("East")
        while True :
            # print("MC2ID E", MatrixCoordinatesToID(center[0]-(i+1), center[1]))
            if MatrixCoordinatesToID(center[0]-(i+1), center[1]) not in zonesMetrics.id:
                return xynew, direction
            xynew[i] = [center[0]-(i+i), center[1]]
            direction[i] = [directionToFollow[0], center[0]-(i+i), center[1]]
            i = i + 1

    elif directionToFollow[0] == 1: ##South
        # print("South")
        while True :
            # print("MC2ID S", MatrixCoordinatesToID(center[0], center[1]-(i+1)))
            if MatrixCoordinatesToID(center[0], center[1]-(i+1)) not in zonesMetrics.id:
                return xynew, direction
            xynew[i] = [center[0], center[1]-(i+i)]
            direction[i] = [directionToFollow[0], center[0], center[1]-(i+i)]
            i = i + 1

    elif directionToFollow[0] == 2: ##West
        # print("West")
        while True :
            # print("MC2ID W",MatrixCoordinatesToID(center[0]+(i+1), center[1]))
            if MatrixCoordinatesToID(center[0]+(i+1), center[1]) not in zonesMetrics.id:
                return xynew, direction
            xynew[i] = [center[0]+(i+1), center[1]]
            direction[i] = [directionToFollow[0], center[0]+(i+1), center[1]]
            i = i + 1

    elif directionToFollow[0] == 3: ##North
        # print("North")
        while True :
            # print("MC2ID N", MatrixCoordinatesToID(center[0], center[1]+(i+1)))
            if MatrixCoordinatesToID(center[0], center[1]+(i+1)) not in zonesMetrics.id:
                return xynew, direction
            xynew[i] = [center[0], center[1]+(i+i)]
            direction[i] = [directionToFollow[0], center[0], center[1]+(i+i)]
            i = i + 1
    else:
        print("Direction Errore", directionToFollow[0])
        return xynew, direction

def copyFileFromServer(provider, policy, algorithm, numberOfStations, acs, tt, wt, utt, pt, lastS):
    server = "cocca@bigdatadb.polito.it:/data/03/Carsharing_data/output/Simulation_%s/"%lastS
    namefile = "%s_%s_%s_%s_%s_%s_%s_%s_%s.txt"%(provider, policy, algorithm, numberOfStations, acs, tt, wt, utt, pt)
    dstDir = "../output/Simulation_%s/"%lastS
    bashCommand = "scp "+ server+namefile + " " +dstDir+namefile
    os.system(bashCommand)
    return


def main(par_numberOfStations):
    iniTimeSim = datetime.datetime.now()
    ##TO AVOID: "OSError: [Errno 24] Too many open files"
    # bashCommand = "ulimit -n 32768"
    # process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    # process.communicate()
    ###
    
    ## To pass in sys ##
    walkingTreshold = 1000000
    city = "Torino"
    zones = sf.numberOfZones(city)
    algorithm = "max-parking"
    numberOfStations = par_numberOfStations
    tankThreshold = 25 
    AvaiableChargingStations = 4
    BestEffort = True
    utt = 100
    pThreshold = 0.5
    randomInitLvl = False
   

    batcmd = 'ssh bigdatadb hadoop fs -ls /user/cocca/Simulator/output/' #Solo per controllare il ticket
    try:
        output = subprocess.check_output(batcmd, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        output = e.output
        if("Kerberos" in str(output)): 
            print("ERROR: Kerberos Token not present. \n \
            Please log in the bigdata server to request kerberos Token")
            exit(-1)
            
    lastS = c2id[city]

    copyFileFromServer(gv.provider, "Hybrid", algorithm, numberOfStations, str(4),
                       str(25), walkingTreshold, str(100), str(int(pThreshold*100)), str(lastS))


    a = datetime.datetime.now()
    Stamps_Events = pickle.load( open( "../events/"+ gv.provider+"_sorted_dict_events_obj.pkl", "rb" ) )
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Load Events: "+str(int(c)))

    a = datetime.datetime.now()
    global DistancesFrom_Zone_Ordered
    DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+gv.provider+"_ZoneDistances.p", "rb" ) )
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Load Zones: "+str(int(c)))
    ZoneCars = pickle.load( open( "../input/"+gv.provider+"_ZoneCars.p", "rb" ) )

    a = datetime.datetime.now()
    global RechargingStation_Zones
    RechargingStation_Zones = sf.loadRecharing(algorithm, numberOfStations)
    # for element in RechargingStation_Zones:
    #     if element not in list(zonesMetrics.id):
    #         print(element, "not present")
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Load Recharging: "+str(int(c)))

    jobs = []

    '''Entra qui solo se non carico abbastanza stazioni, cioè mai'''
    while len(RechargingStation_Zones)<numberOfStations:
        rn = np.random.randint(gv.NColumns*gv.NRows, size = 1)[0]
        if(rn not in RechargingStation_Zones): RechargingStation_Zones.append(rn)

    results = ""
    k=0
    step=0
    manager = multiprocessing.Manager()

    global followDirection
    followDirection = False


    while step <=1e3:
        return_dict = manager.dict()

        if step % 100 == 0:
            print("Iteration #", step)

        if followDirection == True :
            # print ("In FD")
            xynew, direction = exploreDirection(directionToFollow)
            if len(xynew) == 0 :
                # print (step, "in Len")
                ## On the board, No exploring in depth
                xynew , direction, myindex = exploreNeighbours()
        else:
            ## non c'è unq direzione da seguire, quindi prendo random
            # print(step, "not FD")
            xynew , direction, myindex = exploreNeighbours()

        # print("MyIndex:", myindex)

        IDn=-1
        RechargingStation_Zones_new = {}
        for k in range(0,len(xynew)):
            IDn = MatrixCoordinatesToID(xynew[k][0], xynew[k][1])
            if IDn in zonesMetrics.id and IDn not in RechargingStation_Zones :
                RechargingStation_Zones_new[k]=RechargingStation_Zones.copy()
                RechargingStation_Zones_new[k][myindex] = IDn

        for i in RechargingStation_Zones_new:
            # print("RSZ_new",i, RechargingStation_Zones_new[i])
            p = Process(target=RunSim,args = (BestEffort,
                                              algorithm.replace("_","-"),
                                              algorithm,
                                              AvaiableChargingStations,
                                              tankThreshold,
                                              walkingTreshold,
                                              ZoneCars,
                                              RechargingStation_Zones_new[i],
                                              Stamps_Events,
                                              DistancesFrom_Zone_Ordered,
                                              lastS,
                                              utt,
                                              pThreshold,
                                              randomInitLvl,
                                              return_dict,
                                              i,
                                              direction[i]))
            k+=1
            jobs.append(p)
            p.start()


        for proc in jobs:
            proc.join()

        followDirection = False
        for val in return_dict.values():

            new_results = val
            # print("PID:", new_results["ProcessID"], "Dir:",new_results["Direction"])
            # print("\nNEW STEP")
            # print(RechargingStation_Zones_new[int(new_results["ProcessID"])])
            
            if results == "" or \
                        (new_results["PercDeath"] <= results["PercDeath"]
                     and new_results["MeanMeterEnd"] < results["MeanMeterEnd"]):

                followDirection = True
                directionToFollow = new_results["Direction"]

                fout = open("../output/best_solutions_"+city+"_"+str(numberOfStations)+".txt","a")
                RechargingStation_Zones=RechargingStation_Zones_new[int(new_results["ProcessID"])].copy()

                print("\nNEW BEST SOLUTION FOUND")
                print("**********************************************************************")
                if(results!=""):
                    print("Old: %.2f %.2f"%(results["PercDeath"],results["MeanMeterEnd"]))
                print("New: %.2f %.2f"%(new_results["PercDeath"],new_results["MeanMeterEnd"]))
                print("**********************************************************************")

                fout.write("\nNEW BEST SOLUTION FOUND\n")
                fout.write("**********************************************************************\n")
                fout.write("Nsteps: %d"%step+"\n")
                if(results!=""):
                    fout.write("Old: %.2f %.2f\n"%(results["PercDeath"],results["MeanMeterEnd"]))
                fout.write("New: Deaths=%.2f MeanMeterEnd=%.2f\n"%(new_results["PercDeath"],new_results["MeanMeterEnd"]))
                fout.write(str(RechargingStation_Zones)+"\n")
                fout.write(str(results)+"\n")
                fout.write("**********************************************************************\n")
                fout.close()

                results=new_results.copy()

                print(RechargingStation_Zones)
                print(results)


        step+=1
    print(str(1e3), "Iteration done in", (datetime.datetime.now() - iniTimeSim)/60, "minutes")



    #PercRerouteEnd, PercRerouteStart, PercRecharge,PercDeath, MedianMeterEnd, MeanMeterEnd, MedianMeterStart, MeanMeterStart, NEnd, NStart = \
    #        RunSim(algorithm,numberOfStations,tankThreshold,walkingTreshold,ZoneCars,Stamps_Events,RechargingStation_Zones,DistancesFrom_Zone_Ordered)

    #print("PercRerouteEnd, PercRerouteStart, PercRecharge, PercDeath, MedianMeterEnd, MeanMeterEnd, MedianMeterStart, MeanMeterStart, NEnd, NStart")
    #print(PercRerouteEnd, PercRerouteStart, PercRecharge,PercDeath, MedianMeterEnd, MeanMeterEnd, MedianMeterStart, MeanMeterStart, NEnd, NStart)

    #PercRerouteEnd, PercRerouteStart, PercRecharge,PercDeath, MedianMeterEnd, MeanMeterEnd, MedianMeterStart, MeanMeterStart, NEnd, NStart = \
    #        RunSim(algorithm,numberOfStations,tankThreshold,walkingTreshold,ZoneCars,Stamps_Events,RechargingStation_Zones,DistancesFrom_Zone_Ordered)

    #print("PercRerouteEnd, PercRerouteStart, PercRecharge, PercDeath, MedianMeterEnd, MeanMeterEnd, MedianMeterStart, MeanMeterStart, NEnd, NStart")
    #print(PercRerouteEnd, PercRerouteStart, PercRecharge,PercDeath, MedianMeterEnd, MeanMeterEnd, MedianMeterStart, MeanMeterStart, NEnd, NStart)

jobs=[]

jobs=[]
# for noz in [12,13,14,15,17,19,21,23,25]
# for noz in [2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,19,21,23,25,27, 29, 31, 33, 35, 37, 39, 41]:
for noz in [43,45,47,49,51,53]:
   main(noz)
   # jobs.append(p)
   # p.start()
    

# initZones = [ 226, 208, 224, 262, 210, 243, 246, 169, 154, 229, 261, 244]
# finalZones = [226, 251, 221, 262, 210, 44,  246, 187, 172, 229, 261, 245]
# zones  = zonesMetrics.set_index("id")
# zones.loc[initZones].to_csv("/Users/mc/Desktop/TorInit12.csv")
# zones.loc[finalZones].to_csv("/Users/mc/Desktop/Torfinal12.csv")






