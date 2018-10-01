import sys
import os
import subprocess
import time
p = os.path.abspath('..')
sys.path.append(p+"/")
sys.path.append(p+"/Simulator/")

from Simulator.Simulator import *
import datetime as datetime
import pickle
import multiprocessing
from multiprocessing import Process
import subprocess

import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv

city = 'Torino'
gv.init()
sf.assingVariables(city)

walkingTreshold = 1000000
city = "Torino"
zones = sf.numberOfZones(city)
algorithm = "max-parking"
numberOfStations = 5
tankThreshold = 25
AvaiableChargingStations = 4
BestEffort = True
pThreshold = 0.5
randomInitLvl = False
return_dict = {}

ZoneCars = pickle.load( open( "../input/"+ city + "_" + gv.provider +"_ZoneCars.p", "rb" ) )
DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ city + "_" + gv.provider + "_ZoneDistances.p", "rb" ) )
Stamps_Events = pickle.load( open( "../events/"+ city + "_" + gv.provider + "_sorted_dict_events_obj.pkl", "rb" ) )

RechargingStation_Zones = sf.loadRecharing(algorithm, numberOfStations, city)
p = Process(target=RunSim,args = (BestEffort,
                                  algorithm.replace("_","-"),
                                  algorithm,
                                  AvaiableChargingStations,
                                  tankThreshold,
                                  walkingTreshold,
                                  ZoneCars,
                                  RechargingStation_Zones,
                                  Stamps_Events,
                                  DistancesFrom_Zone_Ordered,
                                  46,
                                  pThreshold,
                                  2,
                                  randomInitLvl,
                                  return_dict,
                                  6,
                                  1,
                                  city))
p.start()


