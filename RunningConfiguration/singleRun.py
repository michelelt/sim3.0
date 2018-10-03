import sys
import os
import subprocess
import time
p = os.path.abspath('..')
sys.path.append(p+"/")
sys.path.append(p+"/Simulator/")

from Simulator.Simulator_DG import *
import datetime as datetime
import pickle
import multiprocessing
from multiprocessing import Process
import subprocess

import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv

import pprint
pp = pprint.PrettyPrinter()

import cProfile
city = 'Torino'
gv.init()
sf.assingVariables(city)

walkingTreshold = 1000000
city = "Torino"
zones = sf.numberOfZones(city)
algorithm = "max-parking"
numberOfStations = 20
tankThreshold = 25
AvaiableChargingStations = 4
BestEffort = False
pThreshold = 0.5
randomInitLvl = False
return_dict = {}

ZoneCars = pickle.load( open( "../input/"+ city + "_" + gv.provider +"_ZoneCars.p", "rb" ) )
DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ city + "_" + gv.provider + "_ZoneDistances.p", "rb" ) )
Stamps_Events = pickle.load( open( "../events/"+ city + "_" + gv.provider + "_sorted_dict_events_obj.pkl", "rb" ) )

# for k in DistancesFrom_Zone_Ordered.keys():
#       print('key', k)
# k=2
# for el in DistancesFrom_Zone_Ordered[k]:
      # print(type(el))
      # print('zone',el[0])
      # print('zones dst', el[1].getZones())
      # print(el[1].getDistance())
      # print()




RechargingStation_Zones = sf.loadRecharing(algorithm, numberOfStations, city)
# RechargingStation_Zones = [1,2,3,4,6]
print(RechargingStation_Zones)

zzz = RunSim(BestEffort,
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
      city)


for k in zzz.keys():
      print(k, zzz[k])
