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
gv.init()
sf.assingVariables()

algorithm="max-parking"
RechargingStation_Zones=2
AvaiableChargingStations=4
tankThreshold=25
walkingTreshold=1000000
utt=100
p=0
lastS = 9
pt = 0
BestEffort = True
randomStrtingLevel = False

ZoneCars = pickle.load( open( "../input/"+ gv.provider +"_ZoneCars.p", "rb" ) )
DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ gv.provider + "_ZoneDistances.p", "rb" ) )
Stamps_Events = pickle.load( open( "../events/"+ gv.provider + "_sorted_dict_events_obj.pkl", "rb" ) )

RechargingStation_Zones = loadRecharing(algorithm, RechargingStation_Zones)
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
                                  lastS,
                                  utt,
                                  pt,
                                  None,
                                  randomStrtingLevel,
                                  -1))
p.start()


