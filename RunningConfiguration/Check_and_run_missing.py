import sys
import os
import subprocess
import time
p = os.path.abspath('..')
sys.path.append(p + "/")
sys.path.append(p + "/Simulator/")

from Simulator.Simulator import *
import datetime as datetime
import pickle
import multiprocessing
from multiprocessing import Process
import subprocess


import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv



def fill_int_field(tofill, data):
    
    ds = data.split()
    
    for d in ds: tofill.append(int(d))
    return

def fill_str_field(tofill, data):
    
    ds = data.split()
    
    for d in ds: tofill.append(d)
    return

def fill_bool_field(tofill, data):
    
    ds = data.split()
    
    for d in ds:
        if(d == "True"):
            tofill.append(True)
        else:
            tofill.append(False)
    return

def CheckMissingHDFS(lastS):
    files = []
    Missing = []
    batcmd = "ssh bigdatadb ls /data/03/Carsharing_data/output/Simulation_" + str(lastS) + "/"
    try:
        output = subprocess.check_output(batcmd, stderr=subprocess.STDOUT, shell=True)
        files = str(output).strip()[2:-3].split("\\n")

    except subprocess.CalledProcessError as e:
        output = e.output
        print(str(output))
        exit(-1)
    
    filesHDFS = []

    batcmd = "ssh bigdatadb hdfs dfs -ls /user/cocca/Simulator/output/Simulation_" + str(lastS) + "/"
    try:
        output = subprocess.check_output(batcmd, stderr=subprocess.STDOUT, shell=True)        
        tmp = str(output).strip()[2:-3].split("\\n")
        for row in tmp:
            filesHDFS.append(row.split("/")[-1])

    except subprocess.CalledProcessError as e:
        output = e.output
        print(str(output))
        exit(-1)

    for f in files:
        if(f not in filesHDFS):
            Missing.append(f)
    print("Missing on HDFS %d files" % len(Missing))
    return Missing


def putHDFS(lastS, Missing):
    for fname in Missing:
        print(fname)
        os.system('ssh bigdatadb hdfs dfs -put -f /data/03/Carsharing_data/output/Simulation_%d/%s Simulator/output/Simulation_%s/%s' % (lastS, fname, lastS, fname))
    return

def CheckMissing(lastS):
    output_directory = "../output/Simulation_" + str(lastS) + "/"  
    fin = open(output_directory + "configuration.txt")
    
    BestEffort_list = []
    algorithm_list = []
    AvaiableChargingStations_list = []
    numberOfStations_list = []
    tankThresholds_list = []
    walkingTreshold = 1000000
    upperTankThreshold_list = []
    pThresholds_list = []

    for line in fin:
        ls = line.split(":")
        if(ls[0] == "BestEffort"): fill_bool_field(BestEffort_list, ls[1])
        if(ls[0] == "Algorithm"): fill_str_field(algorithm_list, ls[1])
        if(ls[0] == "NumberOfStations"): fill_int_field(numberOfStations_list, ls[1])
        if(ls[0] == "AvaiableChargingStations"): fill_int_field(AvaiableChargingStations_list, ls[1])
        if(ls[0] == "TankThresholds"): fill_int_field(tankThresholds_list, ls[1])
        if(ls[0] == "upperTankThreshold"): fill_int_field(upperTankThreshold_list, ls[1])
        if(ls[0] == "pThreshold"): fill_int_field(pThresholds_list, ls[1])
    
    '''
    Simulation Configuration: 
    BestEffort: False True 
    AvaiableChargingStations: 2 3 4 5 6 7 8 
    Algorithm: max-parking max-time avg-time 
    NumberOfStations: 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30 32 34 36 38 40 
    TankThresholds: -1 5 10 15 20 25 50     
    '''
    
    files = []
    
    batcmd = "ssh bigdatadb ls /data/03/Carsharing_data/output/Simulation_" + str(lastS) + "/"
    try:
        output = subprocess.check_output(batcmd, stderr=subprocess.STDOUT, shell=True)
        
        files = str(output).strip()[2:-3].split("\\n")

    except subprocess.CalledProcessError as e:
        output = e.output
        exit(-1)
        

    '''BestEffort_list = [False, True]
    algorithm_list = ["max-parking", "max-time","avg-time"]
    numberOfStations_list = [i for i in range(2,42,2)]#42
    AvaiableChargingStations_list = [2,3,4,5,6,7,8]
    tankThresholds_list = [-1,5,10,15,20,25,50]
    walkingTreshold = 1000000#int(sys.argv[4]) # in [m]'''
    
    Total = 0
    
    #if("car2go_FreeFloating_avg-time_10_2_-1_1000000.txt" not in files): print("asds")
    
    Missing = []
    for BestEffort in BestEffort_list:
        for AvaiableChargingStations in AvaiableChargingStations_list:
            for algorithm in algorithm_list:
                for numberOfStations in numberOfStations_list:
                    for tankThreshold in tankThresholds_list:
                        for utt in upperTankThreshold_list:
                            for pt in pThresholds_list:
                                if sf.validSimulation(BestEffort, tankThreshold, utt, pt) == False:
                                    continue

                                policy, fileID, fname = foutname(BestEffort, algorithm, AvaiableChargingStations,
                                                                 numberOfStations, tankThreshold, walkingTreshold, utt, pt)

                                Total += 1
                                if(fname not in files):
                                    Missing.append((BestEffort, AvaiableChargingStations, algorithm, numberOfStations, tankThreshold, fname, utt, pt))
                                else:
                                    files.remove(fname)
                        
                            
    print("Total Simulations: %d" % Total)
    print("MISSING: %d" % len(Missing))

    return Missing



def RunMissing(Missing, Stamps_Events, DistancesFrom_Zone_Ordered, ZoneCars, lastS, city):
    
    
    nsimulations = 0
    walkingTreshold = 1000000
    jobs = []
    
    for config in Missing:
    
        BestEffort = config[0]
        AvaiableChargingStations = config[1]
        algorithm = config[2]
        numberOfStations = config[3]
        tankThreshold = config[4]
        upperTankThreshold = config[5]
        pThrehshold = config[6]
        
        
        RechargingStation_Zones = loadRecharing(algorithm, numberOfStations)
        p = Process(target=RunSim, args=(BestEffort,
                                          algorithm.replace("_", "-"),
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
                                          pThrehshold,
                                          None,
                                          -1,
                                          city))
        nsimulations += 1
    
        jobs.append(p)
        p.start()
    
        if(len(jobs) > 120):
            time.sleep(.1)  # only to print after other prints
            print("\nWaiting for %d simulations" % len(jobs))
            with click.progressbar(jobs, length=len(jobs)) as bar:
                for proc in bar:
                    proc.join()
            jobs.clear()
            
            print("")
    
    time.sleep(.1)  # only to print after other prints
    print("\nWaiting for %d simulations" % len(jobs))
    with click.progressbar(jobs, length=len(jobs)) as bar:
        for proc in bar:
            proc.join()


    return

def check_and_run_missing(lastS, Stamps_Events, DistancesFrom_Zone_Ordered, ZoneCars, city):

    gv.init()
    sf.assingVariables(city)
    
    Missing = [""]
    
    Run = 0
    while len(Missing) > 0:
        Missing = CheckMissing(lastS)        
        RunMissing(Missing, Stamps_Events, DistancesFrom_Zone_Ordered, ZoneCars, lastS, city)
        Run += 1
        if(Run > 10):
            print("###ERROR### Impossible to conclude simulations after 10 attempts. Still Missing: %d simulations" % len(Missing))
            return -1

    Run = 0
    Missing = [""]
    while len(Missing) > 0:
        print("HDFS")
        Missing = CheckMissingHDFS(lastS)  
        putHDFS(lastS, Missing) 
        Run += 1
        if(Run > 10):
            print("###ERROR### Impossible to copy on HDFS all files after 10 attempts. Still Missing: %d files" % len(Missing))
            return -1
    return 0


def main():

    lastS = 10
    # Missing = CheckMissing(29)
   
    print("#START Loading#")
    aglobal = datetime.datetime.now()
    nsimulations = 0
    a = datetime.datetime.now()
    Stamps_Events =  pickle.load( open( "../events/"+ gv.provider + "_sorted_dict_events_obj.pkl", "rb" ) )
    b = datetime.datetime.now()    
    c = (b - a).total_seconds()
    print("End Loading Events: " + str(int(c)))


    a = datetime.datetime.now()    
    DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ gv.provider + "_ZoneDistances.p", "rb" ) )
    b = datetime.datetime.now()    
    c = (b - a).total_seconds()
    print("End Loading Zones: " + str(int(c)))
    
    a = datetime.datetime.now()    
    ZoneCars = pickle.load( open( "../input/"+ gv.provider +"_ZoneCars.p", "rb" ) )
    b = datetime.datetime.now()    
    c = (b - a).total_seconds()
    print("End Loading Cars: " + str(int(c)))

    check_and_run_missing(lastS, Stamps_Events, DistancesFrom_Zone_Ordered, ZoneCars, city)

    # print(len(Missing))
    return


# main()
