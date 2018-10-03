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
city = 'Torino'
gv.init()
sf.assingVariables(city)
import pprint
pp = pprint.PrettyPrinter(indent=4)
c2id = {"Vancouver":6, "Torino":7, "Berlino":10, "Milano":9}
global zonesMetrics
zonesMetrics = pd.read_csv("../input/"+ city + "_" +gv.provider+"_ValidZones.csv")

import pprint
pp = pprint.PrettyPrinter()
refer_time = int(time.time())


def printMatrix(xynew):
    '''
    function which prints the zones with a charging station
    '''
    print ("R:", gv.NRows, "C:", gv.NColumns)
    matrix={}
    for row in range(gv.NRows):
        matrix[row] ={}
        for col in range (gv.NColumns, 0, -1):
            matrix[row][col] = " "

    for id in zonesMetrics["id"]:
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

def printMatrix_CS(CS_placement):
    '''
    function which prints the zones with a charging station
    '''
    print ("R:", gv.NRows, "C:", gv.NColumns)
    matrix={}
    for row in range(gv.NRows):
        matrix[row] ={}
        for col in range (gv.NColumns, 0, -1):
            matrix[row][col] = " "

    for id in zonesMetrics["id"]:
        couple = sf.zoneIDtoMatrixCoordinates(id)
        matrix[couple[2]][couple[1]] = '\''
    k=0
    for cs in CS_placement:
        couple = sf.zoneIDtoMatrixCoordinates(cs)
        matrix[couple[2]][couple[1]] = str(k)
        k+=1

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

def print_new_solution(results, new_results, fit_impr_perc, fitness_old, fitness_new, RechargingStation_Zones, step):
    print("\nNEW BEST SOLUTION FOUND at step "+ str(step))
    print("**********************************************************************")
    if(results!=""):
        print("Old: %.2f %.2f %.2f"
                   %(results["PercDeath"],results['MeanMeterEnd'], results["WeightedWalkedDistance"])
             )
    print("New: %.2f %.2f %.2f"
                   %(new_results["PercDeath"],new_results['MeanMeterEnd'], new_results["WeightedWalkedDistance"])
             )
    print (RechargingStation_Zones)
    print (pp.pprint(new_results))

    print("fit old: %.2f, fit new: %.2f"%(fitness_old, fitness_new))
    print ('fit_impr_perc', fit_impr_perc)
    print("**********************************************************************")

def write_new_solution(fout, results, new_results, fit_impr_perc, fitness_old, fitness_new,
                       RechargingStation_Zones, step, end_sim_time):
    fout.write("\nNEW BEST SOLUTION FOUND\n")
    fout.write("**********************************************************************\n")
    fout.write("Nsteps: %d"%step+"\n")
    if(results!=""):
        fout.write("Old - Deaths=%f\tMeanMeterEnd=%f\twwd=%.2f\n"
                   %(results["PercDeath"],results['MeanMeterEnd'], results["WeightedWalkedDistance"])
                   )
    fout.write("New - Deaths=%f\tMeanMeterEnd=%f\twwd=%.2f\n"
                   %(new_results["PercDeath"], new_results['MeanMeterEnd'], new_results["WeightedWalkedDistance"])
               )
    fout.write("old fitness: %.4f - new fitness: %.4f\n"%(fitness_old, fitness_new))
    fout.write("Fitness iprovemnt[%]: " + str(fit_impr_perc) + "\n")
    fout.write('CS_placement: ' + str(RechargingStation_Zones)+'\n')
    fout.write(str(results))
    fout.close()

def exploreNeighbours():
    '''
    extract a random index to chose which is the station to move
    :return: logical coordinates of NESW zones, NESW direction, logical index of the zone
    '''
    xynew = {}
    direction = {}
    myindex=np.random.randint(len(RechargingStation_Zones), size = 1)[0]
    ID=RechargingStation_Zones[myindex]
    print ('extracted ID',ID)
    retv = sf.zoneIDtoMatrixCoordinates(ID)
    xy= [retv[1],retv[2]]

    max=2 ## increment to have more choicee
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
        i=i+4
    # print("expNeigh - dir", direction)
    # print()
    return xynew, direction, myindex

def solution_already_exists(new_sol, tested_sol):
    my_sol = sorted(new_sol)
    key = "-".join([str(e) for e in my_sol])
    return key in tested_sol.keys()


def cost_function(results):
    if results == "": return 100000
    else: return 100000 * results['PercDeath'] + results['WeightedWalkedDistance']


def exploreDirection(directionToFollow):
    '''
    :param directionToFollow: direction to follow (composed by the logical coordinates)
    :return: set of solutions, starting index

    add all the zones in a given direcation until the border
    '''
    center = [directionToFollow[1], directionToFollow[2]]
    ID = MatrixCoordinatesToID(center[0],center[1])
    print ('extracted ID',ID)
    print ('Direction', directionToFollow[0])
    i = 0
    xynew = {}
    direction = {}
    if directionToFollow[0] == 0: ##East
        # print("East")
        while True :
            print("MC2ID E", MatrixCoordinatesToID(center[0]-(i+1), center[1]))
            if MatrixCoordinatesToID(center[0]-(i+1), center[1]) not in list(zonesMetrics.id) \
                    or len(xynew) > gv.NColumns - center[0]%gv.NColumns:
                return xynew, direction
            xynew[i] = [center[0]-(i+i), center[1]]
            direction[i] = [directionToFollow[0], center[0]-(i+i), center[1]]
            i = i + 1

    elif directionToFollow[0] == 1: ##South
        # print("South")
        while True :
            # print("MC2ID S", MatrixCoordinatesToID(center[0], center[1]-(i+1)))
            if MatrixCoordinatesToID(center[0], center[1]-(i+1)) not in list(zonesMetrics.id):
                return xynew, direction
            xynew[i] = [center[0], center[1]-(i+1)]
            direction[i] = [directionToFollow[0], center[0], center[1]-(i+1)]
            i = i + 1

    elif directionToFollow[0] == 2: ##West
        # print("West")
        while True :
            print("MC2ID W",MatrixCoordinatesToID(center[0]+(i+1), center[1]))
            if MatrixCoordinatesToID(center[0]+(i+1), center[1]) not in list(zonesMetrics.id)\
                     or len(xynew) > gv.NColumns - center[0]%gv.NColumns:
                return xynew, direction
            xynew[i] = [center[0]+(i+1), center[1]]
            direction[i] = [directionToFollow[0], center[0]+(i+1), center[1]]
            i = i + 1

    elif directionToFollow[0] == 3: ##North
        # print("North")
        while True :
            # print("MC2ID N", MatrixCoordinatesToID(center[0], center[1]+(i+1)))
            if MatrixCoordinatesToID(center[0], center[1]+(i+1)) not in list(zonesMetrics.id):
                return xynew, direction
            xynew[i] = [center[0], center[1]+(i+i)]
            direction[i] = [directionToFollow[0], center[0], center[1]+(i+1)]
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

def RunSim_Toy(RechargingStation_Zones, processID, direction, return_dict):
    RetValues = {}
    RetValues["ProcessID"] = processID
    RetValues["Sum"] = sum(RechargingStation_Zones)
    RetValues['Direction'] = direction
    return_dict[processID] = RetValues
    return sum(RechargingStation_Zones)

def cost_function_toy(results):
    return results['Sum']


def main(par_numberOfStations):
    '''
    :param par_numberOfStations: #CS placed in the city
    :return: the optimal CS placement
    '''
    iniTimeSim = datetime.datetime.now()
    
    walkingTreshold = 1000000
    city = "Torino"
    zones = sf.numberOfZones(city)
    algorithm = "max-parking"
    numberOfStations = 18
    tankThreshold = 25 
    AvaiableChargingStations = 4
    BestEffort = True
    pThreshold = 1
    randomInitLvl = False

    global tested_solution
    tested_solution = {}

    batcmd = 'ssh bigdatadb hadoop fs -ls /user/cocca/Simulator/output/' #Solo per controllare il ticket
    lastS = -1
    try:
        output = subprocess.check_output(batcmd, stderr=subprocess.STDOUT, shell=True)
        if(len(str(output))<5): lastS = 0
        else: lastS = int(str(output).split(" ")[1]) + 1
    except subprocess.CalledProcessError as e:
        output = e.output
        if("Kerberos" in str(output)):
            print("ERROR: Kerberos Token not present. \n \
            Please log in the bigdata server to request kerberos Token")
            exit(-1)


    '''
    Trace, city config and CS placement upload
    '''
    a = datetime.datetime.now()
    Stamps_Events = pickle.load( open( "../events/"+ city + "_" + gv.provider+"_sorted_dict_events_obj.pkl", "rb" ) )
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Load Events: "+str(int(c)))

    a = datetime.datetime.now()
    global DistancesFrom_Zone_Ordered
    DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ city + "_" +gv.provider+"_ZoneDistances.p", "rb" ) )
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Load Zones: "+str(int(c)))
    ZoneCars = pickle.load( open( "../input/"+ city + "_" +gv.provider+"_ZoneCars.p", "rb" ) )

    a = datetime.datetime.now()
    global RechargingStation_Zones
    RechargingStation_Zones = sf.loadRecharing(algorithm, numberOfStations, city)
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Load Recharging: "+str(int(c)))

    '''Entra qui solo se non carico abbastanza stazioni, cioè mai'''
    while len(RechargingStation_Zones)<numberOfStations:
        rn = np.random.randint(gv.NColumns*gv.NRows, size = 1)[0]
        if(rn not in RechargingStation_Zones): RechargingStation_Zones.append(rn)

    print('Start', sorted(RechargingStation_Zones))


    results = ""
    step=0
    fit_impr_perc = 100
    manager = multiprocessing.Manager()
    NNI_counter = 0
    fitness_old = 1000

    global followDirection
    followDirection = False

    '''
    optmization
    '''
    while step <=10000 and fit_impr_perc >=0.1:
        print ('while', sorted(RechargingStation_Zones))
        return_dict = manager.dict()

        # if step % 100 == 0:
        print("Iteration #", step)

        if followDirection == True :
            '''
            If the algorithm has a direction to follow, follow that direction
            '''
            # print ("In FD")
            xynew, direction = exploreDirection(directionToFollow)
            if len(xynew) == 0 :
                # print (step, "in Len")
                ## On the board, No exploring in depth
                xynew , direction, myindex = exploreNeighbours()
        else:
            '''
            If there is not any direction, explore some random direction point with the NSEW neighbors
            '''
            xynew , direction, myindex = exploreNeighbours()
        # print("MyIndex:", myindex)

        RechargingStation_Zones_new = {}
        '''
        xynew contains the ID + logical coordniates of the station which is going to be changed in the CSplacement
        explore xynew to create a new CSP
        check the if the solution was not already tested
        '''
        for k in range(0,len(xynew)):
            IDn = MatrixCoordinatesToID(xynew[k][0], xynew[k][1])
            if IDn in list(zonesMetrics.id) and IDn not in RechargingStation_Zones:
                tmp=RechargingStation_Zones.copy()
                tmp[myindex] = IDn
                if solution_already_exists(tmp, tested_solution) == True :
                    # print('solution tested', "-".join(str(e) for e in sorted(tmp)) )
                    RechargingStation_Zones_new[k] = []

                else:
                    RechargingStation_Zones_new[k]=tmp
                    my_sol = sorted( RechargingStation_Zones_new[k])
                    key = "-".join([str(e) for e in my_sol])
                    tested_solution[key] = True
                    # print('not used', key)

        Sol2Test = {}
        sol_index = 0
        for k in RechargingStation_Zones_new.keys():
            if len(RechargingStation_Zones_new[k]) > 0:
                Sol2Test[sol_index] = RechargingStation_Zones_new[k]
                sol_index+=1
        print ('before', len(RechargingStation_Zones_new))
        RechargingStation_Zones_new= Sol2Test.copy()
        print('after', len(RechargingStation_Zones_new))

        if len(RechargingStation_Zones_new) > 50:
            print ('ouch')
            fdebug = open('debug.txt', 'w')
            fdebug.write('len xwnew: '+ str(len(xynew)) +"\n")
            fdebug.write('myindex: ' + str(myindex)+"\n")
            fdebug.write('ID: ' + str(RechargingStation_Zones[myindex])+"\n")
            fdebug.write('Rech: ' + ' '.join(str(e) for e in RechargingStation_Zones)+"\n")
            fdebug.write('Direction' + str(directionToFollow) +"\n")
            fdebug.write('-----\n')
            for k in RechargingStation_Zones_new.keys():
                fdebug.write(' '.join(str(e) for e in RechargingStation_Zones_new[k])+'\n')
            fdebug.close()
            input()

        if len(RechargingStation_Zones_new) == 0 :
            print ('NNI')
            print ('FD', followDirection)
            followDirection = False
            NNI_counter+=1
            if NNI_counter == 36: break
            continue
        NNI_counter = 0


        jobs = []
        start_sim_time = time.time()
        # RechargingStation_Zones_new2 = {}
        # RechargingStation_Zones_new2[0] = RechargingStation_Zones_new[0]
        for i in RechargingStation_Zones_new:
            print("RSZ_new",i, RechargingStation_Zones_new[i])
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
                                              pThreshold,
                                              2,
                                              randomInitLvl,
                                              return_dict,
                                              i,
                                              direction[i],
                                              city))

            # p = Process(target=RunSim_Toy,args = (
            #                       RechargingStation_Zones_new[i],
            #                       i,
            #                       direction[i],
            #                       return_dict,
            #                       ))

            jobs.append(p)
            p.start()


        for proc in jobs:
            proc.join()

        end_sim_time = time.time() - start_sim_time
        print ('Time for %d sim: %d s'%(len(RechargingStation_Zones_new), end_sim_time))
        print()

        followDirection = False

        '''
        # Results analysis
        '''
        for val in return_dict.values():

            new_results = val

            '''
            Optimality condition
            if results (previous solution) is empity or the optimality condtion is true
            A direction to follow has been found,
            saving solutions
            '''
            #
            fitness_new = cost_function(new_results)
            if  results == "" or fitness_new <= fitness_old:
                if results == "" :
                    fit_impr_perc = 100
                    fitness_old = 1e8
                else:
                    fit_impr_perc = (fitness_old - fitness_new)*100/fitness_old

                followDirection = True

                directionToFollow = new_results["Direction"]
                RechargingStation_Zones=RechargingStation_Zones_new[int(new_results["ProcessID"])].copy()


                print_new_solution(results, new_results, fit_impr_perc, fitness_old, fitness_new, RechargingStation_Zones, step)
                fout = open("../output/best_solutions_"+city+"_"+str(numberOfStations)+"_"+refer_time+".txt","a")
                write_new_solution(fout, results, new_results, fit_impr_perc, fitness_old, fitness_new,
                           RechargingStation_Zones, step, end_sim_time)

            results=new_results.copy()
            fitness_old = fitness_new

        step+=1
        print()
    print(step+NNI_counter, "Iteration done in", (datetime.datetime.now() - iniTimeSim)/60, "minutes")


for noz in [5]:
   main(noz)







