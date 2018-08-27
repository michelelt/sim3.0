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

from Check_and_run_missing import check_and_run_missing



def main():


    BestEffort_list = [False]
    algorithm_list = ["max-parking"]
    numberOfStations_list = []#42
    for i in range(70,71,5): numberOfStations_list.append(i)
    AvaiableChargingStations_list = [4]
    tankThresholds_list = [25]
    walkingTreshold = 1000000#int(sys.argv[4]) # in [m]

    print("Total Simulations: %d"%(len(BestEffort_list)*len(AvaiableChargingStations_list)*
                                   len(algorithm_list)*len(numberOfStations_list)*len(tankThresholds_list)))
    
    
    print("#START Loading#")
    aglobal = datetime.datetime.now()
    nsimulations = 0
    a = datetime.datetime.now()
    Stamps_Events = pickle.load( open( "../events/"+ gv.provider + "_sorted_dict_events_obj.pkl", "rb" ) )
    b = datetime.datetime.now()    
    c = (b - a).total_seconds()
    print("End Loading Events: "+str(int(c)))


    a = datetime.datetime.now()    
    DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ gv.provider + "_ZoneDistances.p", "rb" ) )
    b = datetime.datetime.now()    
    c = (b - a).total_seconds()
    print("End Loading Zones: "+str(int(c)))
    
    a = datetime.datetime.now()    
    ZoneCars = pickle.load( open( "../input/"+ gv.provider +"_ZoneCars.p", "rb" ) )
    b = datetime.datetime.now()    
    c = (b - a).total_seconds()
    print("End Loading Cars: "+str(int(c)))
    
    #batcmd = 'ssh bigdatadb hadoop fs -rm -R Simulator/output/*' NON SERVE PIÙ. CREO UNA NUOVA FOLDER PER LE NUOVE SIMULAZIONI. ATTENZIONE ALLA NUMERAZIONE
    
    batcmd = 'ssh bigdatadb hadoop fs -ls /user/cocca/Simulator/output/' #Solo per controllare il ticket
    
    lastS = 100
    '''try:
        output = subprocess.check_output(batcmd, stderr=subprocess.STDOUT, shell=True)
        if(len(str(output))<5): lastS = 0
        else: lastS = int(str(output).split(" ")[1])
    except subprocess.CalledProcessError as e:
        output = e.output
        if("Kerberos" in str(output)): 
            print("ERROR: Kerberos Token not present. \n \
            Please log in the bigdata server to request kerberos Tocken")
            exit(-1)
            '''
    print("#END Loading#\n")
        
    print("Ouput in output/Simulation_%d"%lastS)
    print("Ouput in output_analysis/Simulation_%d\n"%lastS)

    #os.system('ssh bigdatadb mkdir /data/03/Carsharing_data/output/Simulation_%d'%lastS)
    #os.system('ssh bigdatadb mkdir /data/03/Carsharing_data/output_analysis/Simulation_%d'%lastS)
    
     
    if not os.path.exists("../output/Simulation_"+str(lastS)):
        os.makedirs("../output/Simulation_"+str(lastS))
        os.makedirs("../output_analysis/Simulation_"+str(lastS))
        os.makedirs("../output_analysis/Simulation_"+str(lastS)+"/fig")
    
    SimulationConfigFile = "../output_analysis/Simulation_%d/configuration.txt"%lastS
    fout = open(SimulationConfigFile,"w")
    
    fout.write("Simulation Configuration: \n")
    
    str_out = "BestEffort: "
    for val in BestEffort_list:
        str_out+= str(val)+" "
    str_out+= "\n"
    
    str_out += "AvaiableChargingStations: "
    for val in AvaiableChargingStations_list:
        str_out+= str(val)+" "
    str_out+= "\n"
    
    str_out += "Algorithm: "
    for val in algorithm_list:
        str_out+= str(val)+" "
    str_out+= "\n"
    
    str_out += "NumberOfStations: "
    for val in numberOfStations_list:
        str_out+= str(val)+" "
    str_out+= "\n"
    
    str_out += "TankThresholds: "
    for val in tankThresholds_list:
        str_out+= str(val)+" "
    str_out+= "\n"
    
    fout.write(str_out)
    fout.close()
    
    
    #os.system('cp %s ../output/Simulation_%d/configuration.txt' %(SimulationConfigFile,lastS))
    #os.system('cat %s | ssh bigdatadb hdfs dfs -put -f - Simulator/output/Simulation_%s/configuration.txt' %(SimulationConfigFile,lastS))

    jobs=[]
      
    for BestEffort in BestEffort_list:
        for AvaiableChargingStations in AvaiableChargingStations_list:
            for algorithm in algorithm_list:
                print("Running simulations:")
                for numberOfStations in numberOfStations_list:
                    for tankThreshold in tankThresholds_list:
                        if((BestEffort==False and tankThreshold<0) or  (BestEffort==True and tankThreshold==100) ): 
                                continue

                        #if(algorithm=="rnd" and AvaiableChargingStations == 7): continue #REMOVE IS ONLY TO SKIP IN THIS SIMULATION CONFIG
                           
                        RechargingStation_Zones = loadRecharing(algorithm, numberOfStations)
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
                                                          None,
                                                          -1))
                        nsimulations +=1
    
                        jobs.append(p)
                        p.start()

                        if(len(jobs)>120):
                            time.sleep(.1) #only to print after other prints
                            print("\nWaiting for %d simulations"%len(jobs))
                            with click.progressbar(jobs, length=len(jobs)) as bar:
                                for proc in bar:
                                    proc.join()
                            jobs.clear()
                            
    
    
    
    
    
            print("")
    
    time.sleep(.1) #only to print after other prints
    print("\nWaiting for %d simulations"%len(jobs))
    with click.progressbar(jobs, length=len(jobs)) as bar:
        for proc in bar:
            proc.join()

    b = datetime.datetime.now()
    c = (b - aglobal).total_seconds()
    
    
    
    
    return
    if(check_and_run_missing(lastS,Stamps_Events,DistancesFrom_Zone_Ordered,ZoneCars)<0):
        exit(-1)

    
    print("Run %d Simulations took %d seconds"%(nsimulations,c))
    
    print("\nStart Spark Analysis")
    a = datetime.datetime.now()
    
    
    os.system('scp ../Analysis/Spark_Analyzer.py bigdatadb:/tmp/CarSharing_Spark_Analyzer.py')
    os.system('ssh bigdatadb spark2-submit --master yarn --deploy-mode client /tmp/CarSharing_Spark_Analyzer.py %d'%lastS)
    os.system('scp bigdatadb:/data/03/Carsharing_data/output_analysis/Simulation_%d/out_analysis.txt ../output_analysis/Simulation_%d/'%(lastS,lastS))
    
    os.system('scp bigdatadb:/tmp/Carsharing_Output/out_analysis.txt ../output_analysis/Simulation_%d/'%lastS)
    os.system('cat ../output_analysis/Simulation_%d/out_analysis.txt | ssh bigdatadb hdfs dfs -put -f - Simulator/output/Simulation_%s/out_analysis.txt' %(lastS,lastS))
    
    
    
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    
    print("Analyze data with Spark took %d seconds" %(c))
    
    print("\nPlot graphs")
    a = datetime.datetime.now()
    os.system('python3 ../Analysis/plot_heatmap.py %d'%lastS)
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("Plot graphs took %d seconds" %(c))
    
    print("Ouput in output/Simulation_%d"%lastS)
    print("Ouput in output_analysis/Simulation_%d"%lastS)

    return 
        
main()

