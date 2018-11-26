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

# city = sys.argv[1]
city = 'Torino'
gv.init()
sf.assingVariables(city)

from Check_and_run_missing import check_and_run_missing


def numeberOfZones(city):
   # command = 'wc -l ../input/'+city+'_car2go_ValidZones.csv'
   # zones = int(str(subprocess.check_output(command, shell=True)).split(" ")[0][2:5]) - 1
   zones =261

   return zones


def main():

    #TO AVOID: "OSError: [Errno 24] Too many open files"     
    #bashCommand = "ulimit -n 2048"
    #process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    #process.communicate()
    ###
    '''
    RUNNING CONFIGURATION
    '''

    BestEffort_list = [True]
    algorithm_list = ["max-parking"]
    numberOfStations_list = []
    maxZones = numeberOfZones(gv.city)
    numberOfStations_list = []
    for i in range(2                       , round(maxZones*0.05) + 1, 1):  numberOfStations_list.append(i)
    for i in range(round(maxZones*0.05) + 2, round(maxZones*0.1)  + 1, 2): numberOfStations_list.append(i)
    for i in range(round(maxZones*0.1)  + 2, round(maxZones*0.3)  + 1, 5): numberOfStations_list.append(i)
    for i in range(2, round(maxZones*0.3), 2): numberOfStations_list.append(i)
    numberOfStations_list = [5,10,15,20,25,30,35,40,45,50,55,60,65,70,75]
    AvaiableChargingStations_list = [4] #PALINE PER ZONA
    pThresholds = [0.5]
    tankThresholds_list = [25]
    walkingTreshold = 1000000
    randomInitLvl = False
    kwh_list = [2]
    gamma_list = [0, 0.25, 0.5, 0.75, 1]
    # gamma_list = [1, 5, 10]

    
    print("#START Loading#")
    aglobal = datetime.datetime.now()
    nsimulations = 0
    a = datetime.datetime.now()
    Stamps_Events = pickle.load( open( "../events/"+ city + "_" + gv.provider + "_sorted_dict_events_obj.pkl", "rb" ) )
    print ("Stamp Events len: ", len(Stamps_Events))
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Loading Events: "+str(int(c)) + "\n")


    a = datetime.datetime.now()
    DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ city + "_" + gv.provider + "_ZoneDistances.p", "rb" ) )
    print ("DistancesFrom_Zone_Ordered len: ", len(DistancesFrom_Zone_Ordered))
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Loading Zones: "+str(int(c)) + "\n")

    a = datetime.datetime.now()
    ZoneCars = pickle.load( open( "../input/"+ city + "_" + gv.provider +"_ZoneCars.p", "rb" ) )
    print ("ZoneCars: ", len(ZoneCars))
    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    print("End Loading Cars: "+str(int(c)) + "\n")

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
    lastS = 57
    print("#END Loading#\n")

    print("Ouput in output/Simulation_%d"%lastS)
    print("Ouput in output_analysis/Simulation_%d\n"%lastS)

    os.system('ssh bigdatadb mkdir /data/03/Carsharing_data/output/Simulation_%d'%lastS)
    os.system('ssh bigdatadb mkdir /data/03/Carsharing_data/output_analysis/Simulation_%d'%lastS)


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

    str_out += "pThresholds: "
    for val in pThresholds:
       str_out+= str(val*100)+" "
    str_out+= "\n"

    str_out += "gamma: "
    for val in gamma_list:
       str_out+= str(val*100)+" "
    str_out+= "\n"


    fout.write(str_out)
    fout.close()


    os.system('cp %s ../output/Simulation_%d/configuration.txt' %(SimulationConfigFile,lastS))
    os.system('cat %s | ssh bigdatadb hdfs dfs -put -f - Simulator/output/Simulation_%s/configuration.txt' %(SimulationConfigFile,lastS))

    jobs=[]

    configurations = sf.createListConfig(BestEffort_list, AvaiableChargingStations_list, algorithm_list,
                                         numberOfStations_list, tankThresholds_list, pThresholds, kwh_list, 
                                         gamma_list)


    print("Total Simulations: %d"%(len(configurations)))

    for config in configurations:

      # for z in ZoneCars.keys():
      #   if len(ZoneCars[z]) > 0:
      #       for i in range (len(ZoneCars[z])):
      #           ZoneCars[z][i].setRechKwh(config['kwh'])
      #           config['gamma'] = 666
      #           ZoneCars[z][i].setGamma(config['gamma

      RechargingStation_Zones = loadRecharing(config["Algorithm"], config["numberOfStations"], city)
      p = Process(target=RunSim,args = (config["BestEffort"],
                                        config["Algorithm"].replace("_","-"),
                                        config["Algorithm"],
                                        config["AvaiableChargingStations"],
                                        config["tankThreshold"],
                                        walkingTreshold,
                                        ZoneCars,
                                        RechargingStation_Zones,
                                        Stamps_Events,
                                        DistancesFrom_Zone_Ordered,
                                        lastS,
                                        config["pt"],
                                        config['kwh'],
                                        config['gamma'],
                                        randomInitLvl,
                                        None,
                                        -1,
                                        None,
                                        city
                                        ))


      nsimulations +=1

      jobs.append(p)
      p.start()

      return

      if(len(jobs)>120):
          time.sleep(.1) #only to print after other prints
          print("\nWaiting for %d simulations"%len(jobs))
          with click.progressbar(jobs, length=len(jobs)) as bar:
              for proc in bar:
                  proc.join()
          jobs.clear()


    time.sleep(.1) #only to print after other prints
    print("\nWaiting for %d simulations"%len(jobs))
    with click.progressbar(jobs, length=len(jobs)) as bar:
        for proc in bar:
            proc.join()

    b = datetime.datetime.now()
    c = (b - aglobal).total_seconds()


    if(check_and_run_missing(lastS,Stamps_Events,DistancesFrom_Zone_Ordered, ZoneCars, city)<0):
        exit(-1)


    print("Run %d Simulations took %d seconds"%(nsimulations,c))

    print("\nStart Spark Analysis")
    a = datetime.datetime.now()

    os.system('scp ../Analysis/Spark_Analyzer.py bigdatadb:/tmp/CarSharing_Spark_Analyzer.py')
    os.system('ssh bigdatadb export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/')
    os.system('ssh bigdatadb spark2-submit --master yarn --deploy-mode client /tmp/CarSharing_Spark_Analyzer.py %d'%lastS)
    os.system('scp bigdatadb:/data/03/Carsharing_data/output_analysis/Simulation_%d/out_analysis.txt ../output_analysis/Simulation_%d/'%(lastS,lastS))

    os.system('scp bigdatadb:/tmp/Carsharing_Output/out_analysis.txt ../output_analysis/Simulation_%d/'%lastS)
    os.system('cat ../output_analysis/Simulation_%d/out_analysis.txt | ssh bigdatadb hdfs dfs -put -f - Simulator/output/Simulation_%s/out_analysis.txt' %(lastS,lastS))



    b = datetime.datetime.now()
    c = (b - a).total_seconds()
    #
    # print("Analyze data with Spark took %d seconds" %(c))
    # print("\nPlot graphs")
    # a = datetime.datetime.now()
    # os.system('python3 ../Analysis/plot_heatmap.py %d'%lastS)
    # b = datetime.datetime.now()
    # c = (b - a).total_seconds()
    # print("Plot graphs took %d seconds" %(c))
    #
    print("Ouput in output/Simulation_%d"%lastS)
    print("Ouput in output_analysis/Simulation_%d"%lastS)

    return 
        
main()

