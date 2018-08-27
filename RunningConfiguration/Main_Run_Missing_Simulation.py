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
    
    lastS = 41
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
        
# main()

