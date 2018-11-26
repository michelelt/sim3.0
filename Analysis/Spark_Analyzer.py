from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import numpy as np
import pandas as pd
import os
import sys
import collections
import pickle

header = ["Provider","Policy","Algorithm","Zones","Acs","TankThreshold","WalkingThreshold", "pThreshold", "TypeS", "TypeE",
          "kwh","gamma","AvgWalkedDistance","MedianWalkedDistance", "AvgWalkedDistanceGlobal", "MedianWalkedDistanceGlobal",
          "AvgSOC", "MedianSOC", "AmountRecharge","AmountRechargeForced","AmountRechargeForcedFail",
          "AmountRechargeBestEffort", "AmountRechargePerc", "AvgTimeInStation", "MedianTimeInStation",
          "Deaths","Reroute","ReroutePerc","ReroutePercofRecharge", "LazyUsersPerc", "AvgStationOccupancy"]






def dict_to_tpl_str(s):
    
    tuple = (
             s["Provider"],
             s["Policy"],
             s["Algorithm"],
             int(s["Zones"]),
             int(s["Acs"]),
             int(s["TankThreshold"]),
             int(s["WalkingThreshold"]),
             float(s["pThreshold"]),
             float(s["kwh"]),
             float(s["gamma"])
    )
    
    
    OutStr = ""
    for val in header:
        if(type(s[val]) is int):
            OutStr +="%d "%s[val]
        elif(type(s[val]) is str):
            OutStr +="%s "%s[val]
        else:
            OutStr +="%.2f "%s[val]

    OutStr =OutStr[:-1]+"\n"
    
    
    return tuple, OutStr

def mapf(s):

    sp = s.split(";")

    if(sp[0] != 's' and sp[0] != 'e'):
        return ("DELETE",[])    
    
    if(len(sp[14])<14): 
        print("ERRORE: %s"%s)
    
    key = str(sp[14])
    values=[]
    if(sp[0]=="s"):    
        values = [sp[0],"-","-",float(sp[4]),float(sp[5]),float(sp[7]),int(sp[8]),int(sp[9]),"-","-","-",float(sp[17]) ]
    else:
        values = [sp[0],sp[1],sp[2],float(sp[4]),float(sp[5]),"-","-",int(sp[9]),float(sp[12]), float(sp[13]), float(sp[15]), float(sp[17]) ]
    #Type ToRecharge Recharged ID Lvl Distance Iter Recharge StartRecharge Stamp EventCoords ZoneC Discharge  TripDistance  FileID extractedP
    #0     1            2       3 4     5       6     7        8            9        10        11    12        13            14       15
    #ZoneID OccupiedCS
    #  16       17

    return (key,values)


def mapf2(x):



    df = pd.DataFrame()
    
    key = x[0]
    keysplit=key.split("_")

    values = list(x[1])
    

    labels = ["Type", "ToRecharge", "Recharged", "Lvl", "Distance", "Recharge", "StartRecharge",
              "Stamp", "Discharge", "TripDistance", "extractedP", "OccupiedCS"]

    xv = [ [] for i in range(0,len(labels)) ]
    for val in values:
        for i in range(0,len(labels)):
            xv[i].append(val[i])

    for i in range(0,len(labels)):
        df[labels[i]] = xv[i]
    
    s = {}
    
    s["Provider"] = keysplit[0]
    s["Policy"] = keysplit[1]
    s["Algorithm"] = keysplit[2]
    s["Zones"] = keysplit[3]
    s["Acs"] = keysplit[4]
    s["TankThreshold"] = keysplit[5]
    s["WalkingThreshold"] = keysplit[6]
    s["pThreshold"] = keysplit[7]
    s["kwh"] = keysplit[8]
    s["gamma"] = keysplit[9]

    s["TypeS"] = len(df[df["Type"]=='s'])                            
    s["TypeE"] = len(df[df["Type"]=='e'])
    
    s["AvgWalkedDistance"] = df[
            (df["Type"]=='e') &
            (df["Distance"]>0)].Distance.mean()
    s["MedianWalkedDistance"] = df[
            (df["Type"]=='e')&
            (df["Distance"]>0)].Distance.median()

    s["AvgWalkedDistanceGlobal"] = df[
            (df["Type"]=='e')].Distance.mean()
    s["MedianWalkedDistanceGlobal"] = df[
            (df["Type"]=='e')].Distance.median()

    s["AvgSOC"] = df[(df["Type"]=='e')].Lvl.mean()
    s["MedianSOC"] = df[(df["Type"]=='e')].Lvl.median()
    s["AmountRecharge"] = len(df[
                               (df["Type"]=='e') &
                               (df["Recharged"]=='True')])
    s["AmountRechargeForced"] = len(df[
                              (df["Type"]=='e') &
                              (df["Recharged"]=='True') &
                              (df["ToRecharge"]=='True')])
    s["AmountRechargeForcedFail"] = len(df[
                              (df["Type"]=='e') &
                              (df["Recharged"]=='False') &
                              (df["ToRecharge"]=='True')])
    s["AmountRechargeBestEffort"] = len(df[
                              (df["Type"]=='e') &
                              (df["Recharged"]=='True') &
                              (df["ToRecharge"]=='False')])
    s["AmountRechargePerc"] = float(s["AmountRecharge"]*100) / (s["TypeE"])

    TmpRes                    = df[
                              (df["Type"]=='s') &
                              (df["StartRecharge"]>0)]


    s["AvgTimeInStation"]  = (TmpRes['Stamp'] - TmpRes['StartRecharge']).mean()

    s["MedianTimeInStation"]  = (TmpRes['Stamp'] - TmpRes['StartRecharge']).median()

    s["Deaths"] = len(df[ (df["Type"]=='e') &  (df["Lvl"]<0)])

    s["Reroute"] = len(df[ (df["Type"]=='e') &  (df["Distance"]>0)])
    s["ReroutePerc"] = float(s["Reroute"])*100/float(s["TypeE"])
    if(float(s["AmountRecharge"])==0):s["ReroutePercofRecharge"] = -1 #this should be an impossible condition
    else: s["ReroutePercofRecharge"] = float(s["Reroute"])*100/float(s["AmountRecharge"])

    s["LazyUsersPerc"] = len( df[
                              (df["Type"] == "e")
                              & (df["extractedP"] < keysplit[8])
                          ]
                        )/(s["TypeE"])

    # ReducedDf = df[df["Type"] == 'e']
    s["AvgStationOccupancy"] = (df[df["Type"]=='e'].OccupiedCS.mean()) / (int(s["Zones"])*int(s["Acs"]))
    return (key,s)


def main():

    lastS = sys.argv[1]


    output_directory =  "/data/03/Carsharing_data/output_analysis/Simulation_%s/"%lastS
    #os.system('rm -rf %s*'%output_directory)
    # establish the spark context


    conf = (SparkConf()
     .setAppName("Carsharing_BigDataAnalytics")
     .set("spark.dynamicAllocation.enabled", "false")
     .set("spark.task.maxFailures", 128)
     .set("spark.yarn.max.executor.failures", 128)
     .set("spark.executor.cores", "8")
     .set("spark.executor.memory", "8G")
     .set("spark.driver.memory", "32G")
     .set("spark.executor.instances", "50")
     .set("spark.network.timeout", "300")
     .set("spark.hadoop.validateOutputSpecs", "false")
    )

    sc = SparkContext(conf=conf)

    text_file = sc.textFile("hdfs:///user/cocca/Simulator/output/Simulation_%s/*"%lastS)
    counts = text_file.map(mapf). \
    filter(lambda x: x[0]!="DELETE"). \
    groupByKey().map(mapf2).collectAsMap()



    #pickle.dump(counts,open('data.pkl', 'wb'))

    HederStr = ""
    for val in header:
        HederStr+=val+" "

    HederStr =HederStr[:-1]+"\n"

    fout = open(output_directory+"out_analysis.txt","w")

    fout.write(HederStr)

    data = {}
    for val in counts:
        tuple, outs = dict_to_tpl_str(counts[val])
        data[tuple] = outs


    od = collections.OrderedDict(sorted(data.items()))
    for val in od:
        fout.write(od[val])

    fout.close()

    os.system('hdfs dfs -put -f /data/03/Carsharing_data/output_analysis/Simulation_%s/out_analysis.txt \
                Simulator/output/Simulation_%s/out_analysis.txt' %(lastS,lastS))


    return

main()
# print ("running spark analyzer")
