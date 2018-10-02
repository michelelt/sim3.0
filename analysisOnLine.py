#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def parser(myString):
    myString= myString.split(".")
    myString = myString[0].split("_")
    
    d ={}
    d["Provider"] = myString[0]
    d["Policy"] = myString[1]
    d["Algorithm"] = myString[2]
    d["Zones"] = myString[3]
    d["Acs"] = myString[4]
    d["TankThreshold"] = myString[5]
    return d
    

path = "/Users/mc/"
fileName = "test.txt"
fileName2 = "test2.txt"


file = open(path+fileName,'r')
file2 = open(path+fileName2, 'w')

for line in file:
    if "1;2;3;4;5;6;7;8" in line:
        continue
    else :
        lineout = line.split()[8]
        file2.write(lineout+"\n")
        
file2.close()

dfInput = pd.read_csv(path+fileName2, header=None)
test = "car2go_FreeFloating_max-parking_10_2_-1_1000000.txt"


df=pd.DataFrame(columns = ['Provider', 'Policy', 'Algorithm', 'Zones', 'Acs', 'TankThreshold'])
for i in range(len(dfInput)):
    d = parser(dfInput.iloc[i][0])
    tmp = pd.Series(d)
    df = df.append(tmp, ignore_index=True)

BestEffort_list = ['FreeFloating', 'HybridForced', 'HybridNeeded', 'StationBased']
AvaiableChargingStations_list = ['2', '3', '4', '5', '6', '7', '8']
algorithm_list = ["rnd", "max-parking", "max-time"]
numberOfStations_list = [i for i in range(2,42,2)]
tankThresholds_list = [-1,5,10,25,50,100]

#missing = open(path+"missing.txt", "w")
#missing.write('Policy,Algorithm,Zones,Acs,TankThreshold\n')
#for BestEffort in BestEffort_list:
#    for AvaiableChargingStations in AvaiableChargingStations_list:
#        for algorithm in algorithm_list:
#            for numberOfStations in numberOfStations_list:
#                for tankThreshold in tankThresholds_list:
#                    tmp = df[df["Policy"] ==  BestEffort]
#                    tmp = tmp[tmp["Acs"] == AvaiableChargingStations]
#                    tmp = tmp[tmp["Algorithm"] == algorithm]
#                    tmp = tmp[tmp["TankThreshold"] == tankThreshold]
#                    
#                    if len(tmp) == 0:
#                        s = "%s,%s,%s,%s,%s\n" % (BestEffort, algorithm,numberOfStations,AvaiableChargingStations,tankThreshold)
#                        missing.write(s)
#                        
#missing.close()

"StationBased,max-time,34,8,10"

tmp = df[df["Policy"] == "StationBased"]
tmp = tmp[tmp["Algorithm"] == "max-time"]

                    
                    
            
    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    