# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
import pandas as pd
import json
import ast
import numpy as np

import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")
sys.path.append(p+"/Simulator/")
import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv
gv.init()
sf.assingVariables()

city = gv.city

#zones = [2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,19,21,23,25,27, 29, 31, 33, 35, 37, 39, 41]
outputPath = os.path.abspath("../output/")
files = os.listdir(outputPath)
zones =[]
for file in files:
    file = file.split(".")
    if len(file) >1 and file[1] == "txt":
        file = file[0].split("_")
        if (len(file) == 4 ):
            zones.append(int(file[3]))

zones = sorted(zones)
print (zones)

def loadOptmizationResults(zones):
    df = pd.DataFrame()
    for z in zones:
        series = pd.Series()
        series["zones"] = z

        print ("Zone: ", z)

        fname = "../output/best_solutions_%s_%d.txt"%(city, z)
        with open(fname) as f:
            content = f.readlines()

        for line in content:
            if line[0] == "*" or line[0] == "\n" or (line.split(" ")[0] == "NEW") :
                continue

            if "Nsteps" in line.split(" ")[0]:
                splitted = line.replace("\n","").replace(":","").split(" ")
                series[splitted[0]]= int(splitted[1])


            if "Old" in line.split(" ")[0]:
                splitted = line.replace("\n","").replace(":","").split(" ")
                series["oldDeaths"] = float(splitted[1])
                series["oldAvgValue"] = float(splitted[2])

            if "New" in line.split(" ")[0]:
                splitted = line.replace("\n","").replace(":","").replace("=", " ").replace("\n","").split(" ")
                series["newDeaths"] = float(splitted[2])
                series["newAvgValue"] = float(splitted[4])

            if "[" in line.split(" ")[0]:
                splitted = line.replace(",","").replace("\n","").replace("[","").replace("]","").split(" ")
                splitted = [int(i) for i in splitted]
                series["solutionIDList"] = splitted

            if "{" in line.split(" ")[0]:

                myDict = ast.literal_eval(line)
                for k in myDict.keys():
                    series[k] = myDict[k]
            df = df.append(series, ignore_index=True)
            f.close()
    df = df.dropna()
    return df

def addCaselle(inputList):
    caselleID = 689
    if caselleID not in inputList:
        newList = []
        newList = inputList[0:-1].copy()
        newList.append(caselleID)
        return str(newList)

    return str(inputList)


'''
TORINO
'''

to_not_load = loadOptmizationResults(zones)
df = to_not_load
df["zones"] = df["zones"].astype(int)
index_df = df.groupby("zones")["newAvgValue"].min()
index_df = index_df.reset_index()
index_df = index_df.set_index(['zones', 'newAvgValue'])
df = df.set_index(['zones', 'newAvgValue'])
df = df[df.index.isin(index_df.index)]
df["Nsteps"] = df.Nsteps.replace(0,np.nan)
df = df.dropna(how='any',axis=0)
df = df.iloc[0:len(df):3]
df["SolutionIDforcingCaselle"] = df.apply(lambda x: addCaselle(x["solutionIDList"]), axis=1)
#
df[["solutionIDList"]].to_csv("../../Torino_sim3.0/input/bestSolTorino.csv")
df[["SolutionIDforcingCaselle"]].to_csv("../../Torino_sim3.0/input/bestSolTorinoWithCaselle.csv")




