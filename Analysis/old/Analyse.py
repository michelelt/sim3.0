import numpy as np
import pandas as pd
import pickle
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv
gv.init()
sf.assingVariables()


label = {
            "reroutePerc":"Destination re-routing [%]",
            "avgWalkedDistance": "Users' average walk. dist.[m]",
            "medianWalkedDistance": "Users median walk. dist.[m]",
            "medianSOC": "median SOC [%]",
            "meanSOC": "Average SOC [%]",
            "amountRechargePerc": "Recharges [%]",
            "deaths" : "Battery run outs"
            }



def compose_path(policy,provider, acs, alg, z, tt):
    return p + "/output/"+policy+"_"+provider+"_"+ str(alg)+"_"+str(acs)+"_"+str(z)+"_"+str(tt)+".txt"



header = ["provider","policy","algorithm","tankThreshold","zones","acs","walkingThreshold","typeS", "typeE",
          "avgWalkedDistance","medianWalkedDistance", "avgWalkedDistanceGlobal", "medianWalkedDistanceGlobal",
          "avgSOC", "medianSOC", "amountRecharge","amountRechargeForced","amountRechargeForcedFail",
          "amountRechargeBestEffort", "amountRechargePerc", "avgTimeInStation", "medianTimeInStation",
          "deaths","reroute","reroutePerc","reroutePercofRecharge"]


def dict_to_str(s):
    
    OutStr = ""
    for val in header:
        if(type(s[val]) is int):
            OutStr +="%d "%s[val]
        elif(type(s[val]) is str):
            OutStr +="%s "%s[val]
        else:
            OutStr +="%.2f "%s[val]

    OutStr =OutStr[:-1]+"\n"
    
    
    return OutStr


def main():
    
    
    zones = [i for i in range(10,121,5)]
    policy = "Forced"
    tt = [i for i in range(10,61,5)]
    
    fout = open("../processed.txt","w")
    
    
    HederStr = ""
    for val in header:
        HederStr+=val+" "
    
    HederStr =HederStr[:-1]+"\n"
    
    fout.write(HederStr)
    s={}
    processed=0
    for acs in [2,4,6]:
        for algorithm in ["max_time", "max_parking", "rnd"]:
            jobs=[]
            for z in zones:
                for t in tt:
                    print(processed)
                    processed+=1
                    path = compose_path(policy,gv.provider, acs, algorithm, z, t)
                    try:
                        f = open(path)
                        '''d = {}
                        for j in range(6):
                            line=f.readline()
                            line = line.split(":")
                            d[line[0]] = line[1]
                        settings.append(d)
                        #print ("*", d)
                        
    
                        if len(d)>0:'''
                        df = pd.read_csv(path, skiprows=[0,1,2,3,4,5,6], header=0, sep=";")
                        s["policy"] = policy
                        s["typeE"] = len(df[df["Type"]=='e'])
                        s["typeS"] = len(df[df["Type"]=='s'])                            
                        s["provider"] = gv.provider
                        s["tankThreshold"] = t
                        s["zones"] = z
                        s["algorithm"] = algorithm
                        s["acs"] = acs
                        s["avgWalkedDistance"] = df[
                                (df["Type"]=='e') & 
                                (df["Distance"]>0)].Distance.mean()
                        s["medianWalkedDistance"] = df[
                                (df["Type"]=='e')& 
                                (df["Distance"]>0)].Distance.median()   
                                                             
                        s["avgWalkedDistanceGlobal"] = df[
                                (df["Type"]=='e')].Distance.mean()                                
                        s["medianWalkedDistanceGlobal"] = df[
                                (df["Type"]=='e')].Distance.median()                                
                                
                        s["avgSOC"] = df[(df["Type"]=='e')].Lvl.mean()
                        s["medianSOC"] = df[(df["Type"]=='e')].Lvl.median()
                        s["amountRecharge"] = len(df[df["Recharged"]==True])
                        s["amountRechargeForced"] = len(df[
                                                  (df["Recharged"]==True) & 
                                                  (df["ToRecharge"]==True)])
                        s["amountRechargeForcedFail"] = len(df[
                                                  (df["Recharged"]==False) & 
                                                  (df["ToRecharge"]==True)])
                        s["amountRechargeBestEffort"] = len(df[
                                                  (df["Recharged"]==True) & 
                                                  (df["ToRecharge"]==False)])
                        s["amountRechargePerc"] = s["amountRecharge"]*100 / s["typeE"]

                        TmpRes                    = df[
                                                  (df["Type"]=='s') & 
                                                  (df["StartRecharge"]!=0)]                        
                        s["avgTimeInStation"]  = (TmpRes['Stamp'] - TmpRes['StartRecharge']).mean()
                        s["medianTimeInStation"]  = (TmpRes['Stamp'] - TmpRes['StartRecharge']).median()
                        
                        s["deaths"] = len(df[df["Lvl"]< 0])                                                
                        s["reroute"] = len(df[
                                (df["Type"]=='e') & 
                                (df["Distance"]>0)])
                        s["reroutePerc"] = s["reroute"]*100/s["typeE"]
                        s["reroutePercofRecharge"] = s["reroute"]*100/s["amountRecharge"]
                        
                        '''OutStr = ""
                        for val in header[0:-1]:
                            if(type(s[val]) is int):
                                OutStr +="%d "%s[val]
                            elif(type(s[val]) is str):
                                OutStr +="%s "%s[val]
                            else:
                                OutStr +="%.2f "%s[val]
                        OutStr+="%.2f\n"%s[header[-1]]'''
                        
                        fout.write(dict_to_str(s))
                            
                    # except e:
                    except :
                        # print(e)
                        print(path)
                        exit(0)
                        pass
    
    return
                
main()
