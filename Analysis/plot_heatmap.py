import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt
import numpy as np
import pprint
import pandas as pd
import os
import sys

 #df = pd.read_csv("../output_analysis/out_analysis.txt", sep=' ')
#df = df.T

#pp = pprint.PrettyPrinter(indent=4)

def main():


    '''lastS = -1
    output_dir = os.listdir("../output/")

    for val in output_dir:
         if("Simulation" in val):
             lastS +=1'''
    lastS = int(sys.argv[1])
    filePath ="../output_analysis/Simulation_"+str(lastS)+"/out_analysis.txt"

    skip = ["Provider", "Policy","Algorithm", "Zones", "Acs","TankThreshold", "WalkingThreshold", "TypeS","TypeE"]
    features_column= {}
    
    f = open(filePath,"r")

    providerColumn = -1
    policyColumn = -1 
    algoColumn= -1

    zColumn = -1    
    acsColumn = -1
    tColumn = -1

    

    providers = []    
    algos = []
    policies = []
    
    acses = []
    zones = []    
    tt = []    

    features = []

    ln = 0
    for line in f:
        ls = line.strip().split(" ")
        if(ln==0):
            for val in ls:    
                if(val not in skip):
                    features_column[val]=int(ls.index(val))
                    features.append(val)    
                else:
                    if(val == "Provider"):
                        providerColumn=int(ls.index(val))
                    if(val == "Policy"):
                        policyColumn=int(ls.index(val))
                    if(val == "Algorithm"):
                        algoColumn=int(ls.index(val))
                    elif(val == "TankThreshold"):
                        tColumn=int(ls.index(val))
                    elif(val == "Acs"):
                        acsColumn=int(ls.index(val))
                    elif(val == "Zones"):
                        zColumn=int(ls.index(val))
            ln = 1
        else:            
            if(ls[providerColumn] not in providers): providers.append(ls[providerColumn])
            if(ls[policyColumn] not in policies): policies.append(ls[policyColumn])
            if(ls[algoColumn] not in algos): algos.append(ls[algoColumn])
            
            if(int(ls[zColumn]) not in zones): zones.append(int(ls[zColumn]))
            if(int(ls[acsColumn]) not in acses): acses.append(int(ls[acsColumn]))
            if(int(ls[tColumn]) not in tt): tt.append(int(ls[tColumn]))
    f.close()
    
    # print(features_column.keys())
    # print(len(features_column))


    acses.sort()
    zones.sort()
    tt.sort()

    globalData = {}
    for provider in providers:
        globalData[provider] = {}
        for policy in policies:
                globalData[provider][policy] = {}
                for algo in algos:
                    globalData[provider][policy][algo]={}
                    for val in features_column:
                        globalData[provider][policy][algo][val] = [[0 for j in range(0,len(zones)*len(acses))] for k in range(0,len(tt))]

        
    fin = open(filePath, "r")

    ln=0
    for line in fin:
        ls = line.strip().split(" ")
        if(ln==0):
             ln=1
             continue

        provider = ls[providerColumn] 
        policy = ls[policyColumn]       
        algo = ls[algoColumn]
        
        t = int(float(ls[tColumn]))
        acs = int(float(ls[acsColumn]))
        z = int(float(ls[zColumn]))

        x = zones.index(z)*len(acses)+acses.index(acs)
        y = tt.index(t)

        for feature in features_column:
            globalData[provider][policy][algo][feature][y][x] = float(ls[features_column[feature]])
    
    
    xlabels = []
    for z in zones:
        #for acs in acses: 
        #xlabels.append(str(val)+"_2")
        #xlabels.append(str(val)+"_4")
        xlabels.append(str(z)+"_"+str(acses[-1]))
    
    
    for provider in providers:
        
        output_directory ="../output_analysis/Simulation_"+str(lastS)+"/fig/"+provider+"/"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)    
        #os.system('rm -rf %s/*'%output_directory)
        for policy in policies:
            print(policy)
            print("All")            
            data = globalData[provider][policy]
            for algo in data:
                for feature in data[algo]:
                    toplot= np.array(data[algo][feature])
                    fig, ax = plt.subplots()
                    heatmap = ax.pcolor(toplot)
                    ax.set_ylim(0,len(tt))
                    ax.set_xlim(0,len(zones)*len(acses))
                    ax.set_yticks([i+0.5 for i in range(0,len(tt))])
                    ax.set_yticklabels(tt)
                    ax.set_xticks([i+0.5 for i in range(0,len(xlabels)*len(acses),len(acses))])
                    ax.set_xticklabels(xlabels,rotation=60)
                    fulltitle = provider+" "+policy + " " +algo + " "+feature
                    ax.set_title(fulltitle)
                    ax.set_ylabel("TT")
                    ax.set_xlabel("Zones_Acs")
                    cbar = fig.colorbar(heatmap)
                    plt.tight_layout()
                    plt.savefig(output_directory+provider+"_"+policy+"_"+algo+"_"+feature+".png")
                    plt.close()
            
            
            print("Feature")            
            for feature in data[algo]:
                k=0
                fig, axes = plt.subplots(len(data), figsize = (10,20))
                for algo in data:
                    ax = axes
                    if(len(data) > 1):
                        ax = axes[k]

                    toplot= np.array(data[algo][feature])
                    heatmap = ax.pcolor(toplot)
                    ax.set_ylim(0,len(tt))
                    ax.set_xlim(0,len(zones)*len(acses))
                    ax.set_yticks([i+0.5 for i in range(0,len(tt))])
                    ax.set_yticklabels(tt)
                    ax.set_xticks([i+0.5 for i in range(0,len(xlabels)*len(acses),len(acses))])
                    ax.set_xticklabels(xlabels,rotation=60)
                    fulltitle = provider+" "+policy + " " +algo + " "+feature
                    ax.set_title(fulltitle)
                    ax.set_ylabel("TT")
                    ax.set_xlabel("Zones_Acs")
                    cb1 = fig.colorbar(heatmap, ax=ax)
                    #cb1.ax.tick_params(labelsize=20)
                    k+=1
                plt.tight_layout()
                plt.savefig(output_directory+provider+"_"+policy+"_"+feature+".png")
                plt.close()
            

            print("Algo")
            factor=1.25
            for algo in data:
                fig, axes = plt.subplots(5,4, figsize=(60, 40))
                k=0
                fig.suptitle(algo, fontsize=15*factor)
                for feature in features:
                    if(feature == "reroute"): continue
                    j=k%4
                    i=int(k/4)
                    ax = axes[i,j]
                    toplot= np.array(data[algo][feature])
                    heatmap = ax.pcolor(toplot)
                    ax.set_ylim(0,len(tt))
                    ax.set_xlim(0,len(zones)*len(acses))
                    ax.set_yticks([i+0.5 for i in range(0,len(tt))])
                    ax.set_yticklabels(tt,fontsize=5)
                    ax.set_xticks([i+0.5 for i in range(0,len(xlabels)*len(acses),len(acses))])
                    ax.set_xticklabels(xlabels,rotation=60,fontsize=5*factor)
                    fulltitle = feature
                    ax.set_title(fulltitle,fontsize=10*factor)
                    ax.set_ylabel("TT",fontsize=10*factor)
                    ax.set_xlabel("Zones_Acs",fontsize=10*factor)
                    cb1 = fig.colorbar(heatmap, ax=ax)
                    cb1.ax.tick_params(labelsize=7*factor)
                    k+=1
                plt.savefig(output_directory+provider+"_"+policy+"_"+algo+".png", bbox_inches='tight')

                plt.close()
            print ("End plotting\n")
            
        return

main()
