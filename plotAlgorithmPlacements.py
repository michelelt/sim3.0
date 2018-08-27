#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import geopandas as gpd
import numpy as np
import datetime
import time
import random
import sys
import os.path
sys.path.insert(0, '/home/mc/Scrivania/Tesi/MyTool/Analysis/')
sys.path.insert(0, '/home/mc/Scrivania/Tesi/MyTool/Analysis/simulator')
from shapely.geometry import Point, Polygon
import threading
from multiprocessing import Process
import matplotlib.pyplot as plt
from matplotlib import colors



def bar_plot_parkings_stats (df1, provider, column):
    if provider == "car2go" :
        color = "blue"
    else :
        color = "red"
        
    if column == "avg_duration_per_zone":
        y_label = 'Avg. parking time per zone [h]'
        title = provider + ' - zones sorted per maximum avg. parking time'
    elif column == "parking_per_zone" :
        y_label = 'Parkings per zone'
        title = provider + ' - zones sorted per number of parkings'
    elif column == "duration_per_zone" :
        y_label = "Tot. park. durations per zone [h]"
        title = provider + ' - zones sorted per total duration'
    else :
        print ("No column")
        return

    df1 = df1.sort_values(column, ascending=False)
    width=0.5
    ind = np.arange(len(df1.index))
    fig, ax = plt.subplots(figsize=(20,10))
    ax.bar(ind, df1[column], width, color=color)
    ax.grid()
    
    ax.set_ylabel(y_label, fontsize=36)
    ax.set_xlabel("Zone ID", fontsize=36)
    ax.set_title(title, fontsize=36)
    ticks = [""]*len(df1.index)
    ticks[0:len(df1.index):16] = df1.index[range(0,len(ind),16)]
            
    
    ax.set_xticks(ind + width /32)
    ax.set_xticklabels(ticks)
    
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(27) 
                
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(27)    
        
    my_path = "/home/mc/Scrivania/Tesi/toptesi/figures/_results/"+ provider+"_parkings_stats_"+column
    plt.savefig(my_path, bbox_inches = 'tight')

    plt.show()

def return_path(cso, alg, ppz, z):
        string = str(cso) +"_"+ str(alg) + "_" + str(ppz) + "_"+ str(z)
        return string

def plot_clorophlet_colorbar_solutions (my_city, provider, column, z,ppz):
    
    if provider == "car2go":
        color = "blue"
    else:
        color = "red"
    gdf = gpd.GeoDataFrame()
    gdf["taken"] = 0

    return
    
    
    fig, ax = plt.subplots(1, 1, figsize=(10,10))
    
    cmap = colors.ListedColormap(['grey', color])
    gdf.plot(column="taken", cmap=cmap, ax=ax, linewidth=1)
    if column == "max_avg_time":
        algorithm = 'Average parking time'
        
    elif column == "max_parking" :
        algorithm = 'Number of parkingings'
        
    elif column == "max_time" :
        algorithm = "Whole parkings durations"
        
    elif column == "rnd":
        algorithm = "Random positioning"
        
    else :
        print ("No column")
        return
    title = provider + "\nSolution for " + algorithm
#    title += "Zones: "+ str(z) + "; Power Supplies per zones: " + str(ppz)
    plt.title(title, fontsize = 36)
    ax.grid(linestyle='-', linewidth=1.0)
    plt.xticks([])
    plt.yticks([])
#    plt.xlabel("Latitude", fontproperties=font)
#    plt.ylabel("Longitude", fontproperties=font)

#    cax = fig.add_axes([0.9, 0.1, 0.03, 0.8,])
    sm_ = plt.cm.ScalarMappable(cmap=cmap )
    sm_._A = []
    cbar = fig.colorbar(sm_, orientation='horizontal')
#    cbar = plt.colorbar(sm_, cax=cax,  orientation='horizontal')
    cbar.ax.tick_params(labelsize=27,)
    cbar.set_ticks([0.25,0.75])
    cbar.set_ticklabels(["Not Charging station", "Charging station"])

#    cbar.set_label('Mean car parking time per hour', rotation=270, fontsize=18, labelpad=30)
#    gdf.apply(lambda x: ax.annotate(s=x.N, xy=(x.geometry.centroid.x, x.geometry.centroid.y), ha='center'),axis=1)
    
#    fig.savefig(paths.plots_path8+provider+"_"+column, bbox_inches='tight',dpi=250)
    plt.show()
    my_path="/home/mc/Immagini/pres_im/alg_sol_"+column

#    plt.savefig(my_path, bbox_inches = 'tight')
    
    return

#path = "/Users/mc/Desktop/sim3.0/input/"
#mt_name = "car2go_max-time500.csv"
#mp_name = "car2go_max-parking500.csv"
#at_name = "car2go_a"
#mp = pd.read_csv(path+mt_name)
#plot_clorophlet_colorbar_solutions(mp, "car2go", "max_time", 60, 2)
#plot_clorophlet_colorbar_solutions(torino, "car2go", "max_parking", 60, 2)
#plot_clorophlet_colorbar_solutions(torino, "car2go", "max_avg_time", 60, 2)