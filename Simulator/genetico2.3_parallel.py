#GIORDANO, VASSIO 
#OTTOBRE 2018

#### PARAMETRI DEL SIMULATORE
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

import cProfile
city = 'Torino'
gv.init()
sf.assingVariables(city)

walkingTreshold = 1000000
city = "Torino"
zones = sf.numberOfZones(city)
algorithm = "max-parking"
numberOfStations = 18
tankThreshold = 25
AvaiableChargingStations = 4
BestEffort = False
pThreshold = 1
randomInitLvl = False
return_dict = {}

ZoneCars = pickle.load( open( "../input/"+ city + "_" + gv.provider +"_ZoneCars.p", "rb" ) )
DistancesFrom_Zone_Ordered = pickle.load( open( "../input/"+ city + "_" + gv.provider + "_ZoneDistances.p", "rb" ) )
Stamps_Events = pickle.load( open( "../events/"+ city + "_" + gv.provider + "_sorted_dict_events_obj.pkl", "rb" ) )

#RechargingStation_Zones = sf.loadRecharing(algorithm, numberOfStations, city)
#print(RechargingStation_Zones)

####


import random
import math

historic_solutions={}

#### PARAMETRI IMPORTANTI
#valid_zones = [i for i in range(1,261)]#[''''vector with valid zones in city''']

with open("../input/"+ city + "_" +gv.provider+"_ValidZones.csv") as f:
    valid_zones_str = [row.split(",")[0] for row in f]
#print(valid_zones)
valid_zones = [int(valid_zones_str[i]) for i in range(1,len(valid_zones_str))]
NunZones = numberOfStations #18 #total number of zones 
NumInitSolutions = 2 #popolazione iniziale e a ogni generazione
NumberofJoint = int(NumInitSolutions/2) # numero di figli per generazione
Mutation_prob=0.02 #probability between 0 and 1
Mutation_prob_strong=0.2 #probability when poor genetic population
Thresh_not_improving=30 #after this number of generation, the simulation stops
pure_elitist=1 # if set to 1 it converges faster (exploitation), if set to 0 it explores more (exploration)
#### 

not_improving=0
counterSolutions=0
AllTimeBest=[]


def check_valid(real_child):
    
    #guarda se valori non sono duplicati
    if(len(real_child) != len(set(real_child))): return False
    
    #controllare che le zone siano fattiblii
    for zonei in real_child:
        if(zonei not in valid_zones): return False
    
    return True

def make_mutation(probability):
    mutate = random.random()    
    #probability of mutation (may be not constant)
    if(mutate<probability): return True
    return False

def extract_valid_zone():    
    
    return random.sample(valid_zones, 1)[0]

# POSSIBILITA 1 per MUTARE (OBSOLETA)
def mutate(real_child):
    '''
    Scelgo un indice che è il gene da mutare
    '''
    mutation_gene = random.randint(0,NunZones-1)
    tmp_child = real_child.copy()
    while True:
        '''
        estraggo una zona tra quelle valide
        se zona NON è gia nel figlio, la cambio e la torno
        '''
        new_zone = extract_valid_zone()
        if(new_zone not in real_child):
            tmp_child[mutation_gene] = new_zone
            tmp_child.sort()
            if(str(tmp_child) not in historic_solutions): 
                real_child[mutation_gene] = new_zone
                real_child.sort()
                return
    
    return 


# POSSIBILITA 2 per MUTARE
def mutate2(real_child,probability):
    #modifico con una certa probabilita ogni gene
    tmp_child = real_child.copy()
    for genei in range(0,len(tmp_child)):
        if(make_mutation(probability)):
            while True:
                '''
                estraggo una zona tra quelle valide
                se zona NON è gia nel figlio, la cambio e la torno
                '''
                new_zone = extract_valid_zone()
                if(new_zone not in real_child):
                    tmp_child[genei] = new_zone
                    break
    tmp_child.sort()
    if(str(tmp_child) not in historic_solutions): 
        return tmp_child
    else:
        return ""



def generate_child(Parents, parents_available):
    
    Attempts = 0
    while True:
        '''
        Scelgo due genitori a caso, e genero un figlio
        campionando un po' di figli dalla madre e dal padre
        '''
        dadi=parents_available.pop(random.randrange(len(parents_available)))
        mumi=parents_available.pop(random.randrange(len(parents_available)))

        dad = Parents[dadi][2]
        mum = Parents[mumi][2]
            
        child = set.union(set(dad),set(mum))
        real_child = random.sample(child, NunZones)
        print(real_child)
        real_child.sort()     

        #MUTAZIONE 2
        res="" 
        while res=="": 
            #se la popolazione globale ha pochi geni, aumento di molto la probabilita
            if geni_tot<=2*float(NunZones)/float(len(valid_zones)): #forzo il successo        
                res = mutate2(real_child,Mutation_prob_strong)  
                #print("STRONG MUTATION ", str(float(NunZones)/float(len(valid_zones))))
            else:
                res = mutate2(real_child,Mutation_prob)
        real_child=res

        if(str(real_child) not in historic_solutions
           and check_valid(real_child)==True): 
            return real_child, parents_available

        Attempts +=1 #se non riesco a trovare figli nuovi per tanto tempo (probabilmente perche ho un patrimonio genetico basso)
        if(Attempts>NumberofJoint*5):# and geni_tot<=2*float(NunZones)/float(len(valid_zones))): #forzo il successo
            #print("ATTEMPT MAXIMUM")
            res="" 
            while res=="": 
                res = mutate2(real_child,Mutation_prob_strong) 
                real_child=res
                return real_child, parents_available
        else:   
            #no success, rimetto i genitori a posto    
            parents_available.append(dadi)
            parents_available.append(mumi)
    

def merge_two_dicts(Parents, Childs):
    '''
    Aggiungo ad un ai padri tutte le soluzioni dei figli
    '''
    Solutions = Parents.copy()   
    Solutions.update(Childs)    
    return Solutions

def NewGeneration(Parents, Children):
    
    # elitistica: sopravvivono solo i migliori
    # normale: sopravvivono solo i figli
    
    #NumberofJoint  numero di figli per generazione, ma la popolazione deve essere di NumInitSolutions

    #keep all sons and best of fathers -> una elistica parziale
    
    Solutions = merge_two_dicts(Parents, Children)
    '''
    ordino per D e WWD
    '''
    Sorted_Solutions = sorted(Solutions.items(), key=lambda kv: kv[1])

    NewSolutions = {}
    
    if pure_elitist:
        '''
        Mantengo le TOP-N sol
        '''
        geni_set=set()
        for i in range(0,NumInitSolutions):
            sol = Sorted_Solutions[i]
            NewSolutions[sol[0]]=sol[1]
            geni_set=set.union(geni_set,set(NewSolutions[sol[0]][2]))


    else:
        '''
        Mantengo le TOP-N sol dei padri (meta)
        '''
        geni_set=set()
        
        Sorted_Parents = sorted(Parents.items(), key=lambda kv: kv[1])
        saved_fathers=NumberofJoint
        if Children==[]:# the first generation
            saved_fathers=NumInitSolutions
        
        for i in range(0,saved_fathers):
            sol = Sorted_Parents[i]
            NewSolutions[sol[0]]=sol[1]
            geni_set=set.union(geni_set,set(NewSolutions[sol[0]][2]))
         
        '''
        Mantengo tutti i figli
        
        '''   
        if Children!=[]:#not the first generation
            Sorted_Children = sorted(Children.items(), key=lambda kv: kv[1]) #inultile perche per ora tengo tt i figli
            for i in range(0,NumberofJoint):
                sol = Sorted_Children[i]
                NewSolutions[sol[0]]=sol[1]
                geni_set=set.union(geni_set,set(NewSolutions[sol[0]][2]))
         

    
    global counterSolutions
    global AllTimeBest
    global not_improving
    global geni_tot

    counterSolutions+=1

    if AllTimeBest!=Sorted_Solutions[0][1]:
        AllTimeBest=Sorted_Solutions[0][1]
        print("New best solution found: ", AllTimeBest)
        not_improving=0
    else:
        not_improving+=1
    
    geni_tot=float(len(geni_set)/float(len(valid_zones)));
    print("Generazione: ", counterSolutions, "Geni disponibili in popolazione: ", str('{0:.2f}'.format(geni_tot)), " Not improving for #gen: ", not_improving) 
    
    return NewSolutions
# 
# def RunSim(solution):
#     #esempio dummy
#     #funzione che calcola la somma del numero di zone, 
#     #cosi scelgo zone con id elevato,e poi ritorno come secondo valore un numero randomico
#     #print(solution)
#     return [sum(solution), max(solution)]

def InitParents():

    '''
    Creo una nuova soluzione, campianonando a casaccio le zone disponibili
    NunZones -> dim della solziuone
    Salvo la soluzione finche non ho NumInitSolution soluzioni
    '''
    contatore=0
    Solutions = {}
    Solutions_param={}
    while(len(Solutions) < NumInitSolutions):
        
        new_solution = random.sample(valid_zones, NunZones)  
        new_solution.sort()
        
        if(str(new_solution) not in Solutions and check_valid(new_solution) ): 
            Solutions[str(new_solution)] = [-1,-1]
            Solutions_param[str(new_solution)]=new_solution
    
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    jobs=[]
    '''
    Per questo primo set di soluzione, runno le simulazioni ed salvo i parametri
    '''
    print("simulazioni parallele: ", len(Solutions))
    for solution in Solutions:
        #deaths,wwd=RunSim(Solutions_param[solution])
        contatore+=1
        p = Process(target=RunSim,args = (BestEffort,
                          algorithm.replace("_","-"),
                          algorithm,
                          AvaiableChargingStations,
                          tankThreshold,
                          walkingTreshold,
                          ZoneCars,
                          Solutions_param[solution],
                          Stamps_Events,
                          DistancesFrom_Zone_Ordered,
                          46,
                          pThreshold,
                          2,
                          randomInitLvl,
                          return_dict,
                          contatore,
                          1,
                          city))

        jobs.append(p)
        p.start()

    print("w8ing %d"%len(jobs))
    for proc in jobs:
        proc.join()
        
    print("keys: ", return_dict.keys())
    for val in return_dict.values(): 
                  
        childi = val["Config"]
        deaths = val["PercDeath"]
        wwd = val['WeightedWalkedDistance']
        print(deaths,wwd,childi)
        Solutions[str(childi)] = [deaths,wwd,childi]
        historic_solutions[str(childi)] = [deaths,wwd]
        #Solutions[solution] = [deaths,wwd,Solutions_param[solution]]
        #historic_solutions[solution] = [deaths,wwd]
        
    #    p = Process(target=RunSim(sn))
    #    k+=1
    #    jobs.append(p)
    #    p.start()        

    #for proc in jobs:
    #    proc.join()                        
    


    
    return Solutions


def main():
    
    Parents = InitParents()
    Parents = NewGeneration(Parents,[])
    global not_improving
    while True and not_improving<=Thresh_not_improving:
        Childs = {}
        parents_available = list(Parents.keys())
        #return_dict = manager.dict()
        for i in range(0,NumberofJoint):
            '''
            Genero un determinanto numero di figli
            e su ognuno compio una misuro le metriche
            '''
            childi, parents_available =  generate_child(Parents, parents_available)
            Childs[str(childi)] = ["","",childi]
            historic_solutions[str(childi)] = ["",""]


            
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        jobs=[]
        contatore=0
        print("simulazioni parallele: ", len(Childs))
        for solution in Childs:
            #deaths,wwd=RunSim(Solutions_param[solution])
            contatore+=1;
            p = Process(target=RunSim,args = (BestEffort,
                              algorithm.replace("_","-"),
                              algorithm,
                              AvaiableChargingStations,
                              tankThreshold,
                              walkingTreshold,
                              ZoneCars,
                              Childs[solution][2],
                              Stamps_Events,
                              DistancesFrom_Zone_Ordered,
                              46,
                              pThreshold,
                              2,
                              randomInitLvl,
                              return_dict,
                              contatore,
                              1,
                              city))
    
            jobs.append(p)
            p.start()
    
        print("w8ing %d"%len(jobs))
        for proc in jobs:
            proc.join()
            
        for val in return_dict.values():           
            childi = val["Config"]
            deaths = val["PercDeath"]
            wwd = val['WeightedWalkedDistance']
            print(deaths,wwd,childi)    
            Childs[str(childi)] = [deaths,wwd,childi]
            historic_solutions[str(childi)] = [deaths,wwd]                 

        

        
        '''
        Parents è composto dalle top-n soluzioni dell passo precedente
        '''
        Parents = NewGeneration(Parents, Childs)
        Childs.clear()
            
            
            
main()
