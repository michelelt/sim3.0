'''
Created on 13/nov/2017

@author: dgiordan
'''
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

import Simulator.Globals.GlobalVar as gv
from Simulator.Classes.Car import *

class Zone(object):
      
    def __init__(self, ID, AvaiableChargingStations):
        
        self.AvaiableChargingStations = AvaiableChargingStations
        self.ID = ID
        self.Cars = []
        self.RechargedCars = []        
        return



    def getBestGlobalCars(self,Stamp):
       
        BestCar = ""
        BestLvl = -1
        InRecharge = False

        for CarI in self.RechargedCars:
            if(BestCar == ""):
                BestCar = CarI    
                InRecharge = True
            else:
                CarILvl = CarI.getBatteryLvl(Stamp)
                if(CarILvl > BestLvl): 
                    BestCar = CarI
                    BestLvl = CarILvl
                    InRecharge = True

        for CarI in self.Cars:
            if(BestCar == ""):
                BestCar = CarI  
                InRecharge = False  
            else:
                CarILvl = CarI.getBatteryLvl()
                if(CarILvl > BestLvl): 
                    BestCar = CarI
                    BestLvl = CarILvl
                    InRecharge = False
                    
        if(BestCar != ""): 
            if(InRecharge): 
                del self.RechargedCars[self.RechargedCars.index(BestCar)]
            else: 
                del self.Cars[self.Cars.index(BestCar)]

        return BestCar

        
    def getBestRechargedCars(self,Stamp):
        
        if len(self.RechargedCars)==0: return ""
        
        BestCar = ""
        BestLvl = -1

        for CarI in self.RechargedCars:
            if(BestCar == ""):
                BestCar = CarI    
            else:
                CarILvl = CarI.getBatteryLvl(Stamp)
                if(CarILvl > BestLvl): 
                    BestCar = CarI
                    BestLvl = CarILvl
    
        if(BestCar != ""): del self.RechargedCars[self.RechargedCars.index(BestCar)]

        return BestCar
    
        
    def getBestCars(self):

        if len(self.Cars)==0: return ""
        
        BestCar = ""

        for CarI in self.Cars:
            if(BestCar == ""):
                BestCar = CarI    
            else:
                if(CarI.getBatteryLvl() > BestCar.getBatteryLvl()): BestCar = CarI  
        
        if(BestCar != ""): del self.Cars[self.Cars.index(BestCar)]
        return BestCar
    
    def getAnyParking(self,CarToPark):
        
        self.Cars.append(CarToPark)
        
        return
    
    def getParkingAtRechargingStations(self,CarToPark):
        
        if(len(self.RechargedCars) < self.AvaiableChargingStations):
            self.RechargedCars.append(CarToPark)
            return True
        
        return False

    def getNumRecCar(self):
        
        return len(self.RechargedCars)
        
    def getNumCar(self):
        
        return len(self.Cars)

    def setCars(self,cars):

        self.RechargedCars = []        
        
        CarVector = []
        for CarI in cars:
            CarVector.append(Car(gv.provider, CarI.ID))
        
        self.Cars = CarVector

        return

    def setAvaiableChargingStations(self, n):
        self.AvaiableChargingStations = n
        return

    def getAvaiableChargingStations(self):
        return self.AvaiableChargingStations