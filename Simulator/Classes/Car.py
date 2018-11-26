'''
Created on 13/nov/2017

@author: dgiordan
'''
import sys
import os

from Simulator.Globals.SupportFunctions import haversine, checkCasellePerimeter, checkBerlinZone

import Simulator.Globals.GlobalVar as gv



class Car(object):
    
    def __init__(self, provider, ID):
        self.ID = ID
        self.BatteryMaxCapacity = 25.2
        self.kwh_km = 0.188
        if provider == 'car2go':
            self.BatteryMaxCapacity = 17.6
            self.kwh_km = 0.13

        self.BatteryCurrentCapacity = self.BatteryMaxCapacity 
        self.NumRentals = 0
        self.WasInRecharge = False
        self.StartRecharge = 0 #stamp
        self.StartBookingPosition = 0 #posizione
        self.FirstRental = 0
        self.kwh = 2
        self.cut_hour_perc = 0.7
        self.reduction_factor_kwh = 0.1
        self.gamma = 0


    def setGamma(self, gamma): self.gamma = gamma
    def getGamma(self): return self.gamma

    def setRechKwh(self, kwh): self.kwh = kwh
    def getRechKwh(self): return self.kwh

    def setInStation(self):
        
        self.WasInRecharge = True
        
        return

    def setStartPosition(self, BookingStarting_Position):

        self.StartBookingPosition = BookingStarting_Position

        return

    def setStartRecharge(self, StartRecharge):
        
        self.StartRecharge = StartRecharge
        
        return
    
    def EvalCurrentCapacity(self, CurrentStamp):
        # kw = gv.kwh_supplied
        kw = self.kwh

        
        starting_value = self.BatteryCurrentCapacity
        duration = (CurrentStamp-self.StartRecharge)/(60.0*60.0) #in hour
        delta_c = duration * kw 


        # '''recharge linear to 0-70 %, unlinear from 70 to 100'''
        # max_supply_h = self.BatteryMaxCapacity/self.kwh
        # if duration > max_supply_h
        #     a =  self.cut_hout_perc * max_supply_h * self.kwh
        #     b = (duration/max_supply_h - self.cut_hout_perc * max_supply_h/max_supply_h)  *  (self.reduction_factor_kwh * self.kwh)
        #     delta_c = a + b
        # else
        #     delta_c = duration * kw

        #str_out = str(self.BatteryCurrentCapacity)+ "_"+str(duration)+"_"+str(delta_c)+"_"+str(self.BatteryMaxCapacity)

        if (self.BatteryCurrentCapacity + delta_c <= self.BatteryMaxCapacity):
            return delta_c, self.BatteryCurrentCapacity + delta_c

        return self.BatteryMaxCapacity-starting_value, self.BatteryMaxCapacity
    
    def Recharge(self, EndRecharge):

        delta_c = -1 
        start_recharge = -1

        # distance = haversine(1.1,1.2,1.2,1.3)
        if self.WasInRecharge:
            delta_c, self.BatteryCurrentCapacity = self.EvalCurrentCapacity(EndRecharge)
            start_recharge = self.StartRecharge
            
        self.WasInRecharge = False
        self.StartRecharge = -1
        
        return delta_c, start_recharge

    def Discharge(self, BookingEndPosition):
        s = self.StartBookingPosition
        d = BookingEndPosition
        distance = haversine(s[0],s[1],d[0],d[1])
        
        if(checkCasellePerimeter(s[0],s[1]) or checkCasellePerimeter(d[0],d[1])):
            distance *=1 #For trips FROM or TO Caselle I do not use the corrective factor
        elif (checkBerlinZone(s[0],s[1])):
            distance *=1
        else: 
            distance *= gv.CorrectiveFactor 
        
        dist_km = distance/1000
        dc = dist_km * self.kwh_km * (1+self.gamma)

        self.BatteryCurrentCapacity = self.BatteryCurrentCapacity - dc
        if self.BatteryCurrentCapacity <=0 :
            self.BatteryCurrentCapacity = -0.001

        return dc, distance

    def getBatteryLvl(self, Stamp = False):
                
        if Stamp != False:
            #BCC = local battery current capacity
            #serviva per prendere l'auto con lo stato attuale di carica maggiore!
            delta_c, BCC = self.EvalCurrentCapacity(Stamp)            
            return BCC/self.BatteryMaxCapacity*100

        
        return self.BatteryCurrentCapacity/self.BatteryMaxCapacity*100
    
    def getID(self):
        
        return self.ID
    
    def IsFirstBooking(self):
        
        self.FirstRental+=1
        if(self.FirstRental==1): return True
        
        return False

    def getBatterCurrentCapacity(self):
        
        return self.BatteryCurrentCapacity

    def resetFields(self):

        self.BatteryCurrentCapacity = self.BatteryMaxCapacity 
        self.NumRentals = 0
        self.WasInRecharge = False
        self.StartRecharge = 0 #stamp
        self.StartBookingPosition = 0 #posizione
        self.FirstRental = 0

        #print("Reset")
        return
