from random import shuffle
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

'''
Created on 13/nov/2017

@author: dgiordan
'''

class Distance(object):
    
    def __init__(self, Distance):
      
        self.Zones = []
        self.Distance = Distance
      
        return  
    
    def getZones(self):
        shuffle(self.Zones)
        
        return self.Zones
    
    def getDistance(self):
    
        return self.Distance


    def appendZone(self,z):
        
        self.Zones.append(z)
        
        return
