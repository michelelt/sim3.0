'''
Created on 13/nov/2017

@author: dgiordan
'''
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

class PlatesData:
    def __init__(self, timestamp, coordinates):
        self.timestamp = timestamp
        self.coordinates = coordinates

    def __str__(self):
        return "TS: " + self.timestamp + " - CD: " + self.coordinates
