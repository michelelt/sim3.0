'''
Created on 13/nov/2017

@author: dgiordan
'''
import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

class EventBook:
    def __init__(self, id_booking, type, coordinates):

        self.id_booking=id_booking
        self.type=type
        self.coordinates=coordinates
