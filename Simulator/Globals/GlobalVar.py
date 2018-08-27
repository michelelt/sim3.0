'''
Created on 13/nov/2017

@author: dgiordan
'''

import sys
import os
import pandas as pd
import time
import datetime

p = os.path.abspath("..")
sys.path.append(p+"/")


def init():

    global MaxLat, MaxLon, minLat, minLon, city, provider, initDate, finalDate, fleetSize, CorrectiveFactor
    global shiftLat500m, shiftLon500m, NColumns, NRows, MaxIndex, ShiftLon, ShiftLat, car2goKey, kwh_supplied
    global CaselleMaxLat, CaselleMaxLon, CaselleminLat, CaselleminLon, BAminLat, BAminLon, BAmaxLat, BAmaxLon, BerlinCriticalZone
    
    MaxLat = 0
    MaxLon = 0
    minLat = 0
    minLon = 0
    city = 0
    provider = 0
    initDate = 0
    finalDate = 0
    fleetSize = 0
    BerlinCriticalZone = [2054, 2055, 2056, 1995, 1996, 1997, 1998, 1999, 2000, 1938, 1939, 1940, 1941, 1942, 1881, 1882, 1883, 1884, 1885,
                          1824, 1825, 1826, 1827, 1828, 1829, 1830, 1768, 1769, 1770, 1771, 1711, 1712, 1654, 1655, 1656, 1597, 1598, 1599,
                          2297, 2012, 2013, 583, 228, 229, 1541, 171, 172, 486, 429, 439, 613, 1762, 1763, 622, 623, 624, 625, 565, 566,
                          567, 568, 508, 509, 510, 511, 512]

    CaselleCentralLat = 45.18843
    CaselleCentralLon = 7.6435



    CorrectiveFactor = 1.4

    shiftLat500m = 0.0045
    shiftLon500m = 0.00637
    car2goKey = ""

    '''
    add /2 in order to have a zonization 250x250
    '''
    shiftLat250m = shiftLat500m
    shiftLon250m = shiftLon500m



    NColumns = 1
    NRows = 1
    MaxIndex = NRows*NColumns-1

    ShiftLon = (MaxLon-minLon)/NColumns
    ShiftLat = (MaxLat-minLat)/NRows

    # '''
    # add /2 in order to have a zonization 250x250
    # '''
    CaselleMaxLat = CaselleCentralLat + shiftLat500m
    CaselleMaxLon = CaselleCentralLon + shiftLon500m
    CaselleminLat = CaselleCentralLat - shiftLat500m
    CaselleminLon = CaselleCentralLon - shiftLon500m


    # initDataSet = "###initDataSet###"
    kwh_supplied = 2.0

    return







