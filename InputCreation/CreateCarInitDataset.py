import sys
import os
p = os.path.abspath('..')
sys.path.append(p+"/")

import pickle
from geopy.geocoders import Nominatim
from Simulator.Classes.PlatesData import PlatesData 
import Simulator
import pandas as pd

import Simulator.Globals.SupportFunctions as sf
import Simulator.Globals.GlobalVar as gv

city = sys.argv[1]
gv.init()
sf.assingVariables(city)

dict_plates={} #dictionary keys (plate), inside is a list of objects first appearance

def main():


    collection="enjoy_PermanentParkings"
    if gv.provider == "car2go":
        collection = "PermanentParkings"
        
    collection_parkings = sf.setup_mongodb(collection)
    parkings = collection_parkings.find({"city": gv.city, 
                                         "init_time": {"$gt": gv.initDate, 
                                                       "$lt": gv.finalDate}
                                         })
    parkigns2 = parkings.clone()

    
    if gv.fleetSize.isnumeric():
        realFleetSize = int(gv.fleetSize)
    else :
        df = pd.DataFrame(list(parkigns2))
        df["DailyDate"] = df["init_date"].apply(lambda x : x.strftime("%Y/%m/%d"))
        carsPerDay = df.groupby('DailyDate')["plate"].nunique()
#        carsPerDay = pd.Series(11)
        if gv.fleetSize == "mean":
            realFleetSize = int(round(carsPerDay.mean()))
            
        elif gv.fleetSize == "max":
            realFleetSize = int(carsPerDay.max())

        elif gv.fleetSize == "min":
            realFleetSize = int(carsPerDay.min())

        else:
            print("CCID, ERROR wrong fleetSize Value: "+str(gv.fleetSize))
            return  -1
        
    parkigns2.close()
    print ("CCID, realFleetSize:",str(realFleetSize), "gv.fleetSize:", str(gv.fleetSize))
    
    currentFleetSize = 0
    for val in parkings:
            coords = val['loc']['coordinates']
            lon1 = coords[0]
            lat1 = coords[1]
            #d = haversine(baselon, baselat, lon1, lat1)
            # if( checkPerimeter(lat1, lon1) or
            #    (provider == "car2go" and checkCasellePerimeter(lat1, lon1)) and
            #     currentFleetSize <= FleetSize):
            if currentFleetSize <  realFleetSize:
                if sf.checkPerimeter(lat1, lon1):
                    if val['plate'] not in dict_plates:
                        dict_plates[val['plate']] = PlatesData(val['init_time'], val["loc"]['coordinates'])
                        currentFleetSize += 1

                    else:
                        if dict_plates[val['plate']].timestamp >= val['init_time']: #se non erano in ordine nel dataset iniziale
                            dict_plates[val['plate']] = PlatesData(val['init_time'], val["loc"]['coordinates'])
                else:
                    print("problem")
            else :
                print("CCID, len dict_plates:" + str(len(dict_plates)) + "FleetSize:" + str(realFleetSize))
                print("CCID, cfs", currentFleetSize)
                break
    

    print("CCID", "Seen cars:", len(dict_plates))
    print("cfs", currentFleetSize)
    
    with open("../input/"+ gv.city + "_" + gv.provider + "_plates_appeareance_obj.pkl", 'wb') as handle:
        pickle.dump(dict_plates, handle)

    print ("CCID, col:", gv.NColumns, " row:", gv.NRows)
    print ("CCID, End\n")
    return

main()
