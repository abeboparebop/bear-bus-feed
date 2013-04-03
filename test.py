from pymongo import MongoClient
import time
import gtfs_realtime_pb2 as gtfs_rt
import sys

def getnew(collec,timestamp):
    # Gets the newest location data for each bus ("id"). Timestamp gives
    # a hint for the earliest time to consider. Collec is 
    # the MongoDB collection -- typically "projected_location" for us.
    # This function assumes quite a lot about the structure of the
    # MongoDB collection.
    mydict = {}
    for loc in collec.find({"entity.vehicle.timestamp": \
                                 {"$gt": timestamp}}).\
                                 sort("entity.vehicle.timestamp"):
        mydict[loc['entity']['id']] = loc['entity']['vehicle']
    return mydict

def JSON_to_protobufs(mydict):
    protoList = []
    for key in mydict:
        latitude = mydict[key]['position']['latitude']
        longitude = mydict[key]['position']['longitude']
        bearing = mydict[key]['position']['bearing']
        speed = mydict[key]['position']['bearing']
        timestamp = int(float(mydict[key]['timestamp'])/1000.0)
        timestr = time.strftime("%a, %d %b %Y %H:%M:%S +0000",\
                                    time.localtime(timestamp))

        new_ent = gtfs_rt.FeedEntity()

        new_ent.id = key
        
        new_vpos = new_ent.vehicle

        new_pos = new_vpos.position
        new_pos.latitude = latitude
        new_pos.longitude = longitude
        new_pos.bearing = bearing
        new_pos.speed = speed
                                
        new_vd = new_vpos.vehicle
        new_vd.id = key
                                
        new_vpos.timestamp = timestamp
        protoList.append(new_ent)
    return protoList
    

if __name__ == '__main__':
    input = sys.argv
    if (len(input) == 2):
        client_str = input[1]
        client = MongoClient(client_str)
        db = client['testberkeley']
        projloc = db['projected_location']
    

        mydict = getnew(projloc,1364867151000)
        ent_list = JSON_to_protobufs(mydict)

        new_msg = gtfs_rt.FeedMessage()

        new_head = new_msg.header
        new_head.gtfs_realtime_version = "1.0"
        new_head.timestamp = int(time.time())

        for ent in ent_list:
            new_msg.entity.add().CopyFrom(ent)

        print new_msg

    else:
        print "usage: python test.py <MongoDB URI>"
