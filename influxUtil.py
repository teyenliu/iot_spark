'''
Created on Nov 23, 2016

@author: liudanny
'''

from influxdb import InfluxDBClient
import json


user = 'danny'
password = 'danny'
dbname = 'iot_devices'

{  
   "device_id":2,
   "humidity":34,
   "ip":"174.30.100.113",
   "lat":27,
   "long":14,
   "scale":"Celius",
   "sensor_id":9,
   "sensor_name":"device-mac-9_yVHw4IFeDu",
   "temp":12,
   "timestamp":"2016-11-25T03:45:26Z",
   "zipcode":96614
}
#
# Integrate influx db insertion here as timeseries. Create a measurement point with tags and fields
#
def create_influxdb_point(jdoc, measurement):

    #use this incoming arg as measurement (or synonmous to table in DB)
    data={}
    data['measurement'] = measurement
    #
    # Define key tags in as part of this measurment table. Keys are going to be indexed so queries as
    # faster for its values
    
    tags = {}
    tags['device_id'] = str(jdoc['device_id'])
    tags['sensor_name'] = str(jdoc['sensor_name'])
    tags['sensor_id'] = str(jdoc['sensor_id'])
    tags['zipcode'] = str(jdoc['zipcode'])

    data['tags'] = tags
    data['time'] = jdoc['timestamp']
    #
    # define fields, which are not indexed
    #
    fields={}
    fields['humidity']= jdoc['humidity']
    fields['temp'] = jdoc['temp']
    fields['lat'] = jdoc['lat']
    fields['long'] = jdoc['long']
    fields['scale'] = jdoc['scale']

    data['fields'] = fields

    return [data]


def insert_into_dbs(dbs, jdoc):
    ##TODO Convert this into a Singleton class so that we don't call this each time a message is received.
    client = InfluxDBClient("localhost", 8086, user, password, dbname)
    print ("Recieved JSON for insertion in DB: %s %s" % (dbs, json.dumps(jdoc, sort_keys="True")))
    influxDoc = create_influxdb_point(jdoc, "R300_sensors_data")
    print (influxDoc)
    client.write_points(influxDoc)

def on_error(message):
    print ("ERROR: " + str(message))