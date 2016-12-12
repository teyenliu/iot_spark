'''
Created on Nov 25, 2016

@author: liudanny
'''


from thread import start_new_thread
import urllib2
import time
from datetime import datetime
import sys
import json
import random


"""
From the number of devices it composes device names. Each JSON object has the 
following format:
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
"""


#
# get random letters
#
def get_random_word():
    word = ''
    for i in range(10):
        word += random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')
    return word


#
# get an IP
#
def get_ip_addr():
    n1 = random.randrange(60, 209)
    n2 = random.randrange(1, 127)
    n3 = random.randrange(1, 127)
    n4 = random.randrange(1, 127)

    ip = str(n1) + "." + str(n2) + "." + str (n3) + "." + str(n4)
    return ip


class IoT_Device(object):
    """
    """
    
    def __init__(self, dev_id=0, sensor_number=1):
        """ Return a iot_device object 
        """
        self.dev_id = dev_id
        self.sensor_number = sensor_number
        self.ip_addr = get_ip_addr()

    #
    # create a json object with attributes and values
    #
    def create_json(self, sensor_id):
        temp = random.randrange(10, 35)
        (x, y) = random.randrange(0, 100), random.randrange(0, 100)
        ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        zipcode = random.randrange(94538, 97107)
        humidity = random.randrange(25, 100)
        if sensor_id % 2 == 0:
            sensor_name = "sensor-pad-" + str(sensor_id) + "_" + get_random_word()
        elif sensor_id % 3 == 0:
            sensor_name = "device-mac-" + str(sensor_id) + "_" + get_random_word()
        elif sensor_id % 5 == 0:
            sensor_name = "therm-stick-" + str(sensor_id) + "_" + get_random_word()
        else:
            sensor_name = "meter-gauge-" + str(sensor_id) + "_" + get_random_word()

        return json.dumps({"device_id": self.dev_id, "sensor_id": sensor_id, 
                           "sensor_name": sensor_name, "ip": self.ip_addr, 
                           "timestamp": ts, "temp": temp, "scale": "Celius", 
                           "lat": x, "long": y, "zipcode": zipcode, 
                           "humidity": humidity}, sort_keys=True)

    #
    # create a list of devices 
    #
    def create_sensor_data(self):
        sensor_data = []
        for sensor_id in range(1, self.sensor_number):
            time.sleep(0.1)
            device_msg = self.create_json(sensor_id)
            sensor_data.append(device_msg)
        return sensor_data


