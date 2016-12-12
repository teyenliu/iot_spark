'''
Created on Nov 23, 2016

@author: liudanny
'''


import sys
import socket
import time
import os
import argparse
import signal
from kafka import KafkaProducer
from kafka.errors import KafkaError
from thread import start_new_thread
from iot_devices import IoT_Device 


"""
# Asynchronous by default for testing
future = producer.send('iot-topic', b'testing...')
try:
    record_metadata = future.get(timeout=1)
except KafkaError as err:
# Decide what to do if produce request failed...
    print err
    pass
# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)
"""


def signal_handler(signal, frame):
    print("Caugth Signal CNTL^C..exiting gracefully")
    sys.exit(0)

#
# create a connection to a socket where a spark streaming context will listen for incoming JSON strings
#
def create_spark_connection(port, host):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as err:
        print 'Failed to create socket. Error code: ' + str(err.msg[0]) + ' , Error message : ' + err.msg[1]
        return 0
    try:
        remote_ip = socket.gethostbyname(host)
    except socket.gaierror:
        print 'Hostname could not be resolved. Exiting'
        return 0

    s.connect((remote_ip, port))
    return s

#
# close the connection to the stream
#
def close_spark_connection(s):
    if s:
        s.close()
#
# send JSON object or message to the Spark streaming context listener
#
def send_to_spark(s, dmsg):
    s.sendall(dmsg)

#
# Get the file descriptor for the directory/filename
# Note that this file will overwrite existing file
#
def get_file_handle(iot_dir, i):
    global device_file
    fd = None
    fname = str(i) + "-" + device_file
    try:
        path = os.path.join(iot_dir, fname)
        fd = open(path, "w")
    except IOError as e:
        print >> sys.stderr, "I/O error({0}): {1}".format(e.errno, e.strerror)
        sys.exit(-1)
        return fd, fname

#
# close the filehandle
#
def close_file_handle(fd):
    fd.close()

#
# write the device json to the file
#
def write_to_dir(fd, djson):
    try:
        fd.write(djson)
        fd.write("\n")
    except IOError as e:
        print >> sys.stderr, "I/O error({0}): {1}".format(e.errno, e.strerror)


def getLargeBatches(size):
    batches = []
    for i in range(1, size):
        # hard code to setup 10 sensors in the device
        iot_device_obj = IoT_Device(i, 10) 
        batches.append(iot_device_obj)
    return batches


def parse_args():
    parser = argparse.ArgumentParser(description='PubNub Publisher for JSON devices messages for a public channel "devices"')
    parser.add_argument('--number', type=int, required=False, default=50,
                                            help='Number of psuedo-devices generated')
    parser.add_argument('--data_dir', type=str, required=False, default="data_dir",
                                            help='Directory to write JSON data files for the Spark Streaming application')
    parser.add_argument('--host', type=str, required=False, default='localhost',
                                            help='hostname of Cassandra API[TODO]')
    parser.add_argument('--port', type=int, required=False, default=9160,
                                            help='port of Cassandra API[TODO]')
    return parser.parse_args()

if __name__ == "__main__":

    args = parse_args()
    number = args.number
    data_dir = args.data_dir

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    signal.signal(signal.SIGINT, signal_handler)
    #
    # fetch the batches
    batches = getLargeBatches(number)
    
    while (1) :
        for iot_device in batches:
            sensor_data = iot_device.create_sensor_data()
            for d in sensor_data:
                producer.send('iot-topic', key="iot", value=d)
                print "Send to Kafka: %s" % d
        time.sleep(10)
        # influxUtil.insert_into_dbs(["InfluxDB"], ret)
    
    #
    # Create psuedo devices for provisioning as though they are all publishing upon activation
    # Use number of iterations and sleep between them. For each iteration, launch a thread that will
    # execute the function.
    #
    
    #filed, file_name = get_file_handle(data_dir, i)
    #print ("%d Devices' infomation published on PubNub Channel '%s' and data written to file '%s'" % (number, ch, os.path.join(data_dir, file_name)))
    #close_file_handle(filed)
