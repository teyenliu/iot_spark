'''
Created on Nov 24, 2016

@author: liudanny
'''

import sys
import signal
import os
import json
import argparse
import ast
import influxUtil
from kafka import KafkaConsumer


def signal_handler(signal, frame):
    print("Caugth Signal CNTL^C..exiting gracefully")
    sys.exit(0)
  
def parse_args():
    parser = argparse.ArgumentParser(description='Kafka subscriber for JSON messages for a public channel "devices"')
    parser.add_argument('--topic', type=str, required=False, default='iot-topic',
                        help='Kafka subscriber')
    parser.add_argument('--host', type=str, required=False, default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    return parser.parse_args()


def main(topic="iot-topic", host="localhost", port=8086):
    
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(topic,
        group_id='my-group',
        bootstrap_servers=['localhost:9092'])
    
    signal.signal(signal.SIGINT, signal_handler)
    
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        try:
            ret = ast.literal_eval(message.value)
            influxUtil.insert_into_dbs(["InfluxDB"], ret)
            print ("Receive from Kafka: %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                message.offset, message.key, message.value))
        except Exception as e:
            print "Exception:%s" % e


if __name__ == "__main__":
    args = parse_args()
    main(topic=args.topic)