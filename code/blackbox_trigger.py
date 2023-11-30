#!/usr/bin/env python3
# coding: utf-8

import os
import json
import logging
import argparse

import paho.mqtt.client as mqtt


class BlackBoxTrigger():

    def __init__(self, mqtt_host, mqtt_port, mqtt_topic, objects):
        self.objects = objects.split(',')
        self.mqtt_host  = mqtt_host
        self.mqtt_port  = mqtt_port
        self.mqtt_topic = mqtt_topic
        self.mqtt = mqtt.Client()
        self.mqtt.enable_logger()
        self.mqtt.on_connect = self.mqtt_on_connect
        self.mqtt.on_message = self.mqtt_on_message
        logging.debug(f'Params: {self.__dict__}')
    
    def run(self):
        self.mqtt.connect(self.mqtt_host, self.mqtt_port)
        self.mqtt.subscribe(self.mqtt_topic)
        self.mqtt.loop_forever()

    @staticmethod
    def mqtt_on_connect(client, userdata, flags, rc):
        logging.info(f'Connected with result code {rc}')

    def mqtt_on_message(self, client, userdata, msg):
        logging.debug(f'Message on topic {msg.topic}. {len(msg.payload)} bytes')
        try:
            message = msg.payload.decode()
            logging.debug(f'Topic {msg.topic}: {message}')

            try:
                data = json.loads(message)
            except:
                logging.exception(f'Failed to parse JSON data: {data}')
            else:
                obj = data.get('type')
                if obj in self.objects:
                    self.blackbox_record(f'{obj} detected')
        except Exception as e:
            logging.exception('Failed to process MQTT message: ' + str(e))

    def blackbox_record(self, message):
        self.mqtt.publish('blackbox/record', message)
    

def get_argument_parser():
    parser = argparse.ArgumentParser(add_help=False, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--help',            action='help',       help='Show this help message and exit')
    parser.add_argument('-v', '--verbose',   action='store_true', help='Verbose logging')
    parser.add_argument('-h', '--host',      default=os.environ.get('MQTT_HOST',  '127.0.0.1'),    help='MQTT broker host')
    parser.add_argument('-p', '--port',      default=os.environ.get('MQTT_PORT',  1883), type=int, help='MQTT broker port')
    parser.add_argument('-t', '--topic',     default=os.environ.get('MQTT_TOPIC', '#'),            help='MQTT topic')
    parser.add_argument('-o', '--objects',   default=os.environ.get('OBJECTS', 'person'),          help='List of objects (csv) which will trigger the generation of a BlackBox')
    return parser


def main():
    args = get_argument_parser().parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)

    bb_trigger = BlackBoxTrigger(mqtt_host=args.host, 
                                 mqtt_port=args.port, 
                                 mqtt_topic=args.topic,
                                 objects=args.objects)

    bb_trigger.run()


if __name__ == '__main__':
    main()
