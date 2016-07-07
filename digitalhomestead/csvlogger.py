#!/home/eng-nbb/.python/lansdown/bin/python2.6

# DigitalHomesteadCSVLogger logs the Walk Over Weigher data to CSV Files
# Copyright (C) 2016  NigelB, eResearch, James Cook University
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
import time
import sys

import datetime
import os
from pubnub import Pubnub
import struct
import json
from argparse import ArgumentParser

STATUS = 0xFe
ERROR = 0xFF

ERROR_PARSE_ERROR = 1
STATUS_HEARTBEAT = 1

ERROR_CODE_PARSE_ERROR_UNEXPECTED_NULL = 0
ERROR_CODE_PARSE_ERROR_INCORRECT_INPUT_COUNT = 1

HEARTBEAT_TYPE_STARTUP = 0
HEARTBEAT_TYPE_HOURLY  = 1

csv_format = "%Y-%m-%d"

def parse_wow(message):
    logger = logging.getLogger("parse")
    msg = message['data']['user_payload']
    xmsg = msg.decode('hex')
    id, weight = struct.unpack("qi", xmsg)
    weight = float(weight) / 100
    logger.info("RCV: %s, ID: %s, Weight: %s" % (message['receiver'], id, weight))
    return id, weight

def unpack_status_message(message):
    value = int(message, 16)
    return struct.unpack("BBBB", struct.pack("I", value))

def accept_transmission(configuration, channel, message):
    if message['receiver'] in configuration.accept.receiver:
        return True
    if message['tag_id'] in configuration.accept.radio_ids:
        return True
    if message['location'] in configuration.accept.location:
        return True
    return False



def create_handler(configuration):
    logger = logging.getLogger("Handler")
    def handler(message, channel):
        try:
            if accept_transmission(configuration, channel, message):
                logging.info(message)
                print 'data' in message and 'user_payload' in message['data']
                csv_file_name = os.path.join(configuration.csv_output_dir, "%s.csv"%time.strftime(configuration.csv_format))
                with open(csv_file_name, "a+") as csv:
                    if os.path.getsize(csv_file_name) == 0:
                        print >> csv, "Time,ID,Weight(KG),TagID,Sequence,RSSI,Receiver,MESSAGE_TYPE"
                    tag_id = message['tag_id']
                    rssi = message['rssi']
                    reciever = message['receiver']
                    sequence = message['data']['sequence']
                    if 'data' in message:
                        if 'user_payload' in message['data']:
                            id, weight = parse_wow(message)
                            ts = datetime.datetime.fromtimestamp(message['time'])

                            print >> csv, "%s,%s,%s,%s,%s,%s,%s,WEIGHT"%(ts, id, weight, tag_id, sequence, rssi, reciever)

                        elif 'alt_user_data' in message['data']:
                            ts = datetime.datetime.fromtimestamp(message['time'])
                            message_type, status, _, status_code = unpack_status_message(message['data']['alt_user_data'])
                            status_message = "UNKNOWN_STATUS_MESSAGE"
                            if message_type == ERROR:
                                if status == ERROR_PARSE_ERROR:
                                    status_message="UNKNOWN_PARSE_ERROR"
                                    if status_code == ERROR_CODE_PARSE_ERROR_UNEXPECTED_NULL:
                                        status_message="PARSE_ERROR_UNEXPECTED_NULL"
                                    elif status_code == ERROR_CODE_PARSE_ERROR_INCORRECT_INPUT_COUNT:
                                        status_message="PARSE_ERROR_INCORRECT_INPUT_COUNT"

                            if message_type == STATUS:
                                status_message="UNKNOWN_STATUS_MESSAGE"
                                if status == STATUS_HEARTBEAT:
                                    status_message="HEARTBEAT"
                                    if status_code == HEARTBEAT_TYPE_STARTUP:
                                        status_message="HEARTBEAT_STARTUP"
                                    elif status_code == HEARTBEAT_TYPE_HOURLY:
                                        status_message="HEARTBEAT_PERIODIC"

                            print >> csv, "%s,,%s,%s,%s,%s,%s"%(ts, tag_id,sequence, rssi,reciever, status_message)
        except Exception, e:
            logger.debug("Exception: "+e.message, e)
    return handler

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

def main():

    argParse = ArgumentParser(description="Digital Homestead CSV Logger creates CSV files of the Walk Over Weigher Data")
    argParse.add_argument("--config",metavar="<config>", dest="config", default="config.json", action="store", help="The location of the config JSON file. default: config.json")
    args = argParse.parse_args()

    if not os.path.exists(args.config):
        print 'Configuration file: ' + args.config + " Does not exists."
        print
        argParse.print_help()
        sys.exit(1)

    configuration = None
    with open(args.config, "rb") as data:
        configuration = AttrDict(json.load(data))
        configuration.pubnub = AttrDict(configuration.pubnub)
        configuration.accept = AttrDict(configuration.accept)

    logging.basicConfig(format=configuration.log_format, level=logging.INFO, filename=configuration.log_file)
    logger = logging.getLogger("Main")
    logger.info("Starting...")
    hdlr = logging.StreamHandler(sys.stderr)
    hdlr.setFormatter(logging.Formatter(configuration.log_format))
    hdlr.setLevel(logging.NOTSET)
    logging.root.addHandler(hdlr)

    pubnub = Pubnub(publish_key=configuration.pubnub.publish_key, subscribe_key=configuration.pubnub.subscribe_key)
    pubnub.subscribe(channels=configuration.pubnub.channels, callback=create_handler(configuration))


if __name__ == "__main__":
    main()


