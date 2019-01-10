from configparser import ConfigParser
from datetime import datetime, timedelta
import time
from sqs import sqsBoto
from dls_sas_interface import DlsSasInterface
from copy import copy
from dynamodb import DynamoDB
from sys import exit
from enum import Enum
from collections import OrderedDict


class Configuration(Enum):
    sas_url = 0
    sqs_queue = 1
    declare_queue = 2
    threshold = 3
    enable_declare = 4
    enable_dynamo = 5


# data [current sensors and dpa established from the dls app]
g_sensor_data = {}  # TODO:
g_complete_sensor_data_from_db = {}  # TODO: Done
#
g_dpa_data = {}  # {"dpa_region": dpa_east_1, "channels" = [], "dpa_index": dpa_id, "status": True/False}
g_dpa_channel_in_progress_data = {}  # {"dpa_region": dpa_east_1, "channels";[], "dpa_index": dpa_id}

# sensor list [connected]
g_sensor_connected = []  # [list of connected sensors] ; index is fetched using sensor_id (from sensor health)

# dynamo db; index and list = []
# {'10': ['VA004', 'NC002', 'DE001'], '7': ['VA002', 'NC003'], '1': ['NC001', 'VA001'], '19': ['VA004', 'MD001',
# 'NC004'], '13': ['VA004', 'VA001', 'NC004'], '2': ['NC002', 'VA002'], '14': ['VA004', 'NC003', 'DE001'],
# '16': ['VA004', 'NC003', 'MD001'], '15': ['VA004', 'NC003', 'DE002'], '17': ['VA004', 'DE001', 'NC004'],
# '11': ['VA004', 'NC002', 'DE002'], '4': ['NC001', 'DE002'], '12': ['VA004', 'VA002', 'NC004'],
# '18': ['VA004', 'DE002', 'NC004'], '8': ['VA001', 'NC003'], 'Index': Decimal('3'), '3': ['NC002', 'VA001'],
# '6': ['NC002', 'MD001'], '0': ['NC001', 'VA002'], '9': ['VA004', 'NC001', 'DE001'], '5': ['NC001', 'MD001']}
g_dpa_dynamo_data = OrderedDict()  # from dynamo
g_sensor_site_mapping = {}  # from dynamo

NUM_DPA_S = 27
NUM_CHANNELS = 11

dpa_offline_triggered_ts = datetime.now()  # over ride this variable after triggering offline activation


def assign_none_channel_list():
    return [None, None, None, None, None, None, None, None, None, None]


def return_elapsed(start, end):
    elapsed = end - start
    min, secs = divmod(elapsed.days * 86400 + elapsed.seconds, 60)
    return min, secs


def compare(A, B):
    """Function compare fetches the right dpa combination
    :param A: sensor list (connected ESCs)
    :param B: new sensor id
    :return: True - discovered/ False - combination not found
    """
    # a is sensor list
    # b is dpa list
    if len(A) is 1:
        if len(B) > 1:
            return False
    else:
        x = ([C for C in B if C in A])
        # print("Compare", x, B)
        if len(x) > 0:
            if len(x) == len(B):
                if len(list(set(x) - set(B))) == 0:
                    return True
        else:
            return False


def fetch_sensor_id_mapping():
    global g_sensor_site_mapping  # sensorID (key), deployStatus, siteID, techName
    dy_obj = DynamoDB("us-west-2")
    for idx in range(84800101, 84800392):
        query_item = {"sensorID": idx}
        val = dy_obj.get_item("sensor_site_id_mapping", query_item)
        if val is not None:
            g_sensor_site_mapping[idx] = copy(val)


def discover_the_ranking_combination(sensor_id):
    global g_dpa_dynamo_data
    global g_dpa_data
    for dpa_id, valdct in g_dpa_dynamo_data.items():
        # print(dpa_id, valdct)
        valdct1 = OrderedDict(valdct)
        for index, valdct2 in valdct1.items():
            if type(valdct2) is list:
                if compare(g_sensor_connected, valdct2):
                    dpa_region_index = dpa_id
                    # combination found
                    dpa_region = "dpa_east_" + str(dpa_region_index + 1)
                    g_sensor_data[sensor_id]["assigned_dpa"] = dpa_region  # assign dpa
                    # assign timestamp for each channel in dpa channel list
                    g_dpa_data[dpa_id]["status"] = True
                    g_dpa_data[dpa_id]["channels"] = assign_none_channel_list()


def check_sensor_connection_status(threshold_val):
    for sensor_id, value in g_sensor_data.items():
        end = g_sensor_data[sensor_id]["timestamp"]
        mins, secs = return_elapsed(datetime.now(), end)
        if secs > threshold_val:  # 2 minute delay
            # if the connection lost for more than 2 minutes; TODO raise an alarm
            print("Connection lost -->", sensor_id)


def sensor_health(sqs_py, threshold_val):
    print("Inside producer", datetime.now())
    # sensor health queue
    sensor_id, ts = sqs_py.process_message_from_queue()
    if sensor_id not in g_sensor_data.keys():
        # new sensor connection
        g_sensor_data[sensor_id] = {"state": True, "assigned_dpa": "", "timestamp": datetime.now()}
        g_sensor_connected.append(sensor_id)
    else:
        # update the sensor health timestamp
        g_sensor_data[sensor_id]["timestamp"] = datetime.now()

    # discover the combination based on the sensor connected so far
    discover_the_ranking_combination(sensor_id)

    # check if the connection is lost
    check_sensor_connection_status(threshold_val)


def declare(sqs_py):
    print("")
    sensor_id = sqs_py.process_message_from_queue()
    print(sensor_id)


"""
Function: process_data
1. update the channel detection timestamp in g_dpa_data 
2. trigger sas interface 
"""


def process_data():
    print("Consumer ->", datetime.now())
    time.sleep(1)


def trigger_de_active_for_all_dpa_s(sas_interface):
    global dpa_offline_triggered_ts
    global g_dpa_data  # {dpa_index: {"dpa_region": dpa_east_1, "status": True/False}}
    global g_dpa_channel_in_progress_data  # {dpa_index: {"dpa_region": dpa_east_1, "channels": [],
    # "num_of_channels": channels_in_progress}}
    timestamp = datetime.now() + timedelta(hours=2)
    for dpa_index in range(1, NUM_DPA_S):  # supported DPAs overall - 200
        chlist = (map(lambda x: timestamp, assign_none_channel_list()))
        region = "dpa_east_" + str(dpa_index)
        temp = {"dpa_region": region, "dpa_status": True}
        g_dpa_data[dpa_index] = copy(temp)
        # channels in progress -> dpa
        temp1 = {"dpa_region": region, "channels": chlist, "num_of_channels": 10}
        g_dpa_channel_in_progress_data[dpa_index] = copy(temp1)

    msg = {"heartbeatRequest": [], "timestamp": None}
    # post de-activate message to SAS
    temp_list = []
    for index in range(1, NUM_DPA_S):
        for channel in range(1, NUM_CHANNELS):
            temp_dict = {"dpaId": index, "channel": channel, "type": "interference", "dpaActivated": True}
            temp_list.append(temp_dict)

    msg["heartbeatRequest"] = copy(temp_list)
    now = datetime.now()
    dpa_offline_triggered_ts = now  # reference for newly activated DPAs
    msg["timestamp"] = now.strftime('%Y-%m-%dT%H:%M:%S') + now.strftime('.%f')[:0] + 'Z'
    sas_interface.sendHeartbeatRequest(msg)

    # clean the contents from temp_list
    del temp_list[:]


def check_dpa_channel_time():
    data_dt = {}  # {'dpaId': [], 'dpaId': []}
    t1 = datetime.now()
    for dpa_id, data in g_dpa_data.items():
        if data["status"]:  # if dpa is assigned ; check for timestamp for each channel
            t_list = data["channels"]  # fetch the channels from each assigned DPA
            channel = 0
            temp_list = []
            for t2 in t_list:  # t2 is offline time and delta 2 hours
                if t2 is None:
                    continue
                if t1 > t2:
                    # de-activation timer expired, updated timestamp of each channel to None
                    data["channels"] = assign_none_channel_list()
                    temp_list.append(channel)
                channel = channel + 1
            if len(temp_list) > 0:
                data_dt[dpa_id] = copy(temp_list)
                del temp_list[:]

    return data_dt


def gather_data_from_dynamo():
    global g_dpa_dynamo_data  # Index (dpa), combination priorities
    dy_obj = DynamoDB("us-west-2")
    for idx in range(1, NUM_DPA_S):
        query_item = {"Index": idx}  # Index is of each dpa
        val = dy_obj.get_item("dpa_configuration_with_index", query_item)
        g_dpa_dynamo_data[idx] = copy(val)


def read_configuration():
    global NUM_DPA_S
    read_list = []
    # parse the configuration
    config = ConfigParser()
    candidates = ['config.ini']

    found = config.read(candidates)
    print("Found configuration file:", found)

    # interface
    url = config.get('interface', 'url')

    # general configuration
    sqs_queue = config.get('configuration', 'queue')
    declare_queue = config.get('configuration', 'declare_queue')
    delay_secs = config.get('configuration', 'connection_delay')
    enable_declare = config.get('configuration', 'declare_feature')
    dynamo_interface = config.get('configuration', 'dynamo_interface')

    # number of dpas
    NUM_DPA_S = config.get('dpas_deployed', 'number_of_dpas')

    read_list.insert(Configuration.sas_url.value, url)
    read_list.insert(Configuration.sqs_queue.value, sqs_queue)
    read_list.insert(Configuration.declare_queue.value, declare_queue)
    read_list.insert(Configuration.threshold.value, delay_secs)
    read_list.insert(Configuration.enable_declare.value, enable_declare)
    read_list.insert(Configuration.enable_dynamo.value, dynamo_interface)

    return read_list


def get_complete_sensors_from_db_data():
    global g_complete_sensor_data_from_db  # {<sensor_id> : "site_id": "", "index": "", "assigned": False}
    global g_sensor_site_mapping
    fetch_sensor_id_mapping()
    counter = 1
    for key, values in g_sensor_site_mapping.items():
        sensor_id = values["sensorID"]
        site_id = values["siteID"]
        g_complete_sensor_data_from_db[sensor_id] = {"site_id": site_id, "sensor_id": sensor_id,
                                                     "sensor_index": None, "assigned": False, "dpa_id": None}
        counter = counter + 1


if __name__ == '__main__':

    # read the configuration; config_data is list with starting #0 interface URL
    # 1 SQS Queue URL
    # 2 Declare Queue URL
    # 3 Threshold for sensor health
    # 4 Declarations enable/disable
    # 5 Dynamo interface enable/disable
    config_data = read_configuration()

    # Initialize DPA and Sensor
    if config_data[Configuration.enable_dynamo.value]:  # check if dynamo interface enabled or disabled
        gather_data_from_dynamo()  # completes g_dpa_data
        get_complete_sensors_from_db_data()  # complete g_sensor_site_id_mapping
    else:
        print("Critical: Local dpa data is not configured..")
        print("Make sure you enable dynamo flag in config.ini")
        exit(1)
    # initialize sqs object
    sqs_obj = sqsBoto("us-west-2", config_data[Configuration.sqs_queue.value])
    sas_intf_obj = DlsSasInterface(config_data[Configuration.sas_url.value])

    # de-activate all dpa_s
    trigger_de_active_for_all_dpa_s(sas_intf_obj)
    exit(2)
    # loop starts
    while True:
        # call producer for 25 secs and stop
        secs = 0
        start = datetime.now()
        while secs < 25:
            sensor_health(sqs_obj,
                          config_data[Configuration.threshold.value])  # process the sensor health messages from esc
            if config_data[Configuration.enable_declare.value]:  # check if declarations support is enabled/disabled
                declare(sqs_obj)  # process the declare messages from esc

        # call consumer
        process_data()  # process both sensor health and declare

        # check if any channel timer expires in one of the assigned DPA
        dpa_de_active_data = check_dpa_channel_time()

        # trigger sas interface message
        msg = {"heartbeatRequest": [], "timestamp": None}
        temp_list = []
        for dpa_id, channels in dpa_de_active_data.items():
            for channel in channels:
                temp_dict = {"dpaId": dpa_id, "channel": channel, "type": "interference", "dpaActivated": False}
                temp_list.append(temp_dict)

        msg["heartbeatRequest"] = temp_list
        now = datetime.now()
        msg["timestamp"] = now.strftime('%Y-%m-%dT%H:%M:%S') + now.strftime('.%f')[:0] + 'Z'
        sas_intf_obj.sendHeartbeatRequest(msg)
