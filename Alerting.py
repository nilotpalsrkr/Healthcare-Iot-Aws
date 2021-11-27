import json
from decimal import Decimal

import boto3

from Database import Database

HEART_RATE = "HeartRate"
SPO2 = "SPO2"
TEMPERATURE = "Temperature"
rules = {
    HEART_RATE: {'avg_min': 55, 'avg_max': 105, 'trigger_count': 2},
    SPO2: {'avg_min': 55, 'avg_max': 105, 'trigger_count': 2},
    TEMPERATURE: {'avg_min': 55, 'avg_max': 105, 'trigger_count': 1}
}

heartRate_max_alert, heartRate_min_alert = []
SPO2_max_alert, SPO2_min_alert = []
temperature_max_alert, temperature_min_alert = []

database = Database()

client = boto3.client('kinesis')
shardIterator = client.get_shard_iterator(
    StreamName='bsm_agg_kinesis',
    ShardId='shardId-000000000000',
    ShardIteratorType='LATEST',
)['ShardIterator']


def checkBreach(avg, datatype, vital, starttime, alert_max_array, alert_min_array):
    rule = rules[datatype]
    if datatype == vital and avg >= rule['avg_max']:
        alert_max_array.append(avg)
        if len(alert_max_array) >= rule['trigger_count']:
            print(f'{datatype} alert at {starttime}')
            alert_data = {"datatype": datatype, "starttime": starttime, "avg_max": avg}
            database.insert_data("bsm_alert", alert_data)
    else:
        alert_max_array = []

    if datatype == vital and avg <= rule['avg_min']:
        alert_min_array.append(avg)
        if len(alert_max_array) >= rule['trigger_count']:
            print(f'{datatype} alert at {starttime}')
            alert_data = {"datatype": datatype, "starttime": starttime, "avg_min": avg}
            database.insert_data("bsm_alert", alert_data)
    else:
        alert_min_array = []


while True:
    response = client.get_records(
        ShardIterator=shardIterator
    )
    shardIterator = response['NextShardIterator']
    if len(response['Records']) > 0:
        for item in response['Records']:
            readings = json.loads(item["Data"])
            if readings['eventName'] != "REMOVE":
                keys = readings['dynamodb']['Keys']
                avg = Decimal(readings['dynamodb']['NewImage']["avg"]['N'])
                datatype = keys['datatype']['S']
                starttime = keys['starttime']['S']
                if datatype == HEART_RATE:
                    checkBreach(avg, datatype, HEART_RATE, starttime, alert_max_array=heartRate_max_alert, alert_min_array=heartRate_min_alert)
                if datatype == SPO2:
                    checkBreach(avg, datatype, SPO2, starttime, alert_max_array=SPO2_max_alert, alert_min_array=SPO2_min_alert)
                if datatype == TEMPERATURE:
                    checkBreach(avg, datatype, TEMPERATURE, starttime, alert_max_array=temperature_max_alert, alert_min_array=temperature_min_alert)
