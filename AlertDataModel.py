import boto3

from Constants import agg_table_key_separator, get_epoch, pattern2, table_bsm_alerts, kinesis_agg_stream, \
    table_bsm_data, table_bsm_agg, devices, datatypes
from Database import Database

HEART_RATE = "HeartRate"
SPO2 = "SPO2"
TEMPERATURE = "Temperature"
rules = {
    HEART_RATE: {'avg_min': 55, 'avg_max': 79, 'trigger_count': 2},
    SPO2: {'avg_min': 55, 'avg_max': 88, 'trigger_count': 2},
    TEMPERATURE: {'avg_min': 55, 'avg_max': 105, 'trigger_count': 1}
}

heartRate_max_alert = []
heartRate_min_alert = []
SPO2_max_alert = []
SPO2_min_alert = []
temperature_max_alert = []
temperature_min_alert = []

database = Database()

client = boto3.client('kinesis')
shardIterator = client.get_shard_iterator(
    StreamName=kinesis_agg_stream,
    ShardId='shardId-000000000000',
    ShardIteratorType='LATEST',
)['ShardIterator']


def is_diff_less_than_equal_minute(last_starttime, starttime):
    if last_starttime == -1:
        return True;
    epoch_last_starttime = get_epoch(last_starttime, pattern2)
    epoch_starttime = get_epoch(starttime, pattern2)
    diff_less_than_minute = (epoch_starttime - epoch_last_starttime) < 60
    return diff_less_than_minute


def checkBreach(avg, deviceid_datatype: str, vital, starttime, alert_max_array: list, alert_min_array: list):
    datatype = deviceid_datatype.split(agg_table_key_separator)[1]
    deviceid = deviceid_datatype.split(agg_table_key_separator)[0]
    rule = rules[datatype]
    try:
        l = len(alert_max_array)
        last_starttime = alert_max_array[l - 1]
    except IndexError:
        last_starttime = -1

    if datatype == vital and avg >= rule['avg_max'] and is_diff_less_than_equal_minute(last_starttime, starttime):
        alert_max_array.append(starttime)
        if len(alert_max_array) >= rule['trigger_count']:
            print(f'{deviceid}\'s {datatype} alert at {starttime}')
            # alert_data = {"deviceid": deviceid, "datatype": datatype, "startime": starttime, "avg_max": avg}
            # database.insert_data(table_bsm_alerts, alert_data)
            return True
    else:
        alert_max_array = []
    if datatype == vital and avg <= rule['avg_min'] and is_diff_less_than_equal_minute(last_starttime, starttime):
        alert_min_array.append(starttime)
        if len(alert_max_array) >= rule['trigger_count']:
            print(f'{datatype} alert at {starttime}')
            # alert_data = {"deviceid": deviceid, "datatype": datatype, "startime": starttime, "avg_min": avg}
            # database.insert_data(table_bsm_alerts, alert_data)
            return True
    else:
        alert_min_array = []
    return False


'''
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
                deviceid_datatype = keys['datatype']['S']
                datatype = deviceid_datatype.split(agg_table_key_separator)[1]
                starttime = keys['starttime']['S']
                if datatype == HEART_RATE:
                    checkBreach(avg, deviceid_datatype, HEART_RATE, starttime, alert_max_array=heartRate_max_alert,
                                alert_min_array=heartRate_min_alert)
                if datatype == SPO2:
                    checkBreach(avg, deviceid_datatype, SPO2, starttime, alert_max_array=SPO2_max_alert,
                                alert_min_array=SPO2_min_alert)
                if datatype == TEMPERATURE:
                    checkBreach(avg, deviceid_datatype, TEMPERATURE, starttime, alert_max_array=temperature_max_alert,
                                alert_min_array=temperature_min_alert)

'''


def raise_alerts_between(start_date, end_date):
    alert_data_list = []
    alert_devices = []
    for device in devices:
        for datatype in datatypes:
            key = f"{device}{agg_table_key_separator}{datatype}"
            for agg_data in database.query_between_dates(tableName=table_bsm_agg, key=key, keyName="datatype", startdate=start_date, enddate=end_date, timeAttributeName="starttime"):
                bsm_data_list = []
                deviceid_datatype = agg_data["datatype"]
                avg = agg_data["avg"]
                starttime = agg_data["starttime"]
                endtime = agg_data["endtime"]
                if datatype == HEART_RATE:
                    if checkBreach(avg, deviceid_datatype, HEART_RATE, starttime, alert_max_array=heartRate_max_alert,
                                   alert_min_array=heartRate_min_alert):
                        alert_devices.append(prepare_alert_data(alert_data_list, device, endtime, starttime, HEART_RATE))
                if datatype == SPO2:
                    if checkBreach(avg, deviceid_datatype, SPO2, starttime, alert_max_array=SPO2_max_alert,
                                   alert_min_array=SPO2_min_alert):
                        alert_devices.append(prepare_alert_data(alert_data_list, device, endtime, starttime, SPO2))
                if datatype == TEMPERATURE:
                    if checkBreach(avg, deviceid_datatype, TEMPERATURE, starttime, alert_max_array=temperature_max_alert,
                                   alert_min_array=temperature_min_alert):
                        alert_devices.append(prepare_alert_data(alert_data_list, device, endtime, starttime, TEMPERATURE))

    for alert_data in alert_devices:
            database.insert_data(table_bsm_alerts, alert_data)


def prepare_alert_data(alert_data_list, deviceid, endtime, starttime, devicetype):
    bsm_data_list = database.query_between_dates(tableName=table_bsm_data, key=deviceid, startdate=starttime,
                                                 enddate=endtime, keyName="deviceid", timeAttributeName="timestamp",
                                                               )
    alert_data_map = {}
    for bsm_data in bsm_data_list:
        datatype = bsm_data["datatype"]
        if datatype == devicetype:
            if bsm_data["value"] <= rules[bsm_data["datatype"]]["avg_min"]:
                threshhold_value = rules[bsm_data["datatype"]]["avg_min"]
                alert_data = {"deviceid": f"{deviceid}{agg_table_key_separator}{datatype}", "startime": bsm_data["timestamp"],
                              "rule_breached": f"avg_min:{threshhold_value}", "datatype": datatype,
                              "value": bsm_data["value"]}
                alert_data_list.append(alert_data)
                if deviceid not in alert_data_map:
                    alert_data_map[deviceid] = alert_data
            elif bsm_data["value"] >= rules[bsm_data["datatype"]]["avg_max"]:
                threshhold_value = rules[bsm_data["datatype"]]["avg_max"]
                alert_data = {"deviceid": f"{deviceid}{agg_table_key_separator}{datatype}", "startime": bsm_data["timestamp"],
                              "rule_breached": f"avg_max:{threshhold_value}", "datatype": datatype,
                              "value": bsm_data["value"]}
                alert_data_list.append(alert_data)
                if deviceid not in alert_data_map:
                    alert_data_map[deviceid] = alert_data
    return alert_data_map[deviceid]


