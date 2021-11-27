import datetime
from decimal import Decimal
import time
from statistics import mean

from Database import Database

database = Database()
bsm_data = database.query(tableName="bsm_raw_data", select="ALL_ATTRIBUTES")
pattern = '%Y-%m-%d %H:%M:%S.%f'
pattern2 = '%Y-%m-%d %H:%M:%S'

def get_epoch_every_minutes(startepoch, endepoch):
    epoch_list = []
    temp = startepoch
    while temp <= endepoch:
        epoch_list.append(temp)
        temp += 60
    return epoch_list


def get_data_between(bsm_data, starttime, endtime):
    epoch_start = int(time.mktime(time.strptime(starttime, pattern2)))
    epoch_end = int(time.mktime(time.strptime(endtime, pattern2)))
    epoch_list = get_epoch_every_minutes(epoch_start, epoch_end)
    bsm_data_filtered = list()
    bsm_time_divided_dict = {}
    bsm_agg_dict = {}
    c = 0
    for d in bsm_data:
        epoch_of_device_timestamp = int(time.mktime(time.strptime(d['timestamp'], pattern)))
        i = 0
        while i < len(epoch_list) - 1 and epoch_of_device_timestamp >= epoch_list[i]:
            epoch_end = epoch_list[i+1] + 60
            epoch_start = epoch_list[i] - 60
            if epoch_start <= epoch_of_device_timestamp <= epoch_end:
                bsm_data_filtered.append(d)
                datatype = d['datatype']
                key_for_agg_dict = f"{datatype}_{str(datetime.datetime.fromtimestamp(epoch_start))}"

                if key_for_agg_dict in bsm_time_divided_dict:
                    bsm_time_divided_dict[key_for_agg_dict].append({"starttime": str(datetime.datetime.fromtimestamp(epoch_start)),
                                                   "endtime": str(datetime.datetime.fromtimestamp(epoch_end)), "bsm_data": d})
                    agg_data = bsm_agg_dict[key_for_agg_dict]
                    value = d['value']
                    agg_data["max"] = max(value, agg_data["max"])
                    agg_data["min"] = min(value, agg_data["min"])
                    agg_data["sum"] = agg_data["sum"] + value
                    agg_data["count"] = agg_data["count"] + 1
                    agg_data[key_for_agg_dict] = agg_data
                else:
                    bsm_time_divided_dict[key_for_agg_dict] = []
                    bsm_time_divided_dict[key_for_agg_dict].append({"starttime": str(datetime.datetime.fromtimestamp(epoch_start)),
                                                   "endtime": str(datetime.datetime.fromtimestamp(epoch_end)), "bsm_data": d})

                    bsm_agg_dict[key_for_agg_dict] = {"starttime": str(datetime.datetime.fromtimestamp(epoch_start)), "max": -1, "min": 99999, "sum": 0, "count": 1}
            i += 1

    bsm_agg_data = {}
    result = []
    for datatype_starttime, agg_data in bsm_agg_dict.items():
        maxx = agg_data['max']
        minn = agg_data['min']
        avgg = Decimal(agg_data['sum'] / agg_data['count'])
        # print(f"{datatype} :: {maxx}, {minn}, {avgg}")
        #bsm_agg_data[datatype] = {"max": maxx, "min": minn, "avg": avgg}
        datatype = datatype_starttime.split("_")[0]
        starttime = datatype_starttime.split("_")[1]
        result.append({"datatype": datatype, "starttime": starttime, "max": maxx, "min": minn, "avg": avgg})

    return result


def print_bsm(bsm_data):
    for data in bsm_data:
        epoch = int(time.mktime(time.strptime(data['timestamp'], pattern)))
        deviceid = data['deviceid']
        value = data['value']
        date_time = datetime.datetime.fromtimestamp(epoch)
        print(f"{deviceid} :: {date_time} :: {value}")


def print_dict(dictionary: dict):
    for key, value in dictionary.items():
        print(f"{key} :: {value}")
        print("\n")


result = get_data_between(bsm_data, "2021-11-20 23:38:00", "2021-11-22 00:09:00")
for res in result:
    database.insert_data("bsm_agg_test", res)
