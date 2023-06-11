import datetime
import time
from decimal import Decimal

from Constants import pattern2, pattern1, get_epoch, table_bsm_agg, dict_key_separator, agg_table_key_separator, \
    devices, table_bsm_data
from Database import Database

database = Database()


def get_bsmData_between(startdate, endDate):
    bsm_data = []
    for device in devices:
        print(f"Aggregating for device :{device}")
        data = database.query_between_dates(tableName=table_bsm_data, key=device,
                                            startdate=startdate, enddate=endDate, keyName="deviceid",
                                            timeAttributeName="timestamp")
        for d in data:
            # bsm_data.append(json.dumps(d, use_decimal=True))
            bsm_data.append(d)
    return bsm_data


# pattern1 = '%Y-%m-%d %H:%M:%S.%f'
# pattern2 = '%Y-%m-%d %H:%M:%S'

def get_epoch_every_minutes(startepoch, endepoch):
    epoch_list = []
    temp = startepoch
    while temp <= endepoch:
        epoch_list.append(temp)
        temp += 60
    return epoch_list


def aggregate_bsm_date_between(bsm_data, starttime, endtime):
    epoch_start = get_epoch(starttime, pattern2)
    epoch_end = get_epoch(endtime, pattern2)
    epoch_list = get_epoch_every_minutes(epoch_start, epoch_end)
    bsm_data_filtered = list()
    bsm_time_divided_dict = {}
    bsm_agg_dict = {}
    c = 0
    for d in bsm_data:
        epoch_of_device_timestamp = int(
            time.mktime(time.strptime(d['timestamp'], pattern1)))  # get_epoch(d['timestamp'], pattern1) #
        i = 0
        while i < len(epoch_list) - 1 and epoch_of_device_timestamp >= epoch_list[i]:
            epoch_end = epoch_list[i + 1]
            epoch_start = epoch_list[i]
            if epoch_start <= epoch_of_device_timestamp <= epoch_end:
                bsm_data_filtered.append(d)
                datatype = d['datatype']
                deviceid = d['deviceid']
                timestamp = d['timestamp']
                key_for_agg_dict = f"{deviceid}{dict_key_separator}{datatype}{dict_key_separator}{str(datetime.datetime.fromtimestamp(epoch_start))}"
                value = d['value']
                if key_for_agg_dict in bsm_time_divided_dict:
                    bsm_time_divided_dict[key_for_agg_dict].append(
                        {
                            "starttime": f"{str(datetime.datetime.fromtimestamp(epoch_start))} To {str(datetime.datetime.fromtimestamp(epoch_end))}",
                            "endtime": str(datetime.datetime.fromtimestamp(epoch_end)), "bsm_data": d})
                    agg_data = bsm_agg_dict[key_for_agg_dict]
                    agg_data["max"] = max(value, agg_data["max"])
                    agg_data["min"] = min(value, agg_data["min"])
                    agg_data["sum"] = agg_data["sum"] + value
                    agg_data["count"] = agg_data["count"] + 1
                    agg_data["timestamp"] = timestamp
                    agg_data[key_for_agg_dict] = agg_data
                else:
                    bsm_time_divided_dict[key_for_agg_dict] = []
                    bsm_time_divided_dict[key_for_agg_dict].append(
                        {
                            "starttime": f"{str(datetime.datetime.fromtimestamp(epoch_start))} To {str(datetime.datetime.fromtimestamp(epoch_end))}",
                            "endtime": str(datetime.datetime.fromtimestamp(epoch_end)), "bsm_data": d})

                    bsm_agg_dict[key_for_agg_dict] = {"starttime": str(datetime.datetime.fromtimestamp(epoch_start)),
                                                      "max": value, "min": value, "sum": value, "count": 1}
            i += 1

    bsm_agg_data = {}
    result = []
    for datatype_starttime, agg_data in bsm_agg_dict.items():
        maxx = agg_data['max']
        minn = agg_data['min']
        avgg = Decimal(agg_data['sum'] / agg_data['count'])
        # range = agg_data['starttime']
        # timestamp = agg_data['timestamp']
        deviceid = datatype_starttime.split(dict_key_separator)[0]
        datatype = datatype_starttime.split(dict_key_separator)[1]
        starttime = datatype_starttime.split(dict_key_separator)[2]
        endtime_epoch = get_epoch(starttime, pattern2) + 60
        endtime = str(datetime.datetime.fromtimestamp(endtime_epoch))
        result.append(
            {"datatype": f"{deviceid}{agg_table_key_separator}{datatype}", "starttime": starttime, "endtime": endtime,
             "max": maxx, "min": minn, "avg": avgg})  # {deviceid}{agg_table_key_separator}

    return result


def print_bsm(bsm_data):
    for data in bsm_data:
        epoch = int(time.mktime(time.strptime(data['timestamp'], pattern1)))
        deviceid = data['deviceid']
        value = data['value']
        date_time = datetime.datetime.fromtimestamp(epoch)
        print(f"{deviceid} :: {date_time} :: {value}")


def print_dict(dictionary: dict):
    for key, value in dictionary.items():
        print(f"{key} :: {value}")
        print("\n")


def aggregate_bsm_data_push_to_awsDynamoDB(start_date, end_date):
    bsm_data = get_bsmData_between(start_date, end_date)
    result = aggregate_bsm_date_between(bsm_data, start_date, end_date)
    print(f"Aggregation between {start_date} and {end_date} results in {len(result)} data. Going to persist in {table_bsm_agg}")
    for res in result:
        database.insert_data(table_bsm_agg, res)
# total_minutes = len(get_epoch_every_minutes(get_epoch(start_date, pattern2), get_epoch(end_date,pattern2))) - 1
