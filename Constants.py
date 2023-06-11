import datetime
import time

pattern1 = '%Y-%m-%d %H:%M:%S.%f'
pattern2 = '%Y-%m-%d %H:%M:%S'

dict_key_separator = "__"
agg_table_key_separator = "_"

table_bsm_alerts = 'bsm_alerts'
table_bsm_data = "bedside"
table_bsm_agg = "bsm_agg_data"

kinesis_agg_stream = 'bsm_agg_kinesis'
devices = ["Bedside-Thing-1", "Bedside-Thing-2"]
datatypes = ["HeartRate", "SPO2", "Temperature"]


def get_epoch(t: datetime, pattern):
    return int(time.mktime(time.strptime(t, pattern)))
