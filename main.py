from AggregateModel import aggregate_bsm_data_push_to_awsDynamoDB
from AlertDataModel import raise_alerts_between
from Constants import table_bsm_agg

start_date = "2021-12-03 11:30:00"
end_date = "2021-12-04 01:30:00"
print(f"Aggregating BSM Data between dates {start_date} and {end_date}")
aggregate_bsm_data_push_to_awsDynamoDB(start_date, end_date)
print(f"All Writes to {table_bsm_agg} completed successfully")
print("----------------------------------------------")
print(f"Now scanning for alerts between {start_date} and {end_date}")
raise_alerts_between(start_date, end_date)