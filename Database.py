import time

import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key, Attr


class Database:
    def __init__(self):
        self.client = boto3.client('dynamodb')


    def get_table(self, table_name: str):
        response = self.client.list_tables()['TableNames']
        print(response)

    def create_table(self, name):
        attribute_definitions = [{
            'AttributeName': 'deviceid',
            'AttributeType': 'S'
        },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            }
        ]
        table_name = name
        key_schema = [
            {
                'AttributeName': 'deviceid',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'
            }
        ]
        provisioned_throughput = {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
        table = self.client.create_table(AttributeDefinitions=attribute_definitions, TableName=table_name,
                                         KeySchema=key_schema, ProvisionedThroughput=provisioned_throughput)
        return table

    def insert_data(self, table, data):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table)
        table.put_item(Item=data)
        print(f"Items successfully inserted in {table} :: {data}")

    def query(self, tableName, select):
        #response = self.client.query(TableName=tableName, Select=select,KeyConditionExpression=Key('deviceid').eq('Dev-Thing-2').expression_format) #"partitionKeyName= :Dev-Thing-2"
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(tableName)
        filter_expression = 'timestamp between :time1 and :time2'
        #expression_attribute_value = {'':,'time1':'2021-11-20 23:38', 'time2':'2021-11-20 23:39'}
        #response = table.query(KeyConditionExpression=Key('deviceid').eq('Dev-Thing-2'), FilterExpression=Attr('timestamp').between('2021-11-25 19:59','2021-11-25 19:60'))
        response = table.query(KeyConditionExpression=Key('deviceid').eq('Dev-Thing-2'))

        return response['Items']

database = Database()
database.get_table("bsm_raw_data")
#test = database.create_table("test")
#time.sleep(10)
data = database.query(tableName="bsm_raw_data", select="ALL_ATTRIBUTES")
#for d in data:
#    print(d)
#table.insert_data(test, "")
