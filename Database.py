import boto3
from boto3.dynamodb.conditions import Key, Attr


class Database:
    def __init__(self):
        self.client = boto3.client('dynamodb')


    def get_table(self, table_name: str):
        response = self.client.list_tables()['TableNames']

    def get_things(self):
        client = boto3.client('iot')
        things = client.list_things()
        return things["things"]

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

    def query(self, tableName, key):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(tableName)
        response = table.query(KeyConditionExpression=Key('deviceid').eq(key))
        return response['Items']

    def query_between_dates(self, tableName, keyName, key, startdate, enddate, timeAttributeName):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(tableName)
        response = table.query(KeyConditionExpression=Key(keyName).eq(key) & Key(timeAttributeName).between(startdate, enddate))
        return response['Items']

    def query_between_dates_for_attribute(self, tableName, keyName, key, startdate, enddate, timeAttributeName, attribute_name, attribute_value):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(tableName)
        response = table.query(KeyConditionExpression=Key(keyName).eq(key) & Key(timeAttributeName).between(startdate, enddate)
                               & Key(attribute_name).eq(attribute_value))
        return response['Items']

