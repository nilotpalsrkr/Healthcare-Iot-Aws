import boto3

from models.models import Things


class AWSHelper:
    def __init__(self):
        self.client = boto3.client('iot')

    def get_things(self):
        things = list()
        for response in self.client.list_things(thingTypeName='BedSideMonitor')['things']:
            thing = Things(response['thingName'], response['thingTypeName'], response['thingArn'])
            things.append(thing)
        for thing in things:
            print(thing.__dict__)
        return things


awsHelper = AWSHelper()
awsHelper.get_things()
