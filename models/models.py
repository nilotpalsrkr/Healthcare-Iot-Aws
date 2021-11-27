class Things:

    def __init__(self,thingName,thingTypeName,thingArn):
        self.thingName = thingName
        self.thingTypeName = thingTypeName
        self.thingArn = thingArn

    def get_thing_name(self):
        return self.thingName
