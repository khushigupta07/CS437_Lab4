# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np

#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 5

#Path to your certificates, modify this
certificate_formatter = "certs/Vehicle_{}_certificate.pem.crt"
key_formatter = "certs/Vehicle_{}_private.pem.key"


#Path to the dataset, modify this
data_path = "data/vehicle{}.csv"


def maxCO2Callback(client, userdata, message):
    print(f"Max CO2 level from {message.topic} is {message.payload.decode()}")

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a25osb8euzdzh-ats.iot.us-east-2.amazonaws.com", 8883)
        self.client.configureCredentials("./certs/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        self.client.connect()
        self.client.subscribe(topic=f"iot/Vehicle_{device_id}", QoS=1, callback=maxCO2Callback)

    def customOnMessage(self, message):
        # print("client {} received payload {} from topic {}".format(self.device_id, "undefined", "undefined"))
        pass

    # Suback callback
    def customSubackCallback(self, mid, data):
        pass

    # Puback callback
    def customPubackCallback(self, mid):
        #You don't need to write anything here
        print("Callback")
        pass

    def publish(self, topic="vehicle/emission/data"):
        # Load the vehicle's emission data
        df = pd.read_csv(data_path.format(self.device_id))
        row = df.iloc[self.state]
        payload = json.dumps(row.to_dict())
        # payload = payload + payload + payload  # 1800 bytes
        print(f"Publishing: {payload} to {topic}")
        self.client.publishAsync(topic, payload, 0, ackCallback=self.customPubackCallback)
        self.state += 1

print("Loading vehicle data...")
data = []
for i in range(5):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    client = MQTTClient(device_id, certificate_formatter.format(device_id),
                        key_formatter.format(device_id))
    client.client.connect()
    clients.append(client)

while True:
    print("send now?")
    x = input()
    if x == "s":
        for i, c in enumerate(clients):
            c.publish()

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")


# python3 aws-iot-device-sdk-python-v2/samples/basic_discovery.py --thing_name Vehicle_0 --topic 'vehicle/emission/data' --message '{"vehicle_CO2": 345.05, "vehicle_id": "veh0"}' --ca_file ./certs/AmazonRootCA1.pem --cert ./certs/Vehicle_0_certificate.pem.crt --key ./certs/Vehicle_0_private.pem.key --region us-east-2 --verbosity Warn
# python3 aws-iot-device-sdk-python-v2/samples/basic_discovery.py --thing_name Vehicle_0 --topic 'vehicle/emission/data' --message '{"vehicle_CO2": 345.05, "vehicle_id": "veh0"}' --ca_file ./certs/greengrass-root-ca.pem --cert ./certs/Vehicle_0_certificate.pem.crt --key ./certs/Vehicle_0_private.pem.key --region us-east-2 --verbosity Warn
#
# mosquitto_pub -h 3.137.172.255 -p 8883 \
# --cafile ./certs/AmazonRootCA1.pem \
# --cert ./certs/Vehicle_0_certificate.pem.crt \
# --key ./certs/Vehicle_0_private.pem.key \
# -t "vehicle/emission/data" -m '{"vehicle_CO2": 345.05, "vehicle_id": "veh0"}'

#
# mosquitto_pub \
#   -h 3.137.172.255 \
#   -p 8883 \
#   --cafile ./certs/greengrass-root-ca.pem \
#   --cert ./certs/Vehicle_0_certificate.pem.crt \
#   --key ./certs/Vehicle_0_private.pem.key \
#   -t vehicle/emission/data \
#   -m '{"vehicle_CO2": 123.45, "vehicle_id": "veh0"}'
