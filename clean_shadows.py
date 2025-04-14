import json
import boto3

# Create IoT client
iot = boto3.client("iot-data", region_name='us-east-2')  # Uses AWS region by default


def refresh_shadow_max_CO2(vehicle_id):
    try:
        payload = {
            "state": {
                "reported": {
                    "max_CO2": 0
                }
            }
        }
        iot.update_thing_shadow(
            thingName=f"Vehicle_{vehicle_id}",
            payload=json.dumps(payload)
        )
    except Exception as e:
        print(f"Error updating shadow for {vehicle_id}: {e}")


for i in range(5):
    refresh_shadow_max_CO2(i)
