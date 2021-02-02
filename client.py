import time
import json
import logging
import paho.mqtt.client as mqtt

class MatrixClient:

    def __init__(self, client_id, mqtt_server="mqtt_server", mqtt_port=1883):
        connected = False

        while not connected:
            try:
                self.client = mqtt.Client(client_id=client_id)
                self.client.connect(mqtt_server, mqtt_port)
                self.client.loop_start()
                connected = True
                print("Connected")
            except Exception as ex:
                print(f"Failed to connect to MQTT server {ex}")
                time.sleep(30)

    def _send_msg(self, topic, content):
        self.client.publish(f"matrix/{topic}", json.dumps(content), 2)

    # value should always be false, if not, there is a problem
    def set_issue_state(self, device_id, key, has_issue):
        logging.debug(f"device_id: {device_id} key: {key} has_issue: {has_issue}")
        msg = {"key": key, "has_issue": has_issue, "device_id": device_id}
        self._send_msg(f"issue_state/{device_id}", msg)

    def set_sensor_state(self, device_id, key, value):
        logging.debug(f"SENSOR_STATE | device_id: {device_id}  {key}={value}")

        msg = {"key": key, "value": value, "device_id": device_id}
        self._send_msg(f"sensor_state/{device_id}", msg)

    def set_daily_measurement(self, device_id, date, key, value):
        logging.debug(f"DAILY_MEASUREMENT | device_id: {device_id} date: {date} {key}={value}")

        msg = {"key": key, "value": value, "date": date, "device_id": device_id}
        self._send_msg(f"daily_measurement/{device_id}", msg)
