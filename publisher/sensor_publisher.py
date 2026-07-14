import itertools
import json
import os
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1886"))

# One topic per database destination. The topic decides WHICH database
# stores the message, and each topic carries a DIFFERENT KIND of data.
TOPIC_NETWORK = os.getenv("MQTT_TOPIC_NETWORK", "home/network")             # -> Neo4j   (house/room/sensor topology)
TOPIC_EVENTS = os.getenv("MQTT_TOPIC_EVENTS", "home/events")                # -> MySQL   (sensor readings + alerts)
TOPIC_TELEMETRY = os.getenv("MQTT_TOPIC_TELEMETRY", "home/telemetry")       # -> MongoDB (raw message per event)
TOPIC_DEVICE_STATUS = os.getenv("MQTT_TOPIC_DEVICE_STATUS", "home/device_status")  # -> MongoDB (battery/settings/logs)

HOUSE = {"house_id": "H001", "house_name": "Green Valley Residence", "address": "142 Maple Street"}

# House topology: which sensors live in which room. This is what gets sent
# to Neo4j so the graph can answer "what sensors protect the kitchen" etc.
ROOMS = [
    {"room_id": "R001", "room_name": "Living Room", "sensors": [
        {"sensor_id": "MOT-001", "sensor_type": "MOTION", "model": "PIR-200"},
        {"sensor_id": "TMP-001", "sensor_type": "TEMPERATURE", "model": "TH-100"},
    ]},
    {"room_id": "R002", "room_name": "Kitchen", "sensors": [
        {"sensor_id": "SMK-001", "sensor_type": "SMOKE", "model": "SD-9"},
        {"sensor_id": "GAS-001", "sensor_type": "GAS", "model": "GL-7"},
        {"sensor_id": "TMP-002", "sensor_type": "TEMPERATURE", "model": "TH-100"},
    ]},
    {"room_id": "R003", "room_name": "Master Bedroom", "sensors": [
        {"sensor_id": "MOT-002", "sensor_type": "MOTION", "model": "PIR-200"},
        {"sensor_id": "TMP-003", "sensor_type": "TEMPERATURE", "model": "TH-100"},
    ]},
    {"room_id": "R004", "room_name": "Front Door", "sensors": [
        {"sensor_id": "DW-001", "sensor_type": "DOOR_WINDOW", "model": "DWS-3"},
        {"sensor_id": "MOT-003", "sensor_type": "MOTION", "model": "PIR-200"},
    ]},
    {"room_id": "R005", "room_name": "Garage", "sensors": [
        {"sensor_id": "DW-002", "sensor_type": "DOOR_WINDOW", "model": "DWS-3"},
        {"sensor_id": "GAS-002", "sensor_type": "GAS", "model": "GL-7"},
    ]},
    {"room_id": "R006", "room_name": "Backyard", "sensors": [
        {"sensor_id": "MOT-004", "sensor_type": "MOTION", "model": "PIR-200"},
    ]},
]

# Flat lookup of every sensor, used when picking a random one to fire next.
ALL_SENSORS = [
    {"room_id": room["room_id"], "room_name": room["room_name"], **sensor}
    for room in ROOMS for sensor in room["sensors"]
]

FIRMWARE_VERSIONS = ["v1.0.4", "v1.3.1", "v2.0.0"]

# Whether the system is currently "armed". Motion and door/window events are
# only escalated to alerts while armed, same as a real home security panel.
SYSTEM_ARMED = True


def publish_network(client) -> None:
    """Send the house/room/sensor topology to Neo4j (safe to re-send; MERGE dedupes it)."""
    for room in ROOMS:
        for sensor in room["sensors"]:
            node = {
                "house_id": HOUSE["house_id"],
                "house_name": HOUSE["house_name"],
                "address": HOUSE["address"],
                "room_id": room["room_id"],
                "room_name": room["room_name"],
                "sensor_id": sensor["sensor_id"],
                "sensor_type": sensor["sensor_type"],
                "model": sensor["model"],
            }
            client.publish(TOPIC_NETWORK, json.dumps(node))
    print("Published house/room/sensor topology -> Neo4j")


def create_event(sensor: dict) -> dict:
    """One reading from the given sensor. Goes to MySQL, which auto-generates event_id."""
    sensor_type = sensor["sensor_type"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    event = {
        "house_id": HOUSE["house_id"],
        "room_id": sensor["room_id"],
        "sensor_id": sensor["sensor_id"],
        "sensor_type": sensor_type,
        "numeric_value": None,
        "state_value": None,
        "unit": None,
        "timestamp": now,
    }

    if sensor_type == "MOTION":
        event["state_value"] = random.choices(["CLEAR", "MOTION"], weights=[85, 15])[0]
    elif sensor_type == "DOOR_WINDOW":
        event["state_value"] = random.choices(["CLOSED", "OPEN"], weights=[80, 20])[0]
    elif sensor_type == "SMOKE":
        event["numeric_value"] = round(random.uniform(0, 450), 1)
        event["unit"] = "ppm"
    elif sensor_type == "GAS":
        event["numeric_value"] = round(random.uniform(0, 900), 1)
        event["unit"] = "ppm"
    elif sensor_type == "TEMPERATURE":
        event["numeric_value"] = round(random.uniform(16.0, 65.0), 1)
        event["unit"] = "C"

    return event


def build_telemetry(event: dict) -> dict:
    """Raw device message for MongoDB: the full event plus flexible device metadata."""
    document = dict(event)
    document["armed"] = SYSTEM_ARMED
    document["device"] = {
        "battery_level": random.randint(15, 100),
        "firmware_version": random.choice(FIRMWARE_VERSIONS),
    }
    if random.random() < 0.4:
        document["device"]["signal_strength_dbm"] = random.randint(-95, -35)
    if random.random() < 0.1:
        document["device"]["error_code"] = random.choice(
            ["E07_SENSOR_DRIFT", "E12_LOW_BATTERY", "E19_COMM_TIMEOUT"]
        )
    return document


def build_device_status(sensor: dict) -> dict:
    """Periodic 'health' snapshot for one device: battery + settings, upserted in MongoDB."""
    return {
        "message_type": "status",
        "sensor_id": sensor["sensor_id"],
        "sensor_type": sensor["sensor_type"],
        "room_id": sensor["room_id"],
        "battery_level": random.randint(15, 100),
        "signal_strength_dbm": random.randint(-95, -35),
        "firmware_version": random.choice(FIRMWARE_VERSIONS),
        "settings": {
            "sampling_interval_sec": random.choice([5, 10, 30]),
            "sensitivity": random.choice(["LOW", "MEDIUM", "HIGH"]),
        },
        "last_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


DEVICE_LOG_MESSAGES = [
    ("INFO", "Device heartbeat OK"),
    ("INFO", "Self-test passed"),
    ("WARNING", "Battery below 25 percent"),
    ("WARNING", "Signal strength weak"),
    ("ERROR", "Communication timeout, retrying"),
]


def build_device_log(sensor: dict) -> dict:
    """Occasional free-text log line for one device, a different shape than a status snapshot."""
    level, message = random.choice(DEVICE_LOG_MESSAGES)
    return {
        "message_type": "log",
        "sensor_id": sensor["sensor_id"],
        "room_id": sensor["room_id"],
        "level": level,
        "message": message,
        "logged_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main() -> None:
    client = mqtt.Client(client_id="home_security_publisher")
    client.connect(BROKER_HOST, BROKER_PORT)
    client.loop_start()

    print(f"Connected to MQTT broker at {BROKER_HOST}:{BROKER_PORT}")
    print("Publishing to topics:")
    print(f"  - {TOPIC_NETWORK}        (topology            -> Neo4j)")
    print(f"  - {TOPIC_EVENTS}         (sensor readings      -> MySQL)")
    print(f"  - {TOPIC_TELEMETRY}      (raw telemetry        -> MongoDB)")
    print(f"  - {TOPIC_DEVICE_STATUS}  (battery/settings/log -> MongoDB)")
    print("Press CTRL+C to stop.\n")

    publish_network(client)

    # Cycle through every sensor in a fixed rotation (instead of random.choice
    # each time) so every room gets a reading on a predictable schedule. With
    # 11 sensors and a 2-second interval, each sensor reports roughly every
    # 22 seconds, which keeps per-room charts from having long random gaps.
    sensor_rotation = itertools.cycle(ALL_SENSORS)

    cycle = 0
    try:
        while True:
            sensor = next(sensor_rotation)
            event = create_event(sensor)
            telemetry = build_telemetry(event)

            client.publish(TOPIC_EVENTS, json.dumps(event))
            client.publish(TOPIC_TELEMETRY, json.dumps(telemetry))

            value = event["numeric_value"] if event["numeric_value"] is not None else event["state_value"]
            print(f"Published | Room={event['room_id']} | Sensor={event['sensor_id']} "
                  f"({event['sensor_type']}) | Value={value} -> events (MySQL) + telemetry (MongoDB)")

            # Every few cycles, also publish a device-status snapshot or a log line
            # for a random device, so MongoDB accumulates all three document shapes.
            if cycle % 4 == 0:
                sensor = random.choice(ALL_SENSORS)
                client.publish(TOPIC_DEVICE_STATUS, json.dumps(build_device_status(sensor)))
            elif cycle % 4 == 2:
                sensor = random.choice(ALL_SENSORS)
                client.publish(TOPIC_DEVICE_STATUS, json.dumps(build_device_log(sensor)))

            cycle += 1
            if cycle % 25 == 0:
                publish_network(client)

            time.sleep(2)
    except KeyboardInterrupt:
        print("\nPublisher stopped by user.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
