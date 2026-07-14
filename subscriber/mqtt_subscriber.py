import json
import os
import time
from datetime import datetime
from typing import Optional, Tuple

import mysql.connector
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pymongo import MongoClient

load_dotenv()

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1886"))

# The topic a message arrives on decides which database stores it.
TOPIC_NETWORK = os.getenv("MQTT_TOPIC_NETWORK", "home/network")
TOPIC_EVENTS = os.getenv("MQTT_TOPIC_EVENTS", "home/events")
TOPIC_TELEMETRY = os.getenv("MQTT_TOPIC_TELEMETRY", "home/telemetry")
TOPIC_DEVICE_STATUS = os.getenv("MQTT_TOPIC_DEVICE_STATUS", "home/device_status")

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3311")),
    "user": os.getenv("MYSQL_USER", "homeuser"),
    "password": os.getenv("MYSQL_PASSWORD", "homepass"),
    "database": os.getenv("MYSQL_DATABASE", "home_security"),
}

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:rootpass@localhost:27021/")
MONGODB_DB = os.getenv("MONGODB_DB", "home_security")
COLLECTION_TELEMETRY = os.getenv("MONGODB_TELEMETRY_COLLECTION", "device_messages")
COLLECTION_STATUS = os.getenv("MONGODB_STATUS_COLLECTION", "device_status")
COLLECTION_LOGS = os.getenv("MONGODB_LOGS_COLLECTION", "device_logs")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7690")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")

mysql_connection = mysql.connector.connect(**MYSQL_CONFIG)
mongo_client = MongoClient(MONGODB_URI)
mongo_db = mongo_client[MONGODB_DB]
telemetry_collection = mongo_db[COLLECTION_TELEMETRY]
status_collection = mongo_db[COLLECTION_STATUS]
logs_collection = mongo_db[COLLECTION_LOGS]
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# -------------------- Performance metrics --------------------
PERF = {
    "MySQL":   {"count": 0, "total_ms": 0.0},
    "MongoDB": {"count": 0, "total_ms": 0.0},
    "Neo4j":   {"count": 0, "total_ms": 0.0},
}
FIRST_MESSAGE_TIME = None
ERROR_COUNT = 0


def classify_event(sensor_type: str, numeric_value: Optional[float],
                    state_value: Optional[str]) -> Tuple[str, Optional[str]]:
    """Classify one sensor reading and return (status, alert_type). The
    human-readable message/severity for each alert_type lives once in the
    MySQL alert_types table, not duplicated here."""
    if sensor_type == "MOTION":
        if state_value == "MOTION":
            return "DANGER", "MOTION_WHILE_ARMED"
        return "NORMAL", None

    if sensor_type == "DOOR_WINDOW":
        if state_value == "OPEN":
            return "WARNING", "DOOR_OPENED_WHILE_ARMED"
        return "NORMAL", None

    if sensor_type == "SMOKE":
        if numeric_value >= 300:
            return "DANGER", "HIGH_SMOKE"
        if numeric_value >= 100:
            return "WARNING", "SMOKE_DETECTED"
        return "NORMAL", None

    if sensor_type == "GAS":
        if numeric_value >= 600:
            return "DANGER", "HIGH_GAS"
        if numeric_value >= 300:
            return "WARNING", "GAS_LEAK"
        return "NORMAL", None

    if sensor_type == "TEMPERATURE":
        if numeric_value >= 50:
            return "DANGER", "FIRE_RISK_TEMPERATURE"
        if numeric_value >= 30:
            return "WARNING", "HIGH_TEMPERATURE"
        return "NORMAL", None

    return "NORMAL", None


# -------------------- MySQL: structured sensor events + alerts --------------------
def insert_mysql(data: dict) -> Tuple[int, str]:
    """Insert the sensor event (auto-increment event_id) and, if needed, the
    alert. Returns (event_id, status)."""
    status, alert_type = classify_event(
        data["sensor_type"], data["numeric_value"], data["state_value"]
    )

    cursor = mysql_connection.cursor()
    cursor.execute(
        """
        INSERT INTO sensor_events
        (house_id, room_id, sensor_id, sensor_type, numeric_value, state_value, unit, event_time, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (data["house_id"], data["room_id"], data["sensor_id"], data["sensor_type"],
         data["numeric_value"], data["state_value"], data["unit"], data["timestamp"], status),
    )
    event_id = cursor.lastrowid  # MySQL generated the id

    if alert_type:
        cursor.execute(
            """
            INSERT INTO security_alerts
            (event_id, house_id, room_id, alert_type, alert_time)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (event_id, data["house_id"], data["room_id"], alert_type, data["timestamp"]),
        )

    mysql_connection.commit()
    cursor.close()
    return event_id, status


# -------------------- MongoDB: raw telemetry, device status, device logs --------------------
def insert_telemetry(data: dict) -> None:
    """Store the raw device message exactly as it arrived (unprocessed)."""
    document = dict(data)
    document["stored_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    telemetry_collection.insert_one(document)


def upsert_device_status(data: dict) -> None:
    """Keep one 'current health' document per device: battery, signal, settings."""
    status_collection.update_one(
        {"sensor_id": data["sensor_id"]},
        {"$set": data},
        upsert=True,
    )


def insert_device_log(data: dict) -> None:
    """Append a free-text log line for a device. A different shape than a status doc."""
    logs_collection.insert_one(data)


# -------------------- Neo4j: house / room / sensor topology + security events --------------------
def insert_neo4j_network(data: dict) -> None:
    """Build the topology graph: House -> Room -> Sensor."""
    with neo4j_driver.session() as session:
        session.run(
            """
            MERGE (house:House {house_id: $house_id})
            SET house.name = $house_name, house.address = $address
            MERGE (room:Room {room_id: $room_id})
            SET room.name = $room_name
            MERGE (sensor:Sensor {sensor_id: $sensor_id})
            SET sensor.sensor_type = $sensor_type, sensor.model = $model
            MERGE (house)-[:HAS_ROOM]->(room)
            MERGE (room)-[:HAS_SENSOR]->(sensor)
            """,
            house_id=data["house_id"], house_name=data["house_name"], address=data["address"],
            room_id=data["room_id"], room_name=data["room_name"],
            sensor_id=data["sensor_id"], sensor_type=data["sensor_type"], model=data["model"],
        )


def insert_neo4j_security_event(data: dict, alert_type: str) -> None:
    """Record a security event node and link it to the sensor that triggered
    it and the room it happened in, so the graph can answer questions like
    'which sensors have triggered the most alerts' or 'which rooms are riskiest'."""
    with neo4j_driver.session() as session:
        session.run(
            """
            MATCH (sensor:Sensor {sensor_id: $sensor_id})
            MATCH (room:Room {room_id: $room_id})
            CREATE (event:SecurityEvent {
                alert_type: $alert_type,
                event_time: $event_time
            })
            MERGE (sensor)-[:TRIGGERED]->(event)
            MERGE (event)-[:OCCURRED_IN]->(room)
            """,
            sensor_id=data["sensor_id"], room_id=data["room_id"],
            alert_type=alert_type, event_time=data["timestamp"],
        )


def record(target: str, elapsed_ms: float) -> None:
    """Accumulate timing for the performance summary."""
    PERF[target]["count"] += 1
    PERF[target]["total_ms"] += elapsed_ms


def route_message(topic: str, payload: bytes) -> None:
    """Send the message to the right database BASED ON ITS TOPIC, timing each write."""
    global FIRST_MESSAGE_TIME
    if FIRST_MESSAGE_TIME is None:
        FIRST_MESSAGE_TIME = time.time()

    data = json.loads(payload.decode("utf-8"))

    if topic == TOPIC_EVENTS:
        start = time.perf_counter()
        event_id, status = insert_mysql(data)
        ms = (time.perf_counter() - start) * 1000
        record("MySQL", ms)
        value = data["numeric_value"] if data["numeric_value"] is not None else data["state_value"]
        print(f"[MySQL  ] event #{event_id} | Room={data['room_id']} | Sensor={data['sensor_id']} "
              f"({data['sensor_type']}) | Value={value} | Status={status} | {ms:.1f} ms")

        # If this reading raised an alert, also record it in Neo4j as a
        # SecurityEvent linked to the sensor and room, so the graph and the
        # relational database agree on what happened.
        _, alert_type = classify_event(data["sensor_type"], data["numeric_value"], data["state_value"])
        if alert_type:
            start = time.perf_counter()
            insert_neo4j_security_event(data, alert_type)
            ms = (time.perf_counter() - start) * 1000
            record("Neo4j", ms)
            print(f"[Neo4j  ] security event | {data['sensor_id']} -[TRIGGERED]-> "
                  f"{alert_type} -[OCCURRED_IN]-> {data['room_id']} | {ms:.1f} ms")

    elif topic == TOPIC_TELEMETRY:
        start = time.perf_counter()
        insert_telemetry(data)
        ms = (time.perf_counter() - start) * 1000
        record("MongoDB", ms)
        extra = [k for k in ("error_code", "signal_strength_dbm") if k in data.get("device", {})]
        extra_note = f" | extra={','.join(extra)}" if extra else ""
        print(f"[MongoDB] Sensor={data['sensor_id']} | raw telemetry stored | {ms:.1f} ms{extra_note}")

    elif topic == TOPIC_DEVICE_STATUS:
        start = time.perf_counter()
        if data.get("message_type") == "log":
            insert_device_log(data)
            note = "log line"
        else:
            upsert_device_status(data)
            note = "status snapshot"
        ms = (time.perf_counter() - start) * 1000
        record("MongoDB", ms)
        print(f"[MongoDB] Sensor={data['sensor_id']} | {note} stored | {ms:.1f} ms")

    elif topic == TOPIC_NETWORK:
        start = time.perf_counter()
        insert_neo4j_network(data)
        ms = (time.perf_counter() - start) * 1000
        record("Neo4j", ms)
        print(f"[Neo4j  ] topology | {data['sensor_id']} -> {data['room_id']} "
              f"-> {data['house_id']} | {ms:.1f} ms")

    else:
        print(f"Unknown topic '{topic}', message ignored.")


def print_performance_summary() -> None:
    """Print processing speed, per-database latency, and reliability."""
    total = sum(db["count"] for db in PERF.values())
    elapsed = (time.time() - FIRST_MESSAGE_TIME) if FIRST_MESSAGE_TIME else 0.0

    print("\n" + "=" * 60)
    print("PERFORMANCE SUMMARY")
    print("=" * 60)
    print(f"Run time:          {elapsed:.1f} s")
    print(f"Messages stored:   {total}  "
          f"(MySQL {PERF['MySQL']['count']}, "
          f"MongoDB {PERF['MongoDB']['count']}, "
          f"Neo4j {PERF['Neo4j']['count']})")
    if elapsed > 0:
        print(f"Throughput:        {total / elapsed:.2f} messages/sec")
    print("Avg write latency:")
    for name, db in PERF.items():
        avg = (db["total_ms"] / db["count"]) if db["count"] else 0.0
        print(f"   - {name:<8} {avg:6.2f} ms  ({db['count']} writes)")
    print(f"Errors:            {ERROR_COUNT}")
    print("=" * 60)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
        for topic in (TOPIC_NETWORK, TOPIC_EVENTS, TOPIC_TELEMETRY, TOPIC_DEVICE_STATUS):
            client.subscribe(topic)
            print(f"Subscribed to topic: {topic}")
        print()
    else:
        print(f"Connection failed with code {rc}")


def on_message(client, userdata, msg):
    global ERROR_COUNT
    try:
        route_message(msg.topic, msg.payload)
    except Exception as error:
        ERROR_COUNT += 1
        print(f"Error processing message on '{msg.topic}': {error}")


def main() -> None:
    client = mqtt.Client(client_id="home_security_subscriber")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)

    print("Subscriber is running. Press CTRL+C to stop.")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nSubscriber stopped by user.")
    finally:
        print_performance_summary()
        mysql_connection.close()
        mongo_client.close()
        neo4j_driver.close()
        client.disconnect()


if __name__ == "__main__":
    main()
