import os

import pandas as pd
import streamlit as st
import mysql.connector
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pymongo import MongoClient

load_dotenv()

# Ports are offset so this stack can run alongside other class projects.
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


# -------------------- Connections --------------------
@st.cache_resource
def get_mongo_db():
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
    return client[MONGODB_DB]


@st.cache_resource
def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def fetch_mysql(query, params=None):
    """Run a query against MySQL and return the rows as a DataFrame."""
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        rows = cursor.fetchall()
        cursor.close()
    finally:
        connection.close()
    return pd.DataFrame(rows)


def call_mysql_procedure(procedure, args):
    """Call a stored procedure and return its first result set as a DataFrame."""
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.callproc(procedure, args)
        rows = []
        for result in cursor.stored_results():
            rows = result.fetchall()
        cursor.close()
    finally:
        connection.close()
    return pd.DataFrame(rows)


# -------------------- Connection status --------------------
def mysql_status():
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        connection.close()
        return True, "Connected"
    except Exception as error:
        return False, str(error)


def mongo_status():
    try:
        get_mongo_db().client.admin.command("ping")
        return True, "Connected"
    except Exception as error:
        return False, str(error)


def neo4j_status():
    try:
        get_neo4j_driver().verify_connectivity()
        return True, "Connected"
    except Exception as error:
        return False, str(error)


# -------------------- Data loaders --------------------
def load_events(limit):
    return fetch_mysql(
        "SELECT event_id, house_id, room_id, sensor_id, sensor_type, numeric_value, "
        "state_value, unit, event_time, status "
        "FROM sensor_events ORDER BY event_id DESC LIMIT %s",
        (limit,),
    )


def load_status_counts():
    return fetch_mysql(
        "SELECT status, COUNT(*) AS total FROM sensor_events GROUP BY status"
    )


def load_alerts(limit):
    return fetch_mysql(
        "SELECT alert_id, alert_time, house_id, room_id, alert_type, message, severity, "
        "event_id, sensor_id, sensor_type, numeric_value, state_value "
        "FROM v_alert_details ORDER BY alert_id DESC LIMIT %s",
        (limit,),
    )


def load_room_ids():
    df = fetch_mysql("SELECT DISTINCT room_id FROM sensor_events ORDER BY room_id")
    return df["room_id"].tolist() if not df.empty else []


def load_room_series(room_id, limit):
    df = fetch_mysql(
        "SELECT event_time, sensor_type, numeric_value "
        "FROM sensor_events WHERE room_id = %s AND numeric_value IS NOT NULL "
        "ORDER BY event_id DESC LIMIT %s",
        (room_id, limit),
    )
    if df.empty:
        return df
    df["event_time"] = pd.to_datetime(df["event_time"])
    return df.sort_values("event_time")


def load_room_report(room_id):
    return call_mysql_procedure("sp_room_security_report", [room_id])


def load_telemetry(limit):
    documents = list(get_mongo_db()[COLLECTION_TELEMETRY].find().sort("_id", -1).limit(limit))
    for document in documents:
        document["_id"] = str(document["_id"])
    return documents


def load_device_status():
    documents = list(get_mongo_db()[COLLECTION_STATUS].find())
    for document in documents:
        document["_id"] = str(document["_id"])
    return documents


def load_device_logs(limit):
    documents = list(get_mongo_db()[COLLECTION_LOGS].find().sort("_id", -1).limit(limit))
    for document in documents:
        document["_id"] = str(document["_id"])
    return documents


def load_topology():
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (house:House)-[:HAS_ROOM]->(room:Room)-[:HAS_SENSOR]->(sensor:Sensor)
            RETURN house.house_id AS house_id, house.name AS house_name,
                   room.room_id AS room_id, room.name AS room_name,
                   sensor.sensor_id AS sensor_id, sensor.sensor_type AS sensor_type,
                   sensor.model AS model
            ORDER BY room_id, sensor_id
            """
        )
        return pd.DataFrame([record.data() for record in result])


def load_graph_counts():
    driver = get_neo4j_driver()
    with driver.session() as session:
        nodes = session.run(
            "MATCH (n) UNWIND labels(n) AS label "
            "RETURN label, count(*) AS count ORDER BY count DESC"
        )
        node_df = pd.DataFrame([record.data() for record in nodes])
        relationships = session.run(
            "MATCH ()-[r]->() RETURN type(r) AS relationship, count(*) AS count "
            "ORDER BY count DESC"
        )
        rel_df = pd.DataFrame([record.data() for record in relationships])
    return node_df, rel_df


def load_riskiest_rooms():
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (event:SecurityEvent)-[:OCCURRED_IN]->(room:Room)
            RETURN room.room_id AS room_id, room.name AS room_name, count(event) AS security_events
            ORDER BY security_events DESC
            """
        )
        return pd.DataFrame([record.data() for record in result])


# -------------------- Page --------------------
st.set_page_config(page_title="Home Security IoT Monitoring", layout="wide")

st.sidebar.title("Home Security")
st.sidebar.caption("Smart-home IoT monitoring dashboard")
row_limit = st.sidebar.slider("Rows to load", min_value=10, max_value=500, value=50, step=10)
if st.sidebar.button("Refresh"):
    st.rerun()

st.title("Home Security IoT Monitoring System")

# Connection status
col1, col2, col3 = st.columns(3)
for column, name, checker in (
    (col1, "MySQL", mysql_status),
    (col2, "MongoDB", mongo_status),
    (col3, "Neo4j", neo4j_status),
):
    ok, message = checker()
    with column:
        if ok:
            st.success(f"{name}: {message}")
        else:
            st.error(f"{name}: not connected")

st.divider()

# Overview metrics
try:
    status_counts = load_status_counts()
    counts = {row["status"]: int(row["total"]) for _, row in status_counts.iterrows()}
    total = sum(counts.values())
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total sensor events", total)
    m2.metric("Normal", counts.get("NORMAL", 0))
    m3.metric("Warning", counts.get("WARNING", 0))
    m4.metric("Danger", counts.get("DANGER", 0))
except Exception as error:
    st.warning(f"Could not load overview from MySQL: {error}")

st.divider()

# MySQL sensor events and charts
st.header("Sensor events (MySQL)")
try:
    events = load_events(row_limit)
    st.dataframe(events, use_container_width=True)

    room_ids = load_room_ids()
    if room_ids:
        selected_room = st.selectbox("Numeric readings over time for room", room_ids)
        series = load_room_series(selected_room, row_limit)
        if not series.empty:
            pivot = series.pivot_table(index="event_time", columns="sensor_type", values="numeric_value")
            st.caption("Smoke (ppm) / Gas (ppm) / Temperature (C)")
            st.line_chart(pivot)

            st.caption(f"Stored-procedure report for {selected_room} (sp_room_security_report)")
            st.dataframe(load_room_report(selected_room), use_container_width=True)
        else:
            st.info("No numeric readings yet for this room.")
except Exception as error:
    st.warning(f"Could not load sensor events from MySQL: {error}")

st.divider()

# Alerts
st.header("Security alerts")
try:
    alerts = load_alerts(row_limit)
    if alerts.empty:
        st.info("No alerts recorded yet.")
    else:
        st.dataframe(alerts, use_container_width=True)
except Exception as error:
    st.warning(f"Could not load alerts from MySQL: {error}")

st.divider()

# MongoDB
st.header("Device data (MongoDB)")
tab1, tab2, tab3 = st.tabs(["Raw telemetry", "Device status", "Device logs"])

with tab1:
    try:
        telemetry = load_telemetry(row_limit)
        if not telemetry:
            st.info("No telemetry documents yet.")
        else:
            st.caption(f"Showing latest {len(telemetry)} raw MQTT messages")
            st.json(telemetry, expanded=False)
    except Exception as error:
        st.warning(f"Could not load telemetry from MongoDB: {error}")

with tab2:
    try:
        status_docs = load_device_status()
        if not status_docs:
            st.info("No device status documents yet.")
        else:
            st.dataframe(pd.json_normalize(status_docs), use_container_width=True)
    except Exception as error:
        st.warning(f"Could not load device status from MongoDB: {error}")

with tab3:
    try:
        logs = load_device_logs(row_limit)
        if not logs:
            st.info("No device logs yet.")
        else:
            st.dataframe(pd.DataFrame(logs), use_container_width=True)
    except Exception as error:
        st.warning(f"Could not load device logs from MongoDB: {error}")

st.divider()

# Neo4j topology
st.header("Topology and relationships (Neo4j)")
try:
    node_df, rel_df = load_graph_counts()
    left, right = st.columns(2)
    with left:
        st.subheader("Nodes")
        st.dataframe(node_df, use_container_width=True)
    with right:
        st.subheader("Relationships")
        st.dataframe(rel_df, use_container_width=True)

    st.subheader("House -> Room -> Sensor")
    st.dataframe(load_topology(), use_container_width=True)

    st.subheader("Riskiest rooms (by security events triggered)")
    st.dataframe(load_riskiest_rooms(), use_container_width=True)
except Exception as error:
    st.warning(f"Could not load topology from Neo4j: {error}")
