# Home Security IoT Monitoring System

Simulated home security sensors, routed by MQTT topic into three databases chosen to match the shape of each message, viewed through a Streamlit dashboard.

## Overview

A Python publisher simulates sensors (motion, door/window, smoke, gas, temperature) spread across 6 rooms in one house. Readings are published over MQTT and split by topic into three databases:

| Topic | Destination | Content |
|---|---|---|
| `home/network` | Neo4j | House/room/sensor topology + security events |
| `home/events` | MySQL | Structured sensor readings and alerts |
| `home/telemetry` | MongoDB | Raw MQTT message per event (`device_messages`) |
| `home/device_status` | MongoDB | Battery/settings snapshots (`device_status`) and log lines (`device_logs`) |

## Tech Stack

- **Python** — publisher, subscriber, dashboard (paho-mqtt, mysql-connector-python, pymongo, neo4j driver)
- **Eclipse Mosquitto** — MQTT broker
- **MySQL 8.0** — sensor events, alerts, one view, one trigger, one stored procedure
- **MongoDB 7** — device_messages, device_status, device_logs collections
- **Neo4j 5** — House → Room → Sensor → SecurityEvent graph
- **Docker Compose** — runs the broker and all three databases
- **Streamlit** — dashboard reading from all three databases

Ports are offset from common defaults (MQTT 1886, MySQL 3311, MongoDB 27021, Neo4j 7477/7690) so this stack can run alongside other Docker projects on the same machine.

## Project Structure

```
home_security/
├── database/
│   └── mysql_schema.sql
├── mosquitto/
│   └── config/
│       └── mosquitto.conf
├── publisher/
│   └── sensor_publisher.py
├── subscriber/
│   └── mqtt_subscriber.py
├── dashboard/
│   └── app.py
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── requirements_dashboard.txt
└── PRESENTATION_COMMANDS.md
```

## Setup

### 1. Start the databases and broker

```powershell
docker compose up -d
docker compose ps -a
```

Expected containers: `home_mqtt_broker`, `home_mysql_db`, `home_mongodb_db`, `home_neo4j_db`.

### 2. Create a virtual environment and install requirements

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and adjust if needed.

## Running the Pipeline

Open separate terminals for each of the following:

**Subscriber**
```powershell
.venv\Scripts\activate
python subscriber\mqtt_subscriber.py
```

**Publisher**
```powershell
.venv\Scripts\activate
python publisher\sensor_publisher.py
```

**Dashboard**
```powershell
.venv\Scripts\activate
python -m pip install -r requirements_dashboard.txt
streamlit run dashboard\app.py
```

Then open `http://localhost:8501`.

## Checking the Databases Directly

**MySQL**
```powershell
docker exec -it home_mysql_db mysql -u homeuser -phomepass home_security
```

**MongoDB**
```powershell
docker exec -it home_mongodb_db mongosh -u root -p rootpass --authenticationDatabase admin
```

**Neo4j Browser**

Open `http://localhost:7477` (user `neo4j`, password `password123`).

Full example queries for all three databases are in `PRESENTATION_COMMANDS.md`.

## Database Design Highlights

- **MySQL** — `sensor_events` stores one row per reading; `security_alerts` references events and looks up messages/severity from `alert_types`. Includes a view (`v_alert_details`), a trigger (`trg_log_danger`) that logs DANGER readings into `critical_events`, and a stored procedure (`sp_room_security_report`) for per-room summaries.
- **MongoDB** — three collections cover three different message shapes: raw telemetry, latest device status (upserted), and append-only log lines.
- **Neo4j** — topology is rebuilt with `MERGE` so re-publishing updates the same graph rather than duplicating nodes. Triggered alerts also create `SecurityEvent` nodes linked to the sensor and room involved.

## Stopping the Stack

```powershell
docker compose down       # stop, keep data
docker compose down -v    # stop and wipe all data
```
