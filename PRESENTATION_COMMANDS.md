# Home Security IoT Monitoring System — Commands

Topic-based routing:

```text
home/network        -> Neo4j    -> house/room/sensor topology + security events
home/events          -> MySQL   -> structured sensor readings and alerts
home/telemetry       -> MongoDB -> raw MQTT message per event (device_messages)
home/device_status   -> MongoDB -> battery/settings snapshots (device_status) and log lines (device_logs)
```

Sensors simulated: motion, door/window, smoke, gas leak, temperature — spread
across 6 rooms in one house (see `publisher/sensor_publisher.py`, `ROOMS`).

Ports are offset (MQTT 1886, MySQL 3311, MongoDB 27021, Neo4j 7690/7477) so
this stack can run at the same time as other projects on the same machine.

---

# 1. Start Docker containers

```powershell
docker compose up -d
docker compose ps -a
```

Expected containers:

```text
home_mqtt_broker
home_mysql_db
home_mongodb_db
home_neo4j_db
```

Stop (keep data) / full reset:

```powershell
docker compose down
docker compose down -v
```

---

# 2. Virtual environment and requirements

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

---

# 3. Run the subscriber (Terminal 1)

```powershell
.venv\Scripts\activate
python subscriber\mqtt_subscriber.py
```

Expected:

```text
Subscriber is running. Press CTRL+C to stop.
Connected to MQTT broker at localhost:1886
Subscribed to topic: home/network
Subscribed to topic: home/events
Subscribed to topic: home/telemetry
Subscribed to topic: home/device_status
```

---

# 4. Run the publisher (Terminal 2)

```powershell
.venv\Scripts\activate
python publisher\sensor_publisher.py
```

---

# 5. Check MySQL (Terminal 3)

```powershell
docker exec -it home_mysql_db mysql -u homeuser -phomepass home_security
```

```sql
SHOW TABLES;

SHOW TABLES;


SELECT * FROM alert_types ORDER BY severity;


SELECT room_id, COUNT(*) AS total_events
FROM sensor_events
GROUP BY room_id
ORDER BY total_events DESC;


SELECT c.log_id, c.room_id, c.sensor_type, c.numeric_value, c.state_value,
       e.unit, c.logged_at
FROM critical_events c
JOIN sensor_events e ON c.event_id = e.event_id
ORDER BY c.logged_at DESC LIMIT 10;

CALL sp_room_security_report('R004');

SELECT a.alert_id, a.event_id, a.room_id, a.alert_type,
       t.default_message AS message, t.severity, a.alert_time
FROM security_alerts a
JOIN alert_types t ON a.alert_type = t.alert_type
ORDER BY a.alert_id DESC LIMIT 10;
```

Advanced objects (view, trigger, stored procedure):

```sql
SHOW FULL TABLES WHERE table_type = 'VIEW';
SHOW TRIGGERS;
SHOW PROCEDURE STATUS WHERE db = 'home_security';

SELECT * FROM v_alert_details ORDER BY alert_id DESC LIMIT 5;
SELECT * FROM critical_events ORDER BY log_id DESC LIMIT 5;
CALL sp_room_security_report('R002');

exit;
```

---

# 6. Check MongoDB

```powershell
docker exec -it home_mongodb_db mongosh -u root -p rootpass --authenticationDatabase admin
```

```javascript

use home_security
show collections


db.device_logs.find().sort({ _id: -1 }).limit(5).pretty()


db.device_status.find({}, { sensor_id: 1, battery_level: 1, _id: 0 })

db.device_messages.aggregate([
  { $match: { numeric_value: { $ne: null } } },
  { $group: { _id: "$sensor_type", avg_value: { $avg: "$numeric_value" } } }
])


db.device_logs.aggregate([
  { $match: { level: "ERROR" } },
  { $group: { _id: "$sensor_id", errors: { $sum: 1 } } },
  { $sort: { errors: -1 } }
])

exit

```

---

# 7. Check Neo4j

Open http://localhost:7477 (user `neo4j`, password `password123`).

```cypher
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 10;

MATCH (r:Room {room_id: 'R001'})
CREATE (s:Sensor {sensor_id: 'SMK-099', sensor_type: 'SMOKE', model: 'SD-9'})
MERGE (r)-[:HAS_SENSOR]->(s)
RETURN r.name AS room, s.sensor_id AS new_sensor;


MATCH (s:Sensor)
RETURN s.sensor_type AS type, count(s) AS total
ORDER BY total DESC;

MATCH (s:Sensor {sensor_id: 'MOT-001'})
MATCH (r:Room {room_id: 'R001'})
CREATE (e:SecurityEvent {alert_type: 'MANUAL_TEST_EVENT', event_time: datetime()})
MERGE (s)-[:TRIGGERED]->(e)
MERGE (e)-[:OCCURRED_IN]->(r)
RETURN s.sensor_id AS sensor, e.alert_type AS alert, r.name AS room;


MATCH (s:Sensor)-[:TRIGGERED]->(e:SecurityEvent)
RETURN s.sensor_type AS sensor_type, e.alert_type AS alert_type, count(*) AS occurrences
ORDER BY occurrences DESC;
```

---

# 8. Run the Streamlit dashboard (Terminal 4)

```powershell
.venv\Scripts\activate
python -m pip install -r requirements_dashboard.txt
streamlit run dashboard\app.py
```

Open http://localhost:8501

The dashboard shows:

- MySQL, MongoDB, and Neo4j connection status
- event totals by status (Normal / Warning / Danger)
- latest structured sensor events from MySQL, with a per-room chart of
  numeric readings (smoke, gas, temperature) and the stored-procedure report
- security alerts joined with their messages/severity through the
  `v_alert_details` view
- raw telemetry, device status snapshots, and device logs from MongoDB
- the house/room/sensor topology and the riskiest rooms from Neo4j

---

