CREATE DATABASE IF NOT EXISTS home_security;
USE home_security;

-- Every sensor reading from every sensor type lands here. Motion and
-- door/window sensors use state_value (e.g. 'MOTION'/'CLEAR', 'OPEN'/'CLOSED');
-- smoke, gas, and temperature sensors use numeric_value. Only one of the two
-- is filled in per row, which is why both columns are nullable.
CREATE TABLE IF NOT EXISTS sensor_events (
    event_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    house_id     VARCHAR(20) NOT NULL,
    room_id      VARCHAR(20) NOT NULL,
    sensor_id    VARCHAR(20) NOT NULL,
    sensor_type  VARCHAR(20) NOT NULL,   -- MOTION, DOOR_WINDOW, SMOKE, GAS, TEMPERATURE
    numeric_value DECIMAL(8,2),          -- used by SMOKE (ppm), GAS (ppm), TEMPERATURE (C)
    state_value   VARCHAR(20),           -- used by MOTION (MOTION/CLEAR), DOOR_WINDOW (OPEN/CLOSED)
    unit          VARCHAR(10),
    event_time    DATETIME,
    status        VARCHAR(10)            -- NORMAL, WARNING, DANGER
);

-- One readable message (and severity) per alert_type, kept in one place and
-- reused by every alert instead of being retyped each time.
CREATE TABLE IF NOT EXISTS alert_types (
    alert_type      VARCHAR(40) PRIMARY KEY,
    default_message VARCHAR(255),
    severity        VARCHAR(10)
);

INSERT IGNORE INTO alert_types (alert_type, default_message, severity) VALUES
    ('MOTION_WHILE_ARMED',       'Motion detected while system armed',        'DANGER'),
    ('DOOR_OPENED_WHILE_ARMED',  'Door or window opened while system armed',  'WARNING'),
    ('FORCED_ENTRY',             'Possible forced entry detected',            'DANGER'),
    ('SMOKE_DETECTED',           'Smoke detected',                            'WARNING'),
    ('HIGH_SMOKE',               'Smoke level critically high',               'DANGER'),
    ('GAS_LEAK',                 'Gas concentration above safe threshold',    'WARNING'),
    ('HIGH_GAS',                 'Gas concentration critically high',         'DANGER'),
    ('HIGH_TEMPERATURE',         'Temperature above normal range',            'WARNING'),
    ('FIRE_RISK_TEMPERATURE',    'Temperature indicates possible fire risk',  'DANGER');

-- Alerts store only the event and alert_type; the message/severity comes
-- from alert_types so it only has to be maintained in one place.
CREATE TABLE IF NOT EXISTS security_alerts (
    alert_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id   BIGINT NOT NULL,
    house_id   VARCHAR(20) NOT NULL,
    room_id    VARCHAR(20) NOT NULL,
    alert_type VARCHAR(40) NOT NULL,
    alert_time DATETIME,
    FOREIGN KEY (event_id) REFERENCES sensor_events(event_id),
    FOREIGN KEY (alert_type) REFERENCES alert_types(alert_type)
);

-- Audit table filled automatically by the trigger below, independent of the
-- application code. This is useful evidence for the oral defense: it proves
-- the DANGER-level logging happens inside the database, not just in Python.
CREATE TABLE IF NOT EXISTS critical_events (
    log_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id      BIGINT NOT NULL,
    house_id      VARCHAR(20) NOT NULL,
    room_id       VARCHAR(20) NOT NULL,
    sensor_type   VARCHAR(20),
    numeric_value DECIMAL(8,2),
    state_value   VARCHAR(20),
    logged_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- View: reconstruct each alert with its human-readable message/severity and
-- the sensor reading that triggered it, in one query.
CREATE OR REPLACE VIEW v_alert_details AS
SELECT
    a.alert_id,
    a.alert_time,
    a.house_id,
    a.room_id,
    a.alert_type,
    t.default_message AS message,
    t.severity,
    e.event_id,
    e.sensor_id,
    e.sensor_type,
    e.numeric_value,
    e.state_value,
    e.status
FROM security_alerts a
JOIN alert_types   t ON a.alert_type = t.alert_type
JOIN sensor_events e ON a.event_id   = e.event_id;

-- Trigger: every DANGER-level reading is copied into the audit table
-- automatically, the moment it is inserted, regardless of which script wrote it.
DELIMITER $$
CREATE TRIGGER trg_log_danger
AFTER INSERT ON sensor_events
FOR EACH ROW
BEGIN
    IF NEW.status = 'DANGER' THEN
        INSERT INTO critical_events (event_id, house_id, room_id, sensor_type, numeric_value, state_value)
        VALUES (NEW.event_id, NEW.house_id, NEW.room_id, NEW.sensor_type, NEW.numeric_value, NEW.state_value);
    END IF;
END$$
DELIMITER ;

-- Stored procedure: full security report for one room (counts by status,
-- per-sensor-type breakdown, and total alerts raised).
DELIMITER $$
CREATE PROCEDURE sp_room_security_report(IN p_room_id VARCHAR(20))
BEGIN
    SELECT
        p_room_id                                          AS room_id,
        COUNT(*)                                            AS total_events,
        SUM(status = 'NORMAL')                              AS normal_count,
        SUM(status = 'WARNING')                             AS warning_count,
        SUM(status = 'DANGER')                              AS danger_count,
        SUM(sensor_type = 'MOTION')                         AS motion_events,
        SUM(sensor_type = 'DOOR_WINDOW')                    AS door_window_events,
        SUM(sensor_type = 'SMOKE')                          AS smoke_events,
        SUM(sensor_type = 'GAS')                            AS gas_events,
        SUM(sensor_type = 'TEMPERATURE')                    AS temperature_events,
        (SELECT COUNT(*) FROM security_alerts WHERE room_id = p_room_id) AS total_alerts
    FROM sensor_events
    WHERE room_id = p_room_id;
END$$
DELIMITER ;
