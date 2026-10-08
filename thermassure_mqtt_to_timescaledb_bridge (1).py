#!/usr/bin/env python3

"""
ThermAssure MQTT JSON -> Timescale Cloud Bridge

Flow:

ThermAssure
    |
    v
Little Relay
    |
    v
HiveMQ Cloud
    |
    | thermassure/data
    v
Python MQTT Bridge
    |
    v
Tiger / Timescale Cloud
    |
    v
Grafana Cloud

Supports:
1. Current ThermAssure MQTT JSON v4 format
2. Older ThermAssure JSON formats
3. Samples represented as dictionaries
4. Samples represented as lists
"""

import json
import os
import ssl
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import psycopg2


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# MQTT CONFIGURATION
# ============================================================

MQTT_BROKER = os.getenv(
    "HIVEMQ_HOST",
    "ac1300c0609a45b1b3004f804830381f.s1.eu.hivemq.cloud"
)

MQTT_PORT = int(
    os.getenv("HIVEMQ_PORT", "8883")
)

MQTT_USERNAME = os.getenv(
    "HIVEMQ_USERNAME"
)

MQTT_PASSWORD = os.getenv(
    "HIVEMQ_PASSWORD"
)

MQTT_TOPIC = os.getenv(
    "HIVEMQ_TOPIC",
    "thermassure/data"
)


# ============================================================
# TIGER / TIMESCALE CLOUD CONFIGURATION
# ============================================================

DB_HOST = os.getenv(
    "DB_HOST"
)

DB_PORT = int(
    os.getenv("DB_PORT", "35084")
)

DB_NAME = os.getenv(
    "DB_NAME",
    "tsdb"
)

DB_USER = os.getenv(
    "DB_USER",
    "tsdbadmin"
)

DB_PASSWORD = os.getenv(
    "DB_PASSWORD"
)

DB_TABLE = os.getenv(
    "DB_TABLE",
    "thermassure_realtime"
)


# ============================================================
# THERMASSURE LOCATION
# ============================================================

REGION = os.getenv(
    "THERMASSURE_REGION",
    "Karnataka"
)

CENTRAL = os.getenv(
    "THERMASSURE_CENTRAL",
    "Bengaluru"
)

BAG = os.getenv(
    "THERMASSURE_BAG",
    "Bag-01"
)


# ============================================================
# GLOBAL DATABASE CONNECTION
# ============================================================

db_connection = None


# ============================================================
# CONFIGURATION CHECK
# ============================================================

def check_configuration():

    missing = []

    if not MQTT_USERNAME:
        missing.append("HIVEMQ_USERNAME")

    if not MQTT_PASSWORD:
        missing.append("HIVEMQ_PASSWORD")

    if not DB_HOST:
        missing.append("DB_HOST")

    if not DB_PASSWORD:
        missing.append("DB_PASSWORD")

    if missing:

        print()
        print("=" * 60)
        print("MISSING CONFIGURATION")
        print("=" * 60)

        for variable in missing:
            print(f"Missing: {variable}")

        print()
        print("Please configure the missing environment variables.")
        print()

        raise SystemExit(1)


# ============================================================
# CONVERT DEVICE TIMESTAMP
# ============================================================

def convert_device_timestamp(ts_device_ms, time_valid=True):

    if not time_valid:
        return None

    if ts_device_ms is None:
        return None

    try:

        timestamp_seconds = int(ts_device_ms) / 1000.0

        return datetime.fromtimestamp(
            timestamp_seconds,
            tz=timezone.utc
        )

    except Exception:

        return None


# ============================================================
# PARSE ISO TIMESTAMP
# ============================================================

def parse_timestamp(value):

    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, (int, float)):

        try:

            return datetime.fromtimestamp(
                float(value) / 1000.0,
                tz=timezone.utc
            )

        except Exception:

            return None

    if isinstance(value, str):

        try:

            value = value.strip()

            if value.endswith("Z"):
                value = value[:-1] + "+00:00"

            return datetime.fromisoformat(value)

        except Exception:

            return None

    return None


# ============================================================
# CONNECT TO TIGER / TIMESCALE CLOUD
# ============================================================

def connect_database():

    while True:

        try:

            print()
            print("Connecting to Timescale Cloud...")

            connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                sslmode="require",
                connect_timeout=15
            )

            print("Connected to Timescale Cloud.")

            return connection

        except Exception as error:

            print()
            print("Timescale Cloud connection failed:")
            print(error)

            print()
            print("Retrying in 5 seconds...")

            time.sleep(5)


# ============================================================
# CHECK DATABASE CONNECTION
# ============================================================

def ensure_database_connection():

    global db_connection

    try:

        if db_connection is None:
            db_connection = connect_database()
            create_table(db_connection)
            return

        db_connection.cursor().execute("SELECT 1")

    except Exception:

        print("Database connection lost. Reconnecting...")

        try:
            db_connection.close()
        except Exception:
            pass

        db_connection = connect_database()
        create_table(db_connection)


# ============================================================
# CREATE / PREPARE TABLE
# ============================================================

def create_table(connection):

    if not DB_TABLE.replace("_", "").isalnum():

        raise ValueError(
            "DB_TABLE may contain only letters, numbers and underscores."
        )

    create_query = f"""
        CREATE TABLE IF NOT EXISTS {DB_TABLE} (

            id BIGSERIAL PRIMARY KEY,

            event_id TEXT UNIQUE,

            sample_id BIGINT,

            box_id BIGINT,

            sensor_id TEXT,

            batch_id BIGINT,

            delivery_class TEXT,

            observed_at TIMESTAMPTZ,

            gateway_received_at TIMESTAMPTZ,

            temperature_c DOUBLE PRECISION,

            battery_mv INTEGER,

            excursion BOOLEAN,

            region TEXT,

            central TEXT,

            bag TEXT,

            received_at TIMESTAMPTZ
                NOT NULL DEFAULT NOW()
        );
    """

    with connection.cursor() as cursor:
        cursor.execute(create_query)

    alter_queries = [

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS event_id TEXT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS sample_id BIGINT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS box_id BIGINT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS sensor_id TEXT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS batch_id BIGINT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS delivery_class TEXT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS gateway_received_at TIMESTAMPTZ;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS temperature_c DOUBLE PRECISION;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS battery_mv INTEGER;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS excursion BOOLEAN;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS region TEXT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS central TEXT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS bag TEXT;
        """,

        f"""
        ALTER TABLE {DB_TABLE}
        ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ
            DEFAULT NOW();
        """
    ]

    with connection.cursor() as cursor:

        for query in alter_queries:
            cursor.execute(query)

    # Create indexes

    index_query = f"""
        CREATE INDEX IF NOT EXISTS
        idx_{DB_TABLE}_observed_at
        ON {DB_TABLE}(observed_at);
    """

    with connection.cursor() as cursor:
        cursor.execute(index_query)

    box_index_query = f"""
        CREATE INDEX IF NOT EXISTS
        idx_{DB_TABLE}_box_id
        ON {DB_TABLE}(box_id);
    """

    with connection.cursor() as cursor:
        cursor.execute(box_index_query)

    connection.commit()

    print(f"Table ready: {DB_TABLE}")


# ============================================================
# EXTRACT ONE SAMPLE
# ============================================================

def extract_sample(sample, fields=None):

    # --------------------------------------------------------
    # FORMAT 1: DICTIONARY
    # --------------------------------------------------------

    if isinstance(sample, dict):

        sample_id = (
            sample.get("sample_id")
            or sample.get("id")
        )

        temperature_c = (
            sample.get("temperature_c")
            if sample.get("temperature_c") is not None
            else sample.get("temp_c")
        )

        battery_mv = sample.get(
            "battery_mv"
        )

        excursion = sample.get(
            "excursion"
        )

        sensor_id = sample.get(
            "sensor_id"
        )

        observed_at = parse_timestamp(
            sample.get("observed_at")
        )

        if observed_at is None:

            observed_at = convert_device_timestamp(
                sample.get("ts_device_ms"),
                sample.get("time_valid", True)
            )

        return {
            "sample_id": sample_id,
            "temperature_c": temperature_c,
            "battery_mv": battery_mv,
            "excursion": excursion,
            "sensor_id": sensor_id,
            "observed_at": observed_at
        }


    # --------------------------------------------------------
    # FORMAT 2: LIST
    # --------------------------------------------------------

    if isinstance(sample, list):

        if len(sample) < 6:

            print(
                f"WARNING: Sample list has only "
                f"{len(sample)} values. Skipping."
            )

            return None

        try:

            sample_id = sample[0]

            ts_device_ms = sample[1]

            time_valid = sample[2]

            temperature_c = sample[3]

            battery_mv = sample[4]

            excursion = sample[5]

            observed_at = convert_device_timestamp(
                ts_device_ms,
                time_valid
            )

            return {
                "sample_id": sample_id,
                "temperature_c": temperature_c,
                "battery_mv": battery_mv,
                "excursion": excursion,
                "sensor_id": None,
                "observed_at": observed_at
            }

        except Exception as error:

            print(
                f"ERROR extracting sample: {error}"
            )

            return None


    print(
        f"WARNING: Unsupported sample format: "
        f"{type(sample)}"
    )

    return None


# ============================================================
# INSERT BATCH
# ============================================================

def insert_batch(data):

    global db_connection

    if not isinstance(data, dict):

        print("WARNING: MQTT payload is not a JSON object.")

        return

    # --------------------------------------------------------
    # SCHEMA
    # --------------------------------------------------------

    schema = data.get(
        "schema"
    )

    # --------------------------------------------------------
    # BOX ID
    #
    # Supports:
    #
    # Old:
    #     "box_id": 1
    #
    # New v4:
    #     "device": {
    #         "box_id": 1
    #     }
    # --------------------------------------------------------

    device = data.get(
        "device",
        {}
    )

    if not isinstance(device, dict):
        device = {}

    box_id = (
        data.get("box_id")
        or device.get("box_id")
    )

    # --------------------------------------------------------
    # SENSOR ID
    # --------------------------------------------------------

    sensor_id = data.get(
        "sensor_id"
    )

    # --------------------------------------------------------
    # BATCH ID
    # --------------------------------------------------------

    batch_id = (
        data.get("batch_id")
        or data.get("batch_sequence")
    )

    # --------------------------------------------------------
    # DELIVERY CLASS
    # --------------------------------------------------------

    delivery_class = (
        data.get("delivery_class")
        or data.get("delivery_path")
        or "unknown"
    )

    # --------------------------------------------------------
    # GATEWAY RECEIVED TIME
    # --------------------------------------------------------

    gateway_received_at = parse_timestamp(
        data.get("gateway_received_at")
    )

    # --------------------------------------------------------
    # SAMPLES
    # --------------------------------------------------------

    samples = data.get(
        "samples",
        []
    )

    if not isinstance(samples, list):

        print("WARNING: samples is not a list.")

        return

    if not samples:

        print("WARNING: MQTT batch contains no samples.")

        return

    # --------------------------------------------------------
    # DEBUG INFORMATION
    # --------------------------------------------------------

    print()
    print("=" * 60)
    print("THERMASSURE BATCH RECEIVED")
    print("=" * 60)

    print(f"Schema:          {schema}")
    print(f"Box ID:          {box_id}")
    print(f"Batch ID:        {batch_id}")
    print(f"Delivery class:  {delivery_class}")
    print(f"Samples:         {len(samples)}")

    print("=" * 60)

    # --------------------------------------------------------
    # MAKE SURE DATABASE IS CONNECTED
    # --------------------------------------------------------

    ensure_database_connection()

    # --------------------------------------------------------
    # INSERT SAMPLES
    # --------------------------------------------------------

    inserted_count = 0

    try:

        with db_connection.cursor() as cursor:

            for sample in samples:

                parsed = extract_sample(
                    sample,
                    data.get("fields")
                )

                if parsed is None:
                    continue

                sample_id = parsed.get(
                    "sample_id"
                )

                temperature_c = parsed.get(
                    "temperature_c"
                )

                battery_mv = parsed.get(
                    "battery_mv"
                )

                excursion = parsed.get(
                    "excursion"
                )

                observed_at = parsed.get(
                    "observed_at"
                )

                sample_sensor_id = (
                    parsed.get("sensor_id")
                    or sensor_id
                )

                # ------------------------------------------------
                # SENSOR ID MAY ALSO BE INSIDE DEVICE.SENSORS
                # ------------------------------------------------

                if sample_sensor_id is None:

                    sensors = device.get(
                        "sensors",
                        []
                    )

                    if isinstance(sensors, list) and len(sensors) == 1:
                        sample_sensor_id = sensors[0]

                # ------------------------------------------------
                # EVENT ID
                # ------------------------------------------------

                if box_id is not None and sample_id is not None:

                    event_id = (
                        f"{box_id}:{sample_id}"
                    )

                elif sample_id is not None:

                    event_id = str(
                        sample_id
                    )

                else:

                    event_id = (
                        f"{batch_id}:"
                        f"{uuid.uuid4()}"
                    )

                # ------------------------------------------------
                # INSERT QUERY
                # ------------------------------------------------

                insert_query = f"""
                    INSERT INTO {DB_TABLE} (
                        event_id,
                        sample_id,
                        box_id,
                        sensor_id,
                        batch_id,
                        delivery_class,
                        observed_at,
                        gateway_received_at,
                        temperature_c,
                        battery_mv,
                        excursion,
                        region,
                        central,
                        bag
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    ON CONFLICT (event_id)
                    DO UPDATE SET
                        box_id = EXCLUDED.box_id,
                        sensor_id = EXCLUDED.sensor_id,
                        batch_id = EXCLUDED.batch_id,
                        delivery_class = EXCLUDED.delivery_class,
                        observed_at = EXCLUDED.observed_at,
                        gateway_received_at =
                            EXCLUDED.gateway_received_at,
                        temperature_c =
                            EXCLUDED.temperature_c,
                        battery_mv =
                            EXCLUDED.battery_mv,
                        excursion =
                            EXCLUDED.excursion,
                        region =
                            EXCLUDED.region,
                        central =
                            EXCLUDED.central,
                        bag =
                            EXCLUDED.bag;
                """

                cursor.execute(
                    insert_query,
                    (
                        event_id,
                        sample_id,
                        box_id,
                        sample_sensor_id,
                        batch_id,
                        delivery_class,
                        observed_at,
                        gateway_received_at,
                        temperature_c,
                        battery_mv,
                        excursion,
                        REGION,
                        CENTRAL,
                        BAG
                    )
                )

                inserted_count += 1

        db_connection.commit()

        print(
            f"Inserted/updated {inserted_count} sample(s)"
        )

    except Exception as error:

        print()
        print("DATABASE INSERT ERROR:")
        print(error)

        try:
            db_connection.rollback()
        except Exception:
            pass

        raise


# ============================================================
# MQTT CONNECT CALLBACK
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties
):

    print()
    print("=" * 60)
    print("MQTT CONNECTION")
    print("=" * 60)

    print(
        f"Connection result: {reason_code}"
    )

    if reason_code == 0:

        print(
            f"Connected to HiveMQ Cloud"
        )

        print(
            f"Subscribing to: {MQTT_TOPIC}"
        )

        result, mid = client.subscribe(
            MQTT_TOPIC,
            qos=1
        )

        print(
            f"Subscribe result: {result}"
        )

    else:

        print(
            "MQTT connection failed."
        )

    print("=" * 60)


# ============================================================
# MQTT MESSAGE CALLBACK
# ============================================================

def on_message(
    client,
    userdata,
    message
):

    print()
    print("-" * 60)
    print("MQTT MESSAGE RECEIVED")
    print("-" * 60)

    print(
        f"Topic: {message.topic}"
    )

    print(
        f"Payload bytes: {len(message.payload)}"
    )

    try:

        payload_text = (
            message.payload.decode(
                "utf-8"
            )
        )

        print(
            f"Payload: {payload_text}"
        )

        data = json.loads(
            payload_text
        )

        insert_batch(
            data
        )

    except json.JSONDecodeError as error:

        print(
            f"JSON decode error: {error}"
        )

    except UnicodeDecodeError as error:

        print(
            f"UTF-8 decode error: {error}"
        )

    except Exception as error:

        print()
        print("ERROR PROCESSING MQTT MESSAGE:")
        print(error)

        try:

            db_connection.rollback()

        except Exception:

            pass


# ============================================================
# MQTT DISCONNECT CALLBACK
# ============================================================

def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties
):

    print()
    print("=" * 60)
    print("MQTT DISCONNECTED")
    print("=" * 60)

    print(
        f"Reason: {reason_code}"
    )

    print(
        "Paho will attempt to reconnect."
    )

    print("=" * 60)


# ============================================================
# MAIN
# ============================================================

def main():

    global db_connection

    print()
    print("=" * 60)
    print("THERMASSURE MQTT -> TIMESCALE CLOUD BRIDGE")
    print("=" * 60)

    print(
        f"MQTT Broker: {MQTT_BROKER}"
    )

    print(
        f"MQTT Port:   {MQTT_PORT}"
    )

    print(
        f"MQTT Topic:  {MQTT_TOPIC}"
    )

    print(
        f"Database:    {DB_NAME}"
    )

    print(
        f"DB Host:     {DB_HOST}"
    )

    print("=" * 60)

    # --------------------------------------------------------
    # CHECK CONFIGURATION
    # --------------------------------------------------------

    check_configuration()

    # --------------------------------------------------------
    # CONNECT TO DATABASE
    # --------------------------------------------------------

    db_connection = connect_database()

    create_table(
        db_connection
    )

    # --------------------------------------------------------
    # CREATE MQTT CLIENT
    # --------------------------------------------------------

    client_id = (
        f"thermassure-render-"
        f"{uuid.uuid4().hex[:12]}"
    )

    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id
    )

    # --------------------------------------------------------
    # MQTT USERNAME / PASSWORD
    # --------------------------------------------------------

    mqtt_client.username_pw_set(
        MQTT_USERNAME,
        MQTT_PASSWORD
    )

    # --------------------------------------------------------
    # TLS
    # --------------------------------------------------------

    mqtt_client.tls_set(
        cert_reqs=ssl.CERT_REQUIRED
    )

    # --------------------------------------------------------
    # AUTOMATIC RECONNECT
    # --------------------------------------------------------

    mqtt_client.reconnect_delay_set(
        min_delay=1,
        max_delay=60
    )

    # --------------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------------

    mqtt_client.on_connect = on_connect

    mqtt_client.on_message = on_message

    mqtt_client.on_disconnect = on_disconnect

    # --------------------------------------------------------
    # CONNECT TO HIVEMQ
    # --------------------------------------------------------

    print()
    print("Connecting to HiveMQ Cloud...")

    mqtt_client.connect(
        MQTT_BROKER,
        MQTT_PORT,
        keepalive=60
    )

    print(
        "Starting MQTT loop..."
    )

    # --------------------------------------------------------
    # KEEP RUNNING
    # --------------------------------------------------------

    mqtt_client.loop_forever()


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()