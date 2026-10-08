from workers import WorkerEntrypoint, DurableObject, Response
from workers import import_from_javascript

from urllib.parse import urlparse
import json
import asyncpg

from js import eval as js_eval
from pyodide.ffi import to_js


# ============================================================
# JAVASCRIPT HELPERS
# ============================================================

# Cloudflare TCP writable streams require a native JavaScript
# Uint8Array. This helper performs the actual writer.write()
# operation inside JavaScript.
js_write_bytes = js_eval(
    """
    async function(writer, values) {
        const bytes = new Uint8Array(values);
        await writer.write(bytes);
    }
    """
)


async def write_bytes(writer, data: bytes):
    """
    Convert Python bytes to a JavaScript array and let
    JavaScript create the native Uint8Array.
    """
    values = to_js(list(data))
    await js_write_bytes(writer, values)


# ============================================================
# CLOUDFLARE TCP SOCKET
# ============================================================

sockets = import_from_javascript("cloudflare:sockets")
connect = sockets.connect


# ============================================================
# HIVEMQ SETTINGS
# ============================================================

HIVEMQ_HOST = (
    "ac1300c0609a45b1b3004f804830381f.s1.eu.hivemq.cloud"
)

HIVEMQ_PORT = 8883

MQTT_TOPIC = "thermassure/data"

MQTT_CLIENT_ID = "thermassure-cloudflare-bridge"

MQTT_KEEP_ALIVE = 60

MQTT_PROTOCOL_LEVEL = 4


# ============================================================
# MQTT PACKET CONSTANTS
# ============================================================

MQTT_CONNECT = 0x10
MQTT_CONNACK = 0x20
MQTT_PUBLISH = 0x30
MQTT_SUBSCRIBE = 0x82
MQTT_SUBACK = 0x90


# ============================================================
# MQTT ENCODING HELPERS
# ============================================================

def encode_remaining_length(length: int) -> bytes:
    """
    MQTT Remaining Length encoding.
    """

    output = bytearray()

    while True:

        digit = length % 128
        length //= 128

        if length > 0:
            digit |= 0x80

        output.append(digit)

        if length == 0:
            break

    return bytes(output)


def mqtt_string(value: str) -> bytes:
    """
    MQTT UTF-8 string:
    2-byte big-endian length + UTF-8 data.
    """

    encoded = value.encode("utf-8")

    return (
        len(encoded).to_bytes(2, "big")
        + encoded
    )


# ============================================================
# MQTT CONNECT PACKET
# ============================================================

def build_connect_packet(
    username: str,
    password: str,
) -> bytes:

    variable_header = (
        mqtt_string("MQTT")
        + bytes([MQTT_PROTOCOL_LEVEL])

        # Connect flags:
        # Username flag = 1
        # Password flag = 1
        # Clean Session = 1
        + bytes([0xC2])

        + MQTT_KEEP_ALIVE.to_bytes(2, "big")
    )

    payload = (
        mqtt_string(MQTT_CLIENT_ID)
        + mqtt_string(username)
        + mqtt_string(password)
    )

    remaining = variable_header + payload

    return (
        bytes([MQTT_CONNECT])
        + encode_remaining_length(len(remaining))
        + remaining
    )


# ============================================================
# MQTT SUBSCRIBE PACKET
# ============================================================

def build_subscribe_packet(topic: str) -> bytes:

    packet_identifier = b"\x00\x01"

    payload = (
        mqtt_string(topic)
        +
        # QoS 1
        bytes([0x01])
    )

    remaining = packet_identifier + payload

    return (
        bytes([MQTT_SUBSCRIBE])
        + encode_remaining_length(len(remaining))
        + remaining
    )


# ============================================================
# STREAM READING
# ============================================================

async def read_stream_chunk(reader):

    result = await reader.read()

    if result.done:
        return None

    value = result.value

    if value is None:
        return None

    # Convert JavaScript Uint8Array into Python bytes.
    try:
        return bytes(value)
    except Exception:

        try:
            return bytes(value.to_py())
        except Exception:
            return bytes(list(value))


async def read_exact(reader, size: int) -> bytes:

    buffer = bytearray()

    while len(buffer) < size:

        chunk = await read_stream_chunk(reader)

        if chunk is None:
            raise ConnectionError(
                "HiveMQ closed the connection while reading."
            )

        buffer.extend(chunk)

    return bytes(buffer[:size])


# ============================================================
# MQTT PACKET READER
# ============================================================

async def read_mqtt_packet(reader):

    # Fixed header first byte
    first = await read_exact(reader, 1)

    packet_type = first[0]

    # MQTT Remaining Length
    multiplier = 1
    remaining_length = 0

    while True:

        byte = await read_exact(reader, 1)

        digit = byte[0]

        remaining_length += (
            (digit & 127) * multiplier
        )

        if (digit & 128) == 0:
            break

        multiplier *= 128

        if multiplier > 128 * 128 * 128:
            raise ValueError(
                "Invalid MQTT Remaining Length."
            )

    payload = await read_exact(
        reader,
        remaining_length
    )

    return packet_type, payload


# ============================================================
# MQTT PACKET DESCRIPTION
# ============================================================

def describe_mqtt_packet(packet_type, payload):

    packet_name = {
        1: "CONNECT",
        2: "CONNACK",
        3: "PUBLISH",
        4: "PUBACK",
        5: "PUBREC",
        6: "PUBREL",
        7: "PUBCOMP",
        8: "SUBSCRIBE",
        9: "SUBACK",
        10: "UNSUBSCRIBE",
        11: "UNSUBACK",
        12: "PINGREQ",
        13: "PINGRESP",
        14: "DISCONNECT",
    }.get(packet_type, "UNKNOWN")

    return (
        f"{packet_name} "
        f"(type={packet_type}, "
        f"payload={len(payload)} bytes)"
    )


# ============================================================
# CONNACK CHECK
# ============================================================

def check_connack(payload):

    if len(payload) < 2:

        raise ConnectionError(
            "Invalid MQTT CONNACK packet."
        )

    session_present = payload[0]

    return_code = payload[1]

    return session_present, return_code


# ============================================================
# MQTT PUBLISH DECODER
# ============================================================

def decode_publish(payload: bytes):

    """
    Decode a basic MQTT PUBLISH packet.

    MQTT PUBLISH payload:

        topic length
        topic
        optional packet identifier
        JSON payload
    """

    if len(payload) < 2:
        return None

    topic_length = int.from_bytes(
        payload[0:2],
        "big"
    )

    position = 2

    if len(payload) < position + topic_length:
        return None

    topic = payload[
        position:
        position + topic_length
    ].decode(
        "utf-8",
        errors="replace"
    )

    position += topic_length

    # For QoS 0 there is no packet identifier.
    # The first byte of PUBLISH fixed header is needed
    # to know QoS, so this helper assumes the topic is
    # followed directly by the JSON payload.

    json_payload = payload[position:]

    try:

        text = json_payload.decode(
            "utf-8",
            errors="replace"
        )

        return topic, json.loads(text)

    except Exception:

        return topic, {
            "raw_payload": json_payload.decode(
                "utf-8",
                errors="replace"
            )
        }


# ============================================================
# DATABASE INSERT
# ============================================================

async def insert_thermassure_batch(env, message):

    """
    Store ThermAssure JSON data into Tiger/TimescaleDB
    through Cloudflare Hyperdrive.
    """

    device = message.get("device", {})

    box_id = device.get("box_id")

    delivery_class = message.get(
        "delivery_class",
        "unknown"
    )

    gateway_received_at = message.get(
        "gateway_received_at"
    )

    samples = message.get(
        "samples",
        []
    )

    if not samples:
        return 0

    hd = env.HYPERDRIVE

    connection = await asyncpg.connect(
        host=hd.host,
        port=int(hd.port),
        user=hd.user,
        password=hd.password,
        database=hd.database,
        ssl=False,
    )

    inserted = 0

    try:

        for sample in samples:

            event_id = sample.get(
                "event_id"
            )

            sample_id = sample.get(
                "sample_id"
            )

            sensor_id = sample.get(
                "sensor_id"
            )

            observed_at = sample.get(
                "observed_at"
            )

            temperature_c = sample.get(
                "temperature_c"
            )

            battery_mv = sample.get(
                "battery_mv"
            )

            excursion = sample.get(
                "excursion",
                False
            )

            await connection.execute(
                """
                INSERT INTO thermassure_realtime
                (
                    delivery_class,
                    region,
                    central,
                    bag,
                    box_id,
                    event_id,
                    sample_id,
                    sensor_id,
                    observed_at,
                    gateway_received_at,
                    temperature_c,
                    battery_mv,
                    excursion,
                    received_at
                )
                VALUES
                (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9::timestamptz,
                    $10::timestamptz,
                    $11,
                    $12,
                    $13,
                    NOW()
                )
                """,
                delivery_class,
                "Karnataka",
                "Bengaluru",
                "Bag-01",
                box_id,
                event_id,
                sample_id,
                sensor_id,
                observed_at,
                gateway_received_at,
                temperature_c,
                battery_mv,
                excursion,
            )

            inserted += 1

    finally:

        await connection.close()

    return inserted


# ============================================================
# DURABLE OBJECT
# ============================================================

class ThermAssureMQTT(DurableObject):

    def __init__(self, ctx, env):

        super().__init__(ctx, env)

        self.ctx = ctx
        self.env = env


    # ========================================================
    # MQTT TEST
    # ========================================================

    async def mqtt_test(self):

        username = getattr(
            self.env,
            "HIVEMQ_USERNAME",
            None
        )

        password = getattr(
            self.env,
            "HIVEMQ_PASSWORD",
            None
        )

        if not username or not password:

            return Response(
                "ERROR: HiveMQ credentials are not configured.\n\n"
                "Set these Cloudflare secrets:\n"
                "HIVEMQ_USERNAME\n"
                "HIVEMQ_PASSWORD",
                status=500,
            )

        try:

            print("STEP 1: Starting MQTT test")

            # ------------------------------------------------
            # CONNECT TCP/TLS SOCKET
            # ------------------------------------------------

            print("STEP 2: Connecting to HiveMQ TCP/TLS")

            socket = connect(
                f"{HIVEMQ_HOST}:{HIVEMQ_PORT}",
                {
                    "secureTransport": "on"
                }
            )

            print(
                "STEP 3 SUCCESS: TCP/TLS socket created"
            )

            reader = socket.readable.get_reader()

            writer = socket.writable.get_writer()

            print(
                "STEP 4 SUCCESS: Reader and writer created"
            )

            # ------------------------------------------------
            # BUILD MQTT CONNECT
            # ------------------------------------------------

            connect_packet = build_connect_packet(
                username,
                password
            )

            print(
                f"STEP 5: MQTT CONNECT packet built "
                f"({len(connect_packet)} bytes)"
            )

            # ------------------------------------------------
            # SEND MQTT CONNECT
            # ------------------------------------------------

            print(
                "STEP 6: Sending MQTT CONNECT..."
            )

            await write_bytes(
                writer,
                connect_packet
            )

            print(
                "STEP 6 SUCCESS: MQTT CONNECT sent"
            )

            # ------------------------------------------------
            # WAIT FOR CONNACK
            # ------------------------------------------------

            print(
                "STEP 7: Waiting for HiveMQ CONNACK..."
            )

            packet_type, payload = (
                await read_mqtt_packet(reader)
            )

            print(
                "STEP 8: "
                + describe_mqtt_packet(
                    packet_type,
                    payload
                )
            )

            if packet_type != 2:

                return Response(
                    "MQTT TEST FAILED\n\n"
                    f"Expected CONNACK (type 2)\n"
                    f"Received packet type: {packet_type}",
                    status=500,
                )

            session_present, return_code = (
                check_connack(payload)
            )

            print(
                f"CONNACK return code: {return_code}"
            )

            if return_code != 0:

                return Response(
                    "HiveMQ rejected the MQTT connection.\n\n"
                    f"CONNACK return code: {return_code}",
                    status=500,
                )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            print(
                "STEP 9 SUCCESS: HiveMQ MQTT connection accepted"
            )

            # ------------------------------------------------
            # SUBSCRIBE
            # ------------------------------------------------

            subscribe_packet = (
                build_subscribe_packet(
                    MQTT_TOPIC
                )
            )

            print(
                "STEP 10: Sending SUBSCRIBE..."
            )

            await write_bytes(
                writer,
                subscribe_packet
            )

            print(
                "STEP 10 SUCCESS: SUBSCRIBE sent"
            )

            # ------------------------------------------------
            # WAIT FOR SUBACK
            # ------------------------------------------------

            print(
                "STEP 11: Waiting for SUBACK..."
            )

            packet_type, payload = (
                await read_mqtt_packet(reader)
            )

            print(
                "STEP 12: "
                + describe_mqtt_packet(
                    packet_type,
                    payload
                )
            )

            if packet_type == 9:

                return Response(
                    "SUCCESS!\n\n"
                    "Cloudflare connected to HiveMQ.\n"
                    "MQTT CONNECT accepted.\n"
                    f"Subscribed to: {MQTT_TOPIC}\n\n"
                    "The Cloudflare → HiveMQ connection "
                    "is now working."
                )

            return Response(
                "Connected to HiveMQ, but did not receive "
                "the expected SUBACK.\n\n"
                f"Received packet type: {packet_type}",
                status=500,
            )

        except Exception as exc:

            print(
                "MQTT TEST FAILED:"
            )

            print(
                repr(exc)
            )

            return Response(
                "MQTT TEST FAILED:\n\n"
                + repr(exc),
                status=500,
            )


    # ========================================================
    # DATABASE TEST
    # ========================================================

    async def db_test(self):

        try:

            hd = self.env.HYPERDRIVE

            connection = await asyncpg.connect(
                host=hd.host,
                port=int(hd.port),
                user=hd.user,
                password=hd.password,
                database=hd.database,
                ssl=False,
            )

            row = await connection.fetchrow(
                """
                SELECT
                    NOW() AS current_time,
                    current_database() AS database_name
                """
            )

            await connection.close()

            return Response(
                "DATABASE CONNECTION SUCCESS\n\n"
                f"Database: {row['database_name']}\n"
                f"Time: {row['current_time']}"
            )

        except Exception as exc:

            return Response(
                "DATABASE TEST FAILED:\n\n"
                + repr(exc),
                status=500,
            )


    # ========================================================
    # DURABLE OBJECT FETCH
    # ========================================================

    async def fetch(self, request):

        url = urlparse(request.url)

        path = url.path

        if path == "/mqtt-test":

            return await self.mqtt_test()

        if path == "/db-test":

            return await self.db_test()

        return Response(
            "ThermAssureMQTT Durable Object is working."
        )


# ============================================================
# DEFAULT WORKER
# ============================================================

class Default(WorkerEntrypoint):

    async def fetch(self, request):

        url = urlparse(request.url)

        path = url.path

        # ----------------------------------------------------
        # MQTT TEST
        # ----------------------------------------------------

        if path == "/mqtt-test":

            stub = self.env.THERMASSURE_MQTT.get(
                self.env.THERMASSURE_MQTT.idFromName(
                    "thermassure-main"
                )
            )

            return await stub.fetch(
                request.url
            )

        # ----------------------------------------------------
        # DATABASE TEST
        # ----------------------------------------------------

        if path == "/db-test":

            stub = self.env.THERMASSURE_MQTT.get(
                self.env.THERMASSURE_MQTT.idFromName(
                    "thermassure-main"
                )
            )

            return await stub.fetch(
                request.url
            )

        # ----------------------------------------------------
        # HOME
        # ----------------------------------------------------

        return Response(
            "ThermAssure Cloud Bridge\n\n"
            "Available endpoints:\n"
            "/mqtt-test\n"
            "/db-test\n"
        )