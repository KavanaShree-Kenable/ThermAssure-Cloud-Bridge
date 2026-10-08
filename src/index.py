from workers import WorkerEntrypoint, DurableObject, Response
from workers import import_from_javascript

import json
import asyncpg

from pyodide.ffi import to_js
from js import Uint8Array


# ============================================================
# HIVE MQ CONFIGURATION
# ============================================================

HIVEMQ_HOST = (
    "ac1300c0609a45b1b3004f804830381f.s1.eu.hivemq.cloud"
)

HIVEMQ_PORT = 8883

MQTT_TOPIC = "thermassure/data"

# Must be unique on HiveMQ
MQTT_CLIENT_ID = (
    "thermassure-cloudflare-bridge-20261008"
)

MQTT_KEEPALIVE = 60


# ============================================================
# DATABASE
# ============================================================

DB_TABLE = "thermassure_realtime"


# ============================================================
# CLOUDFLARE TCP SOCKET
# ============================================================

sockets = import_from_javascript(
    "cloudflare:sockets"
)

connect = sockets.connect


# ============================================================
# WRITE PYTHON BYTES TO CLOUDFLARE SOCKET
# ============================================================

async def write_bytes(writer, data: bytes):
    """
    Convert Python bytes to JavaScript Uint8Array.
    Cloudflare TCP writable streams require Uint8Array.
    """

    values = to_js(list(data))

    js_bytes = Uint8Array.new(values)

    await writer.write(js_bytes)


# ============================================================
# MQTT REMAINING LENGTH ENCODER
# ============================================================

def encode_remaining_length(length: int) -> bytes:

    encoded = bytearray()

    while True:

        digit = length % 128

        length //= 128

        if length > 0:
            digit |= 0x80

        encoded.append(digit)

        if length == 0:
            break

    return bytes(encoded)


# ============================================================
# MQTT UTF-8 STRING
# ============================================================

def mqtt_string(value: str) -> bytes:

    data = value.encode("utf-8")

    return (
        len(data).to_bytes(2, "big")
        + data
    )


# ============================================================
# MQTT CONNECT PACKET
# ============================================================

def build_connect_packet(
    client_id: str,
    username: str,
    password: str,
) -> bytes:
    """
    Build MQTT 3.1.1 CONNECT packet.

    Flags:
        Clean Session = 1
        Username      = 1
        Password      = 1

        11000010 = 0xC2
    """

    # --------------------------------------------------------
    # VARIABLE HEADER
    # --------------------------------------------------------

    variable_header = bytearray()

    # Protocol name = MQTT
    variable_header.extend(
        mqtt_string("MQTT")
    )

    # MQTT 3.1.1
    variable_header.append(4)

    # CONNECT flags
    #
    # bit 7 = Username
    # bit 6 = Password
    # bit 1 = Clean Session
    #
    # 11000010 = 0xC2
    variable_header.append(0xC2)

    # Keep Alive = 60 seconds
    variable_header.extend(
        MQTT_KEEPALIVE.to_bytes(
            2,
            "big",
        )
    )

    # --------------------------------------------------------
    # PAYLOAD
    # --------------------------------------------------------

    payload = bytearray()

    # Client ID
    payload.extend(
        mqtt_string(client_id)
    )

    # Username
    payload.extend(
        mqtt_string(username)
    )

    # Password
    payload.extend(
        mqtt_string(password)
    )

    # --------------------------------------------------------
    # COMPLETE PACKET
    # --------------------------------------------------------

    remaining_length = (
        len(variable_header)
        + len(payload)
    )

    packet = bytearray()

    # MQTT CONNECT packet type
    packet.append(0x10)

    # Remaining length
    packet.extend(
        encode_remaining_length(
            remaining_length
        )
    )

    # Variable header
    packet.extend(
        variable_header
    )

    # Payload
    packet.extend(
        payload
    )

    return bytes(packet)


# ============================================================
# SAFE MQTT CONNECT DEBUG
# ============================================================

def debug_connect_packet(
    packet: bytes,
    username: str,
    password: str,
):
    """
    Print useful MQTT packet information without
    exposing the username/password contents.
    """

    print(
        "CONNECT PACKET LENGTH:",
        len(packet),
    )

    print(
        "USERNAME LENGTH:",
        len(username.encode("utf-8")),
    )

    print(
        "PASSWORD LENGTH:",
        len(password.encode("utf-8")),
    )

    # Print only the beginning of the packet.
    # Do NOT print the full packet because it contains
    # the username/password.
    preview_length = min(
        14,
        len(packet),
    )

    print(
        "CONNECT PACKET HEADER HEX:",
        packet[
            :preview_length
        ].hex(" "),
    )


# ============================================================
# MQTT SUBSCRIBE PACKET
# ============================================================

def build_subscribe_packet(
    packet_id: int,
    topic: str,
) -> bytes:

    payload = bytearray()

    # Topic filter
    payload.extend(
        mqtt_string(topic)
    )

    # Requested QoS = 1
    payload.append(1)

    # Packet identifier
    variable_header = (
        packet_id.to_bytes(
            2,
            "big",
        )
    )

    remaining_length = (
        len(variable_header)
        + len(payload)
    )

    packet = bytearray()

    # SUBSCRIBE packet
    packet.append(0x82)

    packet.extend(
        encode_remaining_length(
            remaining_length
        )
    )

    packet.extend(
        variable_header
    )

    packet.extend(
        payload
    )

    return bytes(packet)


# ============================================================
# MQTT PUBACK
# ============================================================

def build_puback_packet(
    packet_id: int,
) -> bytes:

    return bytes(
        [
            0x40,
            0x02,
            (packet_id >> 8) & 0xFF,
            packet_id & 0xFF,
        ]
    )


# ============================================================
# READ EXACT NUMBER OF BYTES
# ============================================================

async def read_exact(
    reader,
    size: int,
):
    """
    Read exactly `size` bytes from the socket.
    """

    result = bytearray()

    while len(result) < size:

        chunk = await reader.read()

        if chunk.done:

            raise RuntimeError(
                "HiveMQ closed the connection."
            )

        value = chunk.value

        if value is None:
            continue

        try:

            python_value = value.to_py()

        except Exception:

            python_value = value

        result.extend(
            bytes(python_value)
        )

    return bytes(
        result[:size]
    )


# ============================================================
# MQTT PACKET READER
# ============================================================

async def read_mqtt_packet(reader):
    """
    Read one complete MQTT packet.
    """

    # --------------------------------------------------------
    # FIXED HEADER BYTE
    # --------------------------------------------------------

    first_byte = await read_exact(
        reader,
        1,
    )

    header = first_byte[0]

    packet_type = (
        header >> 4
    )

    flags = (
        header & 0x0F
    )

    # --------------------------------------------------------
    # REMAINING LENGTH
    # --------------------------------------------------------

    multiplier = 1

    remaining_length = 0

    while True:

        encoded_byte = await read_exact(
            reader,
            1,
        )

        digit = encoded_byte[0]

        remaining_length += (
            (digit & 127)
            * multiplier
        )

        if (
            digit & 128
        ) == 0:

            break

        multiplier *= 128

        if multiplier > (
            128 * 128 * 128
        ):

            raise RuntimeError(
                "Invalid MQTT Remaining Length."
            )

    print(
        "MQTT PACKET TYPE:",
        packet_type,
    )

    print(
        "MQTT PACKET FLAGS:",
        flags,
    )

    print(
        "MQTT REMAINING LENGTH:",
        remaining_length,
    )

    # --------------------------------------------------------
    # PACKET PAYLOAD
    # --------------------------------------------------------

    payload = await read_exact(
        reader,
        remaining_length,
    )

    return (
        packet_type,
        flags,
        payload,
    )


# ============================================================
# CONNACK CHECK
# ============================================================

def check_connack(
    payload: bytes,
):

    if len(payload) < 2:

        raise RuntimeError(
            "Invalid MQTT CONNACK packet."
        )

    acknowledge_flags = payload[0]

    return_code = payload[1]

    print(
        "CONNACK ACKNOWLEDGE FLAGS:",
        acknowledge_flags,
    )

    print(
        "CONNACK RETURN CODE:",
        return_code,
    )

    if return_code != 0:

        error_messages = {

            1: (
                "Unacceptable protocol version"
            ),

            2: (
                "Identifier rejected"
            ),

            3: (
                "Server unavailable"
            ),

            4: (
                "Bad username or password"
            ),

            5: (
                "Not authorized"
            ),
        }

        message = error_messages.get(
            return_code,
            (
                f"Unknown MQTT return code "
                f"{return_code}"
            ),
        )

        raise RuntimeError(
            "HiveMQ rejected MQTT connection: "
            f"{message}"
        )

    return True


# ============================================================
# MQTT PUBLISH DECODER
# ============================================================

def decode_publish(
    flags: int,
    payload: bytes,
):
    """
    Decode MQTT PUBLISH packet.
    Supports QoS 0 and QoS 1.
    """

    if len(payload) < 2:

        return (
            None,
            None,
            None,
        )

    position = 0

    # --------------------------------------------------------
    # TOPIC LENGTH
    # --------------------------------------------------------

    topic_length = int.from_bytes(
        payload[
            position:
            position + 2
        ],
        "big",
    )

    position += 2

    # --------------------------------------------------------
    # TOPIC
    # --------------------------------------------------------

    topic = payload[
        position:
        position + topic_length
    ].decode(
        "utf-8",
        errors="replace",
    )

    position += topic_length

    # --------------------------------------------------------
    # QoS
    # --------------------------------------------------------

    qos = (
        flags >> 1
    ) & 0x03

    packet_identifier = None

    # --------------------------------------------------------
    # QoS 1 / QoS 2 PACKET IDENTIFIER
    # --------------------------------------------------------

    if qos > 0:

        if len(payload) < (
            position + 2
        ):

            return (
                topic,
                None,
                qos,
            )

        packet_identifier = int.from_bytes(
            payload[
                position:
                position + 2
            ],
            "big",
        )

        position += 2

    # --------------------------------------------------------
    # MESSAGE PAYLOAD
    # --------------------------------------------------------

    message_payload = payload[
        position:
    ]

    return (
        topic,
        message_payload,
        packet_identifier,
    )


# ============================================================
# THERMASSURE JSON DECODER
# ============================================================

def decode_thermassure_message(
    raw_payload: bytes,
):

    try:

        text = raw_payload.decode(
            "utf-8",
            errors="replace",
        )

        message = json.loads(
            text
        )

        return message

    except Exception as exc:

        print(
            "JSON DECODER ERROR:",
            exc,
        )

        return None


# ============================================================
# INSERT INTO TIGER / TIMESCALE
# ============================================================

async def insert_thermassure_batch(
    env,
    message,
):
    """
    Insert ThermAssure samples into
    thermassure_realtime using Hyperdrive.
    """

    hyperdrive = env.HYPERDRIVE

    connection_string = (
        hyperdrive.connectionString
    )

    conn = await asyncpg.connect(
        connection_string,
        ssl=False,
    )

    inserted = 0

    try:

        # ----------------------------------------------------
        # DEVICE
        # ----------------------------------------------------

        device = message.get(
            "device",
            {},
        )

        box_id = device.get(
            "box_id"
        )

        # ----------------------------------------------------
        # BATCH
        # ----------------------------------------------------

        delivery_class = (
            message.get(
                "delivery_class"
            )
        )

        gateway_received_at = (
            message.get(
                "gateway_received_at"
            )
        )

        samples = message.get(
            "samples",
            [],
        )

        # ----------------------------------------------------
        # SAMPLES
        # ----------------------------------------------------

        for sample in samples:

            sample_id = sample.get(
                "sample_id"
            )

            sensor_id = sample.get(
                "sensor_id"
            )

            observed_at = sample.get(
                "observed_at"
            )

            temperature_c = (
                sample.get(
                    "temperature_c"
                )
            )

            battery_mv = (
                sample.get(
                    "battery_mv"
                )
            )

            excursion = (
                sample.get(
                    "excursion",
                    False,
                )
            )

            await conn.execute(
                """
                INSERT INTO thermassure_realtime
                (
                    delivery_class,
                    region,
                    central,
                    bag,
                    box_id,
                    received_at,
                    gateway_received_at,
                    sample_id,
                    sensor_id,
                    observed_at,
                    temperature_c,
                    battery_mv,
                    excursion
                )
                VALUES
                (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    NOW(),
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12
                )
                """,

                delivery_class,

                "Karnataka",

                "Bengaluru",

                "Bag-01",

                box_id,

                gateway_received_at,

                sample_id,

                sensor_id,

                observed_at,

                temperature_c,

                battery_mv,

                excursion,
            )

            inserted += 1

    finally:

        await conn.close()

    print(
        f"SUCCESS: Stored {inserted} "
        "sample(s) in Timescale Cloud."
    )

    return inserted


# ============================================================
# DURABLE OBJECT
# ============================================================

class ThermAssureMQTT(
    DurableObject
):

    async def fetch(
        self,
        request,
    ):

        url = request.url

        # ----------------------------------------------------
        # MQTT TEST
        # ----------------------------------------------------

        if url.endswith(
            "/mqtt-test"
        ):

            return await self.mqtt_test()

        # ----------------------------------------------------
        # DATABASE TEST
        # ----------------------------------------------------

        if url.endswith(
            "/db-test"
        ):

            return await self.database_test()

        return Response(
            "ThermAssure MQTT Durable Object is running."
        )


    # ========================================================
    # MQTT TEST
    # ========================================================

    async def mqtt_test(
        self,
    ):

        print(
            "======================================"
        )

        print(
            "STEP 1: Starting MQTT connection test"
        )

        print(
            "======================================"
        )

        env = self.env

        # ----------------------------------------------------
        # READ CLOUDFLARE SECRETS
        # ----------------------------------------------------

        username = (
            env.HIVEMQ_USERNAME
        )

        password = (
            env.HIVEMQ_PASSWORD
        )

        if not username:

            return Response(
                "ERROR: HIVEMQ_USERNAME secret is missing.",
                status=500,
            )

        if not password:

            return Response(
                "ERROR: HIVEMQ_PASSWORD secret is missing.",
                status=500,
            )

        print(
            "STEP 2: HiveMQ credentials found."
        )

        print(
            "MQTT USERNAME LENGTH:",
            len(
                username.encode(
                    "utf-8"
                )
            ),
        )

        print(
            "MQTT PASSWORD LENGTH:",
            len(
                password.encode(
                    "utf-8"
                )
            ),
        )

        # ----------------------------------------------------
        # TCP / TLS
        # ----------------------------------------------------

        print(
            f"STEP 3: Connecting to HiveMQ "
            f"{HIVEMQ_HOST}:{HIVEMQ_PORT}"
        )

        socket_options = to_js(
            {
                "secureTransport": "on",
            }
        )

        # IMPORTANT:
        # Cloudflare connect() requires HOST:PORT.
        socket = connect(
            f"{HIVEMQ_HOST}:{HIVEMQ_PORT}",
            socket_options,
        )

        # IMPORTANT:
        # Wait until TCP/TLS is actually open.
        await socket.opened

        print(
            "STEP 4: TCP/TLS socket opened."
        )

        # ----------------------------------------------------
        # SOCKET READ/WRITE
        # ----------------------------------------------------

        reader = (
            socket.readable.getReader()
        )

        writer = (
            socket.writable.getWriter()
        )

        print(
            "STEP 5: Socket reader/writer obtained."
        )

        # ----------------------------------------------------
        # BUILD MQTT CONNECT
        # ----------------------------------------------------

        connect_packet = (
            build_connect_packet(
                MQTT_CLIENT_ID,
                username,
                password,
            )
        )

        print(
            "STEP 6: MQTT CONNECT packet created."
        )

        # Safe debugging.
        # Password is NOT printed.
        debug_connect_packet(
            connect_packet,
            username,
            password,
        )

        # ----------------------------------------------------
        # SEND MQTT CONNECT
        # ----------------------------------------------------

        print(
            "STEP 7: Sending MQTT CONNECT packet."
        )

        await write_bytes(
            writer,
            connect_packet,
        )

        print(
            "STEP 8: MQTT CONNECT packet sent."
        )

        # ----------------------------------------------------
        # WAIT FOR CONNACK
        # ----------------------------------------------------

        print(
            "STEP 9: Waiting for HiveMQ CONNACK..."
        )

        (
            packet_type,
            flags,
            payload,
        ) = await read_mqtt_packet(
            reader
        )

        print(
            "STEP 10: MQTT packet received."
        )

        print(
            "PACKET TYPE:",
            packet_type,
        )

        print(
            "PACKET PAYLOAD LENGTH:",
            len(payload),
        )

        # ----------------------------------------------------
        # CONNACK
        # ----------------------------------------------------

        if packet_type != 2:

            return Response(
                "ERROR: Expected MQTT CONNACK "
                f"(packet type 2), but received "
                f"packet type {packet_type}.",
                status=500,
            )

        check_connack(
            payload
        )

        print(
            "STEP 11: HiveMQ CONNACK successful!"
        )

        # ----------------------------------------------------
        # SUBSCRIBE
        # ----------------------------------------------------

        subscribe_packet = (
            build_subscribe_packet(
                packet_id=1,
                topic=MQTT_TOPIC,
            )
        )

        print(
            "STEP 12: Sending SUBSCRIBE packet."
        )

        print(
            "SUBSCRIBE TOPIC:",
            MQTT_TOPIC,
        )

        await write_bytes(
            writer,
            subscribe_packet,
        )

        print(
            "STEP 13: SUBSCRIBE packet sent."
        )

        # ----------------------------------------------------
        # WAIT FOR SUBACK
        # ----------------------------------------------------

        (
            packet_type,
            flags,
            payload,
        ) = await read_mqtt_packet(
            reader
        )

        print(
            "STEP 14: MQTT packet received "
            f"after SUBSCRIBE. type={packet_type}"
        )

        if packet_type != 9:

            return Response(
                "ERROR: Expected SUBACK "
                f"(packet type 9), but received "
                f"packet type {packet_type}.",
                status=500,
            )

        print(
            "STEP 15: MQTT subscription successful!"
        )

        # ----------------------------------------------------
        # WAIT FOR PUBLISH
        # ----------------------------------------------------

        print(
            "STEP 16: Waiting for ThermAssure "
            "PUBLISH message..."
        )

        while True:

            (
                packet_type,
                flags,
                payload,
            ) = await read_mqtt_packet(
                reader
            )

            print(
                "RECEIVED MQTT PACKET TYPE:",
                packet_type,
            )

            # ------------------------------------------------
            # PUBLISH
            # ------------------------------------------------

            if packet_type == 3:

                (
                    topic,
                    message_payload,
                    packet_identifier,
                ) = decode_publish(
                    flags,
                    payload,
                )

                print(
                    "PUBLISH TOPIC:",
                    topic,
                )

                if message_payload is None:

                    return Response(
                        "ERROR: MQTT PUBLISH "
                        "payload is invalid.",
                        status=500,
                    )

                print(
                    "PUBLISH PAYLOAD LENGTH:",
                    len(message_payload),
                )

                # ------------------------------------------------
                # QoS
                # ------------------------------------------------

                qos = (
                    flags >> 1
                ) & 0x03

                print(
                    "PUBLISH QoS:",
                    qos,
                )

                # ------------------------------------------------
                # PUBACK FOR QoS 1
                # ------------------------------------------------

                if (
                    qos == 1
                    and packet_identifier
                    is not None
                ):

                    puback = (
                        build_puback_packet(
                            packet_identifier
                        )
                    )

                    await write_bytes(
                        writer,
                        puback,
                    )

                    print(
                        "PUBACK sent."
                    )

                # ------------------------------------------------
                # THERMASSURE MESSAGE
                # ------------------------------------------------

                if topic == MQTT_TOPIC:

                    print(
                        "ThermAssure topic matched."
                    )

                    message = (
                        decode_thermassure_message(
                            message_payload
                        )
                    )

                    if message is None:

                        return Response(
                            "ERROR: ThermAssure "
                            "JSON decoding failed.",
                            status=500,
                        )

                    print(
                        "STEP 17: ThermAssure JSON "
                        "decoded successfully."
                    )

                    print(
                        "BATCH ID:",
                        message.get(
                            "batch_id"
                        ),
                    )

                    print(
                        "SAMPLE COUNT:",
                        len(
                            message.get(
                                "samples",
                                [],
                            )
                        ),
                    )

                    # ------------------------------------------------
                    # DATABASE INSERT
                    # ------------------------------------------------

                    try:

                        inserted = (
                            await insert_thermassure_batch(
                                env,
                                message,
                            )
                        )

                    except Exception as exc:

                        print(
                            "DATABASE ERROR:",
                            exc,
                        )

                        return Response(
                            "MQTT message received, "
                            "but database insert failed: "
                            f"{exc}",
                            status=500,
                        )

                    print(
                        "STEP 18: Database insert complete."
                    )

                    return Response(
                        "SUCCESS: Cloudflare connected "
                        "to HiveMQ, received a "
                        "ThermAssure message, decoded "
                        f"it, and inserted {inserted} "
                        "sample(s) into Tiger Cloud."
                    )


    # ========================================================
    # DATABASE TEST
    # ========================================================

    async def database_test(
        self,
    ):

        try:

            env = self.env

            connection_string = (
                env.HYPERDRIVE.connectionString
            )

            conn = await asyncpg.connect(
                connection_string,
                ssl=False,
            )

            try:

                result = (
                    await conn.fetchval(
                        "SELECT NOW()"
                    )
                )

            finally:

                await conn.close()

            print(
                "DATABASE CONNECTION SUCCESS:",
                result,
            )

            return Response(
                "SUCCESS: Connected to "
                "Tiger/Timescale Cloud. "
                f"Database time: {result}"
            )

        except Exception as exc:

            print(
                "DATABASE CONNECTION ERROR:",
                exc,
            )

            return Response(
                "Database connection failed: "
                f"{exc}",
                status=500,
            )


# ============================================================
# DEFAULT WORKER
# ============================================================

class Default(
    WorkerEntrypoint
):

    async def fetch(
        self,
        request,
    ):

        url = request.url

        # ----------------------------------------------------
        # HOME
        # ----------------------------------------------------

        if url.endswith("/"):

            return Response(
                "ThermAssure Cloud Bridge is running."
            )

        # ----------------------------------------------------
        # DURABLE OBJECT
        # ----------------------------------------------------

        stub = (
            self.env.THERMASSURE_MQTT.get(
                self.env.THERMASSURE_MQTT.idFromName(
                    "thermassure-main"
                )
            )
        )

        # ----------------------------------------------------
        # MQTT TEST
        # ----------------------------------------------------

        if url.endswith(
            "/mqtt-test"
        ):

            return await stub.fetch(
                request
            )

        # ----------------------------------------------------
        # DATABASE TEST
        # ----------------------------------------------------

        if url.endswith(
            "/db-test"
        ):

            return await stub.fetch(
                request
            )

        return Response(
            "ThermAssure Cloud Bridge is running."
        )
