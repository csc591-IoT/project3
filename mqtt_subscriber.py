#!/usr/bin/env python3
# mqtt_subscriber_ack.py
import os
import csv
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
import paho.mqtt.client as mqtt

load_dotenv()

BROKER = os.getenv("MQTT_BROKER", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))

FILE_TOPIC_BASE = os.getenv(
    "FILE_TOPIC_BASE", "fileTransfer"
)  # publisher sends: fileTransfer/<filename>
ACK_TOPIC_BASE = os.getenv(
    "ACK_TOPIC_BASE", "fileAck"
)  # subscriber replies: fileAck/<filename>

SUB_QOS = int(os.getenv("SUB_QOS", "1"))
OUT_DIR = Path(os.getenv("OUT_DIR", "received"))
LOG_CSV = os.getenv(
    "SUB_LOG_CSV", "subscriber_appbytes.csv"
)  # optional per-message logging

# If True, avoid overwriting by appending an index if the file already exists
UNIQUE_FILENAMES = os.getenv("UNIQUE_FILENAMES", "1") not in ("0", "false", "False")


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def mqtt_publish_header_len(topic: str, qos: int, payload_len: int) -> int:
    """
    Estimate MQTT 3.1.1 PUBLISH header bytes (fixed + variable; no payload).
    - Fixed header: 1 + VarInt(remaining_length)
    - Variable: 2 + len(topic) + (2 if qos>0)
    remaining_length = variable + payload_len
    """
    variable = 2 + len(topic) + (2 if qos > 0 else 0)
    remaining = variable + payload_len

    # VarInt length for Remaining Length
    v = remaining
    varbytes = 1
    while v > 127:
        v //= 128
        varbytes += 1

    fixed = 1 + varbytes
    return fixed + variable


def next_unique_path(base_dir: Path, name: str) -> Path:
    """Return a non-colliding path (adds _1, _2, … before extension if needed)."""
    p = base_dir / name
    if not UNIQUE_FILENAMES or not p.exists():
        return p
    stem = p.stem
    suf = p.suffix
    i = 1
    while True:
        cand = base_dir / f"{stem}_{i}{suf}"
        if not cand.exists():
            return cand
        i += 1


def log_row(row: dict):
    if not LOG_CSV:
        return
    new_file = not os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new_file:
            w.writeheader()
        w.writerow(row)


def on_connect(client: mqtt.Client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(f"{FILE_TOPIC_BASE}/#", qos=SUB_QOS)
        print(
            f"[{ts()}] Connected to {BROKER}:{PORT} → Subscribed '{FILE_TOPIC_BASE}/#' (QoS {SUB_QOS})"
        )
        print(f"{'='*72}\n MQTT SUBSCRIBER READY \n{'='*72}")
    else:
        print(f"[{ts()}] Connect failed rc={rc}")


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    # Expect topic: fileTransfer/<filename>
    parts = msg.topic.split("/", 1)
    if len(parts) != 2 or parts[0] != FILE_TOPIC_BASE:
        return

    filename = parts[1]
    payload = msg.payload

    # Estimate application-layer bytes for this single message
    header_bytes = mqtt_publish_header_len(msg.topic, msg.qos, len(payload))
    app_total = header_bytes + len(payload)
    ratio = (app_total / len(payload)) if len(payload) > 0 else float("inf")

    # Write the file to disk
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = next_unique_path(OUT_DIR, filename)
    with open(out_path, "wb") as f:
        f.write(payload)

    # Publish ACK (only after file is fully written)
    ack_topic = f"{ACK_TOPIC_BASE}/{filename}"
    client.publish(ack_topic, b"ACK", qos=SUB_QOS, retain=False)

    # Console log + CSV row
    print(
        f"[{ts()}] wrote='{out_path.name}' size={len(payload)}B "
        f"qos={msg.qos} dup={msg.dup} retain={msg.retain} "
        f"hdr={header_bytes}B app_total={app_total}B ratio={ratio:.3f} → ACK→ {ack_topic}"
    )

    log_row(
        {
            "received_at": ts(),
            "topic": msg.topic,
            "filename": filename,
            "qos": msg.qos,
            "dup": msg.dup,
            "retain": msg.retain,
            "payload_bytes": len(payload),
            "header_bytes_est": header_bytes,
            "app_total_bytes": app_total,
            "app_over_file_ratio": f"{ratio:.6f}",
            "ack_topic": ack_topic,
        }
    )


# ---------- Main ----------
def main():
    client = mqtt.Client(
        client_id="AckSubscriber",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
        clean_session=True,
        protocol=mqtt.MQTTv311,
    )
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[{ts()}] Connecting to broker at {BROKER}:{PORT} …")
    client.connect(BROKER, PORT, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n[{ts()}] Stopping…")
        client.disconnect()
        print("Disconnected.")


if __name__ == "__main__":
    main()
