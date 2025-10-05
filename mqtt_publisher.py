# mqtt_publisher.py
import os
import csv
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from dotenv import load_dotenv


load_dotenv()

BROKER = os.getenv("MQTT_BROKER", "127.0.0.1")
PORT = int(os.getenv("MQTT_PORT", "1883"))
QOS = int(os.getenv("MQTT_QOS", "1"))      

DATA_DIR = os.getenv("DATA_DIR", "./DataFiles")
FILE_TOPIC_BASE = os.getenv("FILE_TOPIC_BASE", "fileTransfer")
ACK_TOPIC_BASE = os.getenv("ACK_TOPIC_BASE", "fileAck")

FILE_RUN_PLAN = [
    ("1MB", 10000),
    ("10KB", 1000),
    ("10MB", 100),
    ("100B", 10),
]

RESULTS_CSV = os.getenv("RESULTS_CSV", f"./results_mqtt_qos{QOS}.csv")
ACK_TIMEOUT_SEC = float(os.getenv("ACK_TIMEOUT_SEC", "60.0"))


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

# track ACKs by filename
acks = set()


def on_connect(client: mqtt.Client, userdata, flags, rc):
    if rc == 0:
        print(f"[{ts()}] Connected to {BROKER}:{PORT} (QoS={QOS})")
        # listen for acknowledgements from subscribers
        client.subscribe(f"{ACK_TOPIC_BASE}/#", qos=QOS)
    else:
        print(f"[{ts()}] Connection failed (rc={rc})")

def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    # expect topic - fileAck/<filename> and payload: b"ACK"
    if msg.topic.startswith(f"{ACK_TOPIC_BASE}/") and msg.payload == b"ACK":
        filename = msg.topic.split("/", 1)[1]
        acks.add(filename)


#main send logic
def send_file(client: mqtt.Client, file_path: str, repeats: int, writer: csv.writer):
    filename = os.path.basename(file_path)
    topic    = f"{FILE_TOPIC_BASE}/{filename}"

    if not os.path.exists(file_path):
        print(f"[{ts()}] Missing file: {file_path} (skipping)")
        return

    with open(file_path, "rb") as f:
        data = f.read()
    size = len(data)

    print(f"\n[{ts()}] Starting {repeats} transfers: {filename} ({size} bytes)")

    # remove any stale ack
    acks.discard(filename)

    for i in range(1, repeats + 1):
        # start timer
        t0 = time.perf_counter()

        # Publish bytes, wait until Paho hands to socket
        info = client.publish(topic, data, qos=QOS)
        info.wait_for_publish()

        # Wait for the subscriber's Acknowledgement
        deadline = t0 + ACK_TIMEOUT_SEC
        while filename not in acks and time.perf_counter() < deadline:
            time.sleep(0.002)

        t1 = time.perf_counter()
        elapsed = t1 - t0
        thr_bps = (size / elapsed) if elapsed > 0 else 0.0

        if filename in acks:
            # consume ack for the next iteration
            acks.discard(filename)
            print(f"[{ts()}] {filename} #{i}/{repeats}: {elapsed:.4f}s  {thr_bps/1e6:.2f} MB/s")
        else:
            print(f"[{ts()}] {filename} #{i}/{repeats}: ACK timeout after {ACK_TIMEOUT_SEC:.1f}s")

        # log a row 
        writer.writerow(["MQTT", f"QoS{QOS}", filename, size, i, f"{elapsed:.6f}", f"{thr_bps:.2f}"])


def main():
    os.makedirs(os.path.dirname(RESULTS_CSV) or ".", exist_ok=True)
    new_csv = not os.path.exists(RESULTS_CSV)

    client = mqtt.Client(client_id="Publisher_PC", callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[{ts()}] Connecting to broker at {BROKER}:{PORT} ...")
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    try:
        with open(RESULTS_CSV, "a", newline="") as f:
            writer = csv.writer(f)
            if new_csv:
                writer.writerow(["protocol", "variant", "file_name", "file_size_bytes", "iteration", "elapsed_sec", "throughput_Bps"])

            for fname, reps in FILE_RUN_PLAN:
                send_file(client, os.path.join(DATA_DIR, fname), reps, writer)

    except KeyboardInterrupt:
        print(f"\n[{ts()}] Interrupted by user.")
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"[{ts()}] 🔌 Disconnected.")

if __name__ == "__main__":
    main()
