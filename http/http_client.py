import requests
import csv
import time
from datetime import datetime
import os

SERVER_IP = "127.0.0.1"
PORT = 8000

FILE_RUN_PLAN = [
    ("100B", 10000),
    ("10KB", 1000),
    ("10MB", 100),
    ("1MB", 10),
]

RESULTS_CSV = "./results_http.csv"


def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def download_file(url):
    t0 = time.perf_counter()
    r = requests.get(url)
    t1 = time.perf_counter()

    elapsed = t1 - t0
    size = len(r.content)
    throughput = (size / elapsed) if elapsed > 0 else 0.0
    return elapsed, throughput, size


def main():
    new_file = not os.path.exists(RESULTS_CSV)
    with open(RESULTS_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(
                [
                    "protocol",
                    "file_name",
                    "file_size_bytes",
                    "iteration",
                    "elapsed_sec",
                    "throughput_Bps",
                ]
            )

        for fname, reps in FILE_RUN_PLAN:
            print(f"\n[{ts()}] Downloading {fname} {reps} times ...")
            url = f"http://{SERVER_IP}:{PORT}/{fname}"
            for i in range(1, reps + 1):
                elapsed, thr, size = download_file(url)
                writer.writerow(
                    ["HTTP", fname, size, i, f"{elapsed:.6f}", f"{thr:.2f}"]
                )
                if i % 100 == 0 or reps < 100:
                    print(
                        f"[{ts()}] {fname} #{i}/{reps}: {elapsed:.4f}s {thr/1e6:.2f} MB/s"
                    )


if __name__ == "__main__":
    main()
