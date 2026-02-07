import os
import json
import math
import redis
import boto3
import cv2
import numpy as np
from datetime import datetime, timezone
from pymongo import MongoClient
from ultralytics import YOLO
from sort import Sort

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# =========================================================
# QUEUE (SEPARATE FROM OTHER PIPELINES)
# =========================================================
STREAM = "vehicle_count_jobs"
GROUP = "vehicle_count_workers"

# =========================================================
# CONFIG
# =========================================================
MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "yolov8n.pt")
INPUT_TMP = "/tmp/input.mp4"
COUNT_LINE_Y = 350

CLASS_MAP = {
    "motorcycle": "Small",
    "car": "Medium",
    "van": "Medium",
    "bus": "Heavy",
    "truck": "Heavy",
}

# =========================================================
# SEVERITY LOGIC
# =========================================================
def compute_severity(totals: dict) -> str:
    score = (
        totals["small"] * 1 +
        totals["medium"] * 2 +
        totals["heavy"] * 3
    )

    if score < 50:
        return "Good"
    elif score < 120:
        return "Moderate"
    elif score < 250:
        return "Poor"
    else:
        return "Critical"

# =========================================================
# REDIS
# =========================================================
r = redis.Redis.from_url(os.getenv("REDIS_URL"), decode_responses=False)

try:
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
except redis.exceptions.ResponseError:
    pass

CONSUMER = os.getenv("HOSTNAME", "vehicle-worker-1")

# =========================================================
# MONGO
# =========================================================
mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo[os.getenv("MONGO_DB")]
vehicle_results = db.vehicle_counts   # separate collection

# =========================================================
# S3 / MINIO
# =========================================================
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)
BUCKET = os.getenv("S3_BUCKET")

# =========================================================
# ML
# =========================================================
model = YOLO(MODEL_PATH)
tracker = Sort()

print("Vehicle-count worker ready")

# =========================================================
# WORK LOOP
# =========================================================
while True:
    streams = r.xreadgroup(
        groupname=GROUP,
        consumername=CONSUMER,
        streams={STREAM: ">"},
        count=1,
        block=5000,
    )

    if not streams:
        continue

    for _, messages in streams:
        for message_id, data in messages:
            video_id = data[b"video_id"].decode()
            gps_coords = json.loads(data[b"gps_coords"].decode())

            # ---------- DEDUPE ----------
            if vehicle_results.find_one({"_id": video_id}):
                r.xack(STREAM, GROUP, message_id)
                continue
            # ----------------------------

            print(f"[{video_id}] PROCESSING")

            try:
                # ---------- DOWNLOAD ----------
                s3.download_file(BUCKET, f"{video_id}.mp4", INPUT_TMP)

                cap = cv2.VideoCapture(INPUT_TMP)
                if not cap.isOpened():
                    raise RuntimeError("Cannot open video")

                # ---------- COUNTERS ----------
                counts = {"Small": 0, "Medium": 0, "Heavy": 0}
                counted_ids = set()
                id_to_type = {}

                frame_idx = 0
                gps_len = len(gps_coords)

                # ---------- FRAME LOOP ----------
                while True:
                    ret, frame = cap.read()
                    if not ret:
                        break

                    frame_idx += 1
                    results = model(frame, conf=0.3, verbose=False)[0]

                    detections = []
                    current_objects = []

                    for box in results.boxes:
                        cls_name = model.names[int(box.cls[0])]
                        if cls_name not in CLASS_MAP:
                            continue

                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        detections.append([x1, y1, x2, y2, box.conf.item()])

                        cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
                        current_objects.append(((cx, cy), CLASS_MAP[cls_name]))

                    tracks = tracker.update(
                        np.array(detections) if detections else np.empty((0, 5))
                    )

                    for x1, y1, x2, y2, tid in tracks:
                        tid = int(tid)
                        ty = (y1 + y2) / 2

                        # Match detection → type
                        closest_type = None
                        min_dist = float("inf")
                        for (dcx, dcy), vtype in current_objects:
                            d = math.hypot((x1 + x2) / 2 - dcx, ty - dcy)
                            if d < min_dist and d < 50:
                                min_dist = d
                                closest_type = vtype

                        if closest_type:
                            id_to_type[tid] = closest_type

                        vtype = id_to_type.get(tid)
                        if not vtype:
                            continue

                        if ty > COUNT_LINE_Y and tid not in counted_ids:
                            counted_ids.add(tid)
                            counts[vtype] += 1

                cap.release()

                # ---------- FINAL TOTALS ----------
                vehicle_totals = {
                    "small": counts["Small"],
                    "medium": counts["Medium"],
                    "heavy": counts["Heavy"],
                    "total": sum(counts.values()),
                }

                severity = compute_severity(vehicle_totals)

                # ---------- SAVE RESULT ----------
                vehicle_results.insert_one({
                    "_id": video_id,
                    "vehicle_totals": vehicle_totals,
                    "severity": severity,
                    "gps_coords": gps_coords,
                    "created_at": datetime.now(timezone.utc),
                })

                r.xack(STREAM, GROUP, message_id)
                print(f"[{video_id}] DONE → {vehicle_totals} | {severity}")

            except Exception as e:
                print(f"[{video_id}] FAILED:", e)
                r.xack(STREAM, GROUP, message_id)
