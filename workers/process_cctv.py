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
# REDIS STREAMS
# =========================================================
JOB_STREAM = "vehicle_count_jobs"
EVENT_STREAM = "vehicle_count_events"
GROUP = "vehicle_count_workers"

# =========================================================
# CONFIG
# =========================================================
MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "yolov8n.pt")
INPUT_TMP = "/tmp/input.mp4"
COUNT_LINE_Y = 350
PROGRESS_EVERY_N_FRAMES = 50

CLASS_MAP = {
    "motorcycle": "Small",
    "car": "Medium",
    "van": "Medium",
    "bus": "Heavy",
    "truck": "Heavy",
}

# =========================================================
# SEVERITY
# =========================================================
def compute_severity(totals: dict) -> str:
    score = totals["small"] + 2 * totals["medium"] + 3 * totals["heavy"]
    if score < 50:
        return "Good"
    elif score < 120:
        return "Moderate"
    elif score < 250:
        return "Poor"
    return "Critical"

# =========================================================
# REDIS
# =========================================================
r = redis.Redis.from_url(os.getenv("REDIS_URL"), decode_responses=False)
try:
    r.xgroup_create(JOB_STREAM, GROUP, id="0", mkstream=True)
except redis.exceptions.ResponseError:
    pass

CONSUMER = os.getenv("HOSTNAME", "vehicle-worker-1")

# =========================================================
# MONGO
# =========================================================
mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo[os.getenv("MONGO_DB")]
videos = db.videos

# =========================================================
# S3
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
        streams={JOB_STREAM: ">"},
        count=1,
        block=5000,
    )

    if not streams:
        continue

    for _, messages in streams:
        for message_id, data in messages:
            video_id = data[b"video_id"].decode()
            gps_coords = json.loads(data[b"gps_coords"].decode())
            print(f"Received job for video_id={video_id} at {gps_coords}")
            doc = videos.find_one({"video_id": video_id})
            if not doc or doc.get("status") == "PROCESSED":
                r.xack(JOB_STREAM, GROUP, message_id)
                continue

            # ---------- START ----------
            r.xadd(EVENT_STREAM, {
                "video_id": video_id,
                "status": "PROCESSING",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

            try:
                s3.download_file(BUCKET, f"{video_id}.mp4", INPUT_TMP)
                cap = cv2.VideoCapture(INPUT_TMP)
                if not cap.isOpened():
                    raise RuntimeError("Cannot open video")

                counts = {"Small": 0, "Medium": 0, "Heavy": 0}
                class_counts = {k: 0 for k in CLASS_MAP}
                counted_ids = set()
                id_to_type = {}
                id_to_class = {}

                frame_idx = 0

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
                        current_objects.append(((cx, cy), CLASS_MAP[cls_name], cls_name))

                    tracks = tracker.update(
                        np.array(detections) if detections else np.empty((0, 5))
                    )

                    for x1, y1, x2, y2, tid in tracks:
                        tid = int(tid)
                        ty = (y1 + y2) / 2

                        for (dcx, dcy), vtype, vclass in current_objects:
                            if math.hypot((x1 + x2)/2 - dcx, ty - dcy) < 50:
                                id_to_type[tid] = vtype
                                id_to_class[tid] = vclass

                        if tid not in counted_ids and ty > COUNT_LINE_Y and tid in id_to_type:
                            counted_ids.add(tid)
                            counts[id_to_type[tid]] += 1
                            class_counts[id_to_class[tid]] += 1

                    # ---------- PROGRESS ----------
                    if frame_idx % PROGRESS_EVERY_N_FRAMES == 0:
                        r.xadd(EVENT_STREAM, {
                            "video_id": video_id,
                            "status": "RUNNING",
                            "frame": frame_idx,
                        })

                cap.release()

                vehicle_totals = {
                    "small": counts["Small"],
                    "medium": counts["Medium"],
                    "heavy": counts["Heavy"],
                    "total": sum(counts.values()),
                }
                print(f"Finished processing video_id={video_id}, totals={vehicle_totals}")
                severity = compute_severity(vehicle_totals)

                videos.update_one(
                    {"video_id": video_id},
                    {
                        "$set": {
                            "status": "PROCESSED",
                            "vehicle_totals": vehicle_totals,
                            "class_counts": class_counts,
                            "severity": severity,
                            "result_key": f"{video_id}.json",
                            "updated_at": datetime.now(timezone.utc),
                        }
                    },
                )

                # ---------- DONE ----------
                r.xadd(EVENT_STREAM, {
                    "video_id": video_id,
                    "status": "DONE",
                    "vehicle_totals": json.dumps(vehicle_totals),
                    "severity": severity,
                })

                r.xack(JOB_STREAM, GROUP, message_id)

            except Exception as e:
                r.xadd(EVENT_STREAM, {
                    "video_id": video_id,
                    "status": "FAILED",
                    "error": str(e),
                })
                r.xack(JOB_STREAM, GROUP, message_id)
