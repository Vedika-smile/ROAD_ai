import os
import redis
import boto3
import cv2
import datetime
from pymongo import MongoClient
from ultralytics import YOLO
from collections import defaultdict

# ---------- CONFIG ----------
MODEL_PATH = "/app/models/YOLOv8_Small_RDD.pt"
INPUT_TMP = "/tmp/input.mp4"
OUTPUT_TMP = "/tmp/output.mp4"
RESULT_PREFIX = "results/"
# ----------------------------

# Redis
r = redis.Redis.from_url(os.getenv("REDIS_URL"))

# Create consumer group (safe if already exists)
try:
    r.xgroup_create("video_jobs", "workers", id="0", mkstream=True)
except redis.exceptions.ResponseError:
    pass

consumer = os.getenv("HOSTNAME", "worker-1")

# MongoDB
mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo[os.getenv("MONGO_DB")]
videos = db.videos

# MinIO / S3
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)

# Load YOLO ONCE
if not os.path.exists(MODEL_PATH):
    raise RuntimeError(f"Model not found at {MODEL_PATH}")

model = YOLO(MODEL_PATH)
model.to("cuda").half()


# ---------- WORKER LOOP ----------
while True:
    streams = r.xreadgroup(
        groupname="workers",
        consumername=consumer,
        streams={"video_jobs": ">"},
        count=1,
        block=5000
    )

    for _, messages in streams:
        for message_id, data in messages:
            video_id = data[b"video_id"].decode()
            input_key = f"{video_id}.mp4"
            output_key = f"{RESULT_PREFIX}{video_id}_detected.mp4"

            print(f"[{video_id}] started")

            try:
                # Mark PROCESSING
                videos.update_one(
                    {"_id": video_id},
                    {"$set": {
                        "status": "PROCESSING",
                        "updated_at": datetime.datetime.utcnow()
                    }}
                )

                # Download input
                s3.download_file(os.getenv("S3_BUCKET"), input_key, INPUT_TMP)

                cap = cv2.VideoCapture(INPUT_TMP)
                if not cap.isOpened():
                    raise RuntimeError("Could not open video")

                w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                fps = cap.get(cv2.CAP_PROP_FPS) or 25.0

                out = cv2.VideoWriter(
                    OUTPUT_TMP,
                    cv2.VideoWriter_fourcc(*"mp4v"),
                    fps,
                    (w, h)
                )

                detection_stats = defaultdict(int)
                frame_count = 0

                # ---------- FRAME LOOP ----------
                while True:
                    success, frame = cap.read()
                    if not success:
                        break

                    results = model(frame, conf=0.25, verbose=False)

                    # Count detections
                    for box in results[0].boxes:
                        cls_id = int(box.cls[0])
                        cls_name = model.names[cls_id]
                        detection_stats[cls_name] += 1

                    annotated = results[0].plot()
                    out.write(annotated)
                    frame_count += 1

                cap.release()
                out.release()

                # Upload result video
                s3.upload_file(
                    OUTPUT_TMP,
                    os.getenv("S3_BUCKET"),
                    output_key
                )

                # Save stats to MongoDB
                videos.update_one(
                    {"_id": video_id},
                    {"$set": {
                        "status": "DONE",
                        "frames": frame_count,
                        "detection_stats": dict(detection_stats),
                        "result_key": output_key,
                        "updated_at": datetime.datetime.utcnow()
                    }}
                )

                r.xack("video_jobs", "workers", message_id)
                print(f"[{video_id}] done ({frame_count} frames)")

            except Exception as e:
                print(f"[{video_id}] FAILED:", e)

                videos.update_one(
                    {"_id": video_id},
                    {"$set": {
                        "status": "FAILED",
                        "error": str(e),
                        "updated_at": datetime.datetime.utcnow()
                    }}
                )
