import os
import redis
import boto3
import cv2
import datetime
from pymongo import MongoClient
from ultralytics import YOLO
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ---------- CONFIG ----------
MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "YOLOv8_Small_RDD.pt")
INPUT_TMP = "/tmp/input.mp4"
OUTPUT_TMP = "/tmp/output.mp4"
RESULT_PREFIX = "results/"
STREAM_NAME = "video_jobs"
GROUP_NAME = "workers"
# ----------------------------

# ---------- REDIS ----------
r = redis.Redis.from_url(os.getenv("REDIS_URL"))

# Create consumer group (safe if exists)
try:
    r.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
except redis.exceptions.ResponseError:
    pass

consumer = os.getenv("HOSTNAME", "worker-1")

# ---------- MONGO ----------
mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo[os.getenv("MONGO_DB")]
videos = db.videos

# ---------- MINIO / S3 ----------
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)

# ---------- YOLO ----------
if not os.path.exists(MODEL_PATH):
    raise RuntimeError(f"Model not found: {MODEL_PATH}")

model = YOLO(MODEL_PATH)

# # GPU (comment this line if CPU-only)
# try:
#     model.to("cuda")
#     print("YOLO running on GPU")
# except Exception:
#     print("YOLO running on CPU")

print("Worker ready")

# ========== WORKER LOOP ==========
while True:
    print("Waiting for jobs...")
    streams = r.xreadgroup(
        groupname=GROUP_NAME,
        consumername=consumer,
        streams={STREAM_NAME: ">"},
        count=1,
        block=5000
    )

    # Block timeout returns None or []; avoid crash on None
    if not streams:
        continue

    for _, messages in streams:
        for message_id, data in messages:
            video_id = data[b"video_id"].decode()
            input_key = f"{video_id}.mp4"
            output_key = f"{RESULT_PREFIX}{video_id}_detected.mp4"

            # ---------- IDEMPOTENCY CHECK ----------
            doc = videos.find_one({"_id": video_id})
            if doc:
                if doc.get("status") == "DONE":
                    r.xack(STREAM_NAME, GROUP_NAME, message_id)
                    print(f"[{video_id}] already DONE → skipped")
                    continue
                if doc.get("status") == "FAILED":
                    r.xack(STREAM_NAME, GROUP_NAME, message_id)
                    print(f"[{video_id}] already FAILED → skipped")
                    continue

            print(f"[{video_id}] processing")

            try:
                print(f"[{video_id}] marking PROCESSING")
                # Mark PROCESSING
                videos.update_one(
                    {"_id": video_id},
                    {"$set": {
                        "status": "PROCESSING",
                        "updated_at": datetime.datetime.utcnow()
                    }},
                    upsert=True
                )

                # Download input video
                print(f"[{video_id}] downloading from S3")
                s3.download_file(os.getenv("S3_BUCKET"), input_key, INPUT_TMP)

                cap = cv2.VideoCapture(INPUT_TMP)
                if not cap.isOpened():
                    raise RuntimeError("Cannot open video")

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

                # Save final result
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

                # ACK MESSAGE
                r.xack(STREAM_NAME, GROUP_NAME, message_id)
                print(f"[{video_id}] DONE ({frame_count} frames)")

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
                # ACK so this message is not re-delivered forever
                r.xack(STREAM_NAME, GROUP_NAME, message_id)


print("Worker stopped")
