import json
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pymongo import MongoClient
import boto3, redis, uuid, os, datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

app = FastAPI()

mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo[os.getenv("MONGO_DB")]
videos = db.videos

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)

r = redis.Redis.from_url(os.getenv("REDIS_URL"))

from datetime import datetime, timezone

@app.post("/videos")
async def upload_video(
    file: UploadFile = File(...),
    gps_coords: str = Form(...),
):
    # ---- validate GPS ----
    try:
        coords = json.loads(gps_coords)
    except json.JSONDecodeError:
        raise HTTPException(422, "gps_coords must be valid JSON")

    if not isinstance(coords, list):
        raise HTTPException(422, "gps_coords must be a list")

    # ---- generate id ----
    vid = str(uuid.uuid4())

    # ---- READ FILE SAFELY ----
    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Uploaded file is empty")

    # ---- DB INSERT (acts as lock) ----
    videos.insert_one({
        "_id": vid,
        "filename": file.filename,
        "status": "UPLOADED",
        "frames": None,
        "gps_coords": coords,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    })

    # ---- UPLOAD TO MINIO / S3 ----
    s3.put_object(
        Bucket=os.getenv("S3_BUCKET"),
        Key=f"{vid}.mp4",
        Body=contents,
        ContentType=file.content_type or "video/mp4",
    )

    # ---- ENQUEUE EXACTLY ONCE ----
    r.xadd("video_jobs", {"video_id": vid})

    return {"video_id": vid}


@app.get("/videos/{video_id}")
def get_video(video_id: str):
    doc = videos.find_one({"_id": video_id}, {"_id": 0})
    return doc

@app.get("/videos")
def list_videos():
    docs = videos.find({}, {"_id": 0})
    return list(docs)


@app.get("/")
def root():
    return {"message": "Hello World"}
