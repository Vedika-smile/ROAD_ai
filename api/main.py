import json
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pymongo import MongoClient
import boto3, redis, uuid, os, datetime

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

@app.post("/videos")
async def upload_video(
    file: UploadFile = File(...),
    gps_coords: str = Form(..., description="JSON array of [lat, lng] coordinates, e.g. [[52.52, 13.405], [52.53, 13.41]]"),
):
    try:
        coords = json.loads(gps_coords)
    except json.JSONDecodeError:
        raise HTTPException(422, "gps_coords must be valid JSON")
    if not isinstance(coords, list):
        raise HTTPException(422, "gps_coords must be a list")

    vid = str(uuid.uuid4())

    s3.upload_fileobj(file.file, os.getenv("S3_BUCKET"), f"{vid}.mp4")

    videos.insert_one({
        "_id": vid,
        "filename": file.filename,
        "status": "UPLOADED",
        "frames": None,
        "gps_coords": coords,
        "created_at": datetime.datetime.utcnow(),
        "updated_at": datetime.datetime.utcnow()
    })

    r.xadd("video_jobs", {"video_id": vid})

    return {"video_id": vid}

@app.get("/videos/{video_id}")
def get_video(video_id: str):
    doc = videos.find_one({"_id": video_id}, {"_id": 0})
    return doc


@app.get("/")
def root():
    return {"message": "Hello World"}
