import json
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import RedirectResponse
from pymongo import MongoClient
import boto3
from botocore.exceptions import ClientError
import redis, uuid, os, datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

app = FastAPI()

# -------------------- DB / STORAGE --------------------

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


@app.on_event("startup")
def ensure_bucket():
    bucket = os.getenv("S3_BUCKET", "videos")
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass


# -------------------- HELPERS --------------------

def _presigned_url(key: str, expires_in: int = 3600) -> str:
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": os.getenv("S3_BUCKET"), "Key": key},
        ExpiresIn=expires_in,
    )


# -------------------- RESULT ACCESS (METHOD 1) --------------------
# Redirects to presigned S3 URL using filename only

@app.get("/results/{filename:path}")
def get_result_video(filename: str):
    bucket = os.getenv("S3_BUCKET")

    try:
        s3.head_object(Bucket=bucket, Key=filename)
    except ClientError:
        raise HTTPException(404, "Result video not found")

    return RedirectResponse(_presigned_url(filename))


# -------------------- CCTV --------------------

@app.get("/cctv/{video_id}")
def get_cctv(video_id: str):
    doc = videos.find_one({"_id": "results/" + video_id})
    if not doc:
        raise HTTPException(404, "Video not found")

    bucket = os.getenv("S3_BUCKET")
    original_key = f"{video_id}.mp4"

    try:
        s3.head_object(Bucket=bucket, Key=original_key)
        video_url = _presigned_url(original_key)
    except ClientError:
        video_url = None

    result_key = doc.get("result_key")
    if result_key:
        try:
            s3.head_object(Bucket=bucket, Key=result_key)
            result_url = _presigned_url(result_key)
        except ClientError:
            result_url = None
    else:
        result_url = None

    return {
        "video_id": doc["_id"],
        "filename": doc.get("filename"),
        "status": doc.get("status"),
        "frames": doc.get("frames"),
        "gps_coords": doc.get("gps_coords"),
        "source": doc.get("source"),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
        "video_url": video_url,
        "result_key": doc.get("result_key"),
        "result_url": result_url,
    }


@app.get("/cctv")
def list_cctv_videos():
    docs = videos.find({"source": "CCTV"})
    return [
        {
            "video_id": d["_id"],
            "filename": d.get("filename"),
            "status": d.get("status"),
            "frames": d.get("frames"),
            "gps_coords": d.get("gps_coords"),
            "source": d.get("source"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "result_key": d.get("result_key"),
        }
        for d in docs
    ]


@app.put("/cctv")
async def upload_cctv_video(
    file: UploadFile = File(...),
    gps_coords: str = Form(...),
):
    try:
        coords = json.loads(gps_coords)
    except json.JSONDecodeError:
        raise HTTPException(422, "gps_coords must be valid JSON")

    if not isinstance(coords, list) or not coords:
        raise HTTPException(422, "gps_coords must be a non-empty list")

    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Uploaded file is empty")

    video_id = str(uuid.uuid4())

    videos.insert_one({
        "_id": video_id,
        "source": "CCTV",
        "filename": file.filename,
        "status": "UPLOADED",
        "gps_coords": coords,
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "updated_at": datetime.datetime.now(datetime.timezone.utc),
    })

    s3.put_object(
        Bucket=os.getenv("S3_BUCKET"),
        Key=f"{video_id}.mp4",
        Body=contents,
        ContentType=file.content_type or "video/mp4",
    )

    r.xadd(
        "vehicle_count_jobs",
        {"video_id": video_id, "gps_coords": json.dumps(coords)},
    )

    return {"video_id": video_id, "status": "UPLOADED", "source": "CCTV"}


# -------------------- RDD VIDEOS --------------------

@app.post("/videos")
async def upload_video(
    file: UploadFile = File(...),
    gps_coords: str = Form(...),
):
    try:
        coords = json.loads(gps_coords)
    except json.JSONDecodeError:
        raise HTTPException(422, "gps_coords must be valid JSON")

    if not isinstance(coords, list):
        raise HTTPException(422, "gps_coords must be a list")

    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Uploaded file is empty")

    video_id = str(uuid.uuid4())

    videos.insert_one({
        "_id": video_id,
        "filename": file.filename,
        "status": "UPLOADED",
        "frames": None,
        "gps_coords": coords,
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "updated_at": datetime.datetime.now(datetime.timezone.utc),
    })

    s3.put_object(
        Bucket=os.getenv("S3_BUCKET"),
        Key=f"{video_id}.mp4",
        Body=contents,
        ContentType=file.content_type or "video/mp4",
    )

    r.xadd("video_jobs", {"video_id": video_id})

    return {"video_id": video_id}


@app.get("/videos/{video_id}")
def get_video(video_id: str):
    doc = videos.find_one({"_id": video_id})
    if not doc:
        raise HTTPException(404, "Video not found")

    bucket = os.getenv("S3_BUCKET")
    original_key = f"{video_id}.mp4"

    try:
        s3.head_object(Bucket=bucket, Key=original_key)
        video_url = _presigned_url(original_key)
    except ClientError:
        video_url = None

    result_key = doc.get("result_key")
    if result_key:
        try:
            s3.head_object(Bucket=bucket, Key=result_key)
            result_url = _presigned_url(result_key)
        except ClientError:
            result_url = None
    else:
        result_url = None

    return {
        "video_id": doc["_id"],
        "filename": doc.get("filename"),
        "status": doc.get("status"),
        "frames": doc.get("frames"),
        "gps_coords": doc.get("gps_coords"),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
        "result_key": doc.get("result_key"),
        "video_url": video_url,
        "result_url": result_url,
    }


@app.get("/videos")
def list_videos():
    docs = videos.find({})
    return [
        {
            "video_id": d["_id"],
            "filename": d.get("filename"),
            "status": d.get("status"),
            "frames": d.get("frames"),
            "gps_coords": d.get("gps_coords"),
            "source": d.get("source"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "result_key": d.get("result_key"),
        }
        for d in docs
    ]


@app.get("/")
def root():
    return {"message": "Hello World"}
