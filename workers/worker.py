import redis, boto3, os, cv2, datetime
from pymongo import MongoClient


print(f'redis url: {os.getenv("REDIS_URL")}')
r = redis.Redis.from_url(os.getenv("REDIS_URL"))

mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo[os.getenv("MONGO_DB")]
videos = db.videos

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)

while True:
    streams = r.xread({"video_jobs": "0"}, block=0)

    for stream_name, messages in streams:
        for message_id, data in messages:
            vid = data[b"video_id"].decode()

            print(f"Processing {vid}")

            # do work here

            print(f"Done {vid}")
