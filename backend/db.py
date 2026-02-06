import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

client = MongoClient(MONGO_URI)
db = client.road_monitoring
potholes_collection = db.potholes


def get_potholes_collection():
    """
    Simple accessor used by route handlers.
    """
    return potholes_collection

