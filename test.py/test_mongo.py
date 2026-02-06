from pymongo import MongoClient

MONGO_URI = "PASTE_YOUR_MONGODB_CONNECTION_STRING_HERE"

client = MongoClient(MONGO_URI)
db = client.road_monitoring
collection = db.potholes

collection.insert_one({
    "message": "MongoDB connection successful"
})

print("✅ MongoDB connected and data inserted")
