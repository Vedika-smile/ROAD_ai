from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routers import potholes

app = FastAPI(
    title="Road Pothole Detection API",
    description="Backend service to store pothole detections, locations, and Cloudinary video URLs.",
    version="0.1.0",
)

# Adjust CORS origins to match where your frontend / YOLO client is running.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"message": "Road Pothole Detection API is running"}


app.include_router(potholes.router)

