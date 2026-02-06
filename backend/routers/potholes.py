from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query

from backend.db import get_potholes_collection
from backend.models import PotholeCreate, PotholeOut, PotholeUpdate

router = APIRouter(prefix="/api/potholes", tags=["potholes"])


def _serialize_pothole(doc) -> PotholeOut:
    return PotholeOut(
        id=str(doc["_id"]),
        location=doc["location"],
        video_url=doc["video_url"],
        frame_time=doc.get("frame_time"),
        confidence=doc.get("confidence"),
        status=doc.get("status", "new"),
        detected_at=doc.get("detected_at", datetime.utcnow()),
        metadata=doc.get("metadata"),
    )


@router.post("", response_model=PotholeOut)
def create_pothole(pothole: PotholeCreate):
    """
    Store a new pothole detection.
    This is intended to be called from your YOLO script after uploading a video to Cloudinary.
    """
    collection = get_potholes_collection()

    doc = {
        "location": pothole.location.model_dump(),
        "video_url": str(pothole.video_url),
        "frame_time": pothole.frame_time,
        "confidence": pothole.confidence,
        "metadata": pothole.metadata,
        "status": "new",
        "detected_at": datetime.utcnow(),
    }

    result = collection.insert_one(doc)
    created = collection.find_one({"_id": result.inserted_id})
    return _serialize_pothole(created)


@router.get("", response_model=List[PotholeOut])
def list_potholes(
    status: Optional[str] = Query(
        default=None, description="Filter by status (e.g. 'new', 'verified', 'fixed')."
    ),
    limit: int = Query(default=100, ge=1, le=500),
    skip: int = Query(default=0, ge=0),
):
    """
    List stored pothole detections, optionally filtered by status.
    """
    collection = get_potholes_collection()

    query = {}
    if status:
        query["status"] = status

    docs = collection.find(query).skip(skip).limit(limit).sort("detected_at", -1)
    return [_serialize_pothole(doc) for doc in docs]


@router.get("/{pothole_id}", response_model=PotholeOut)
def get_pothole(pothole_id: str):
    """
    Fetch a single pothole by ID.
    """
    collection = get_potholes_collection()

    try:
        oid = ObjectId(pothole_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid pothole ID")

    doc = collection.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Pothole not found")

    return _serialize_pothole(doc)


@router.patch("/{pothole_id}", response_model=PotholeOut)
def update_pothole(pothole_id: str, updates: PotholeUpdate):
    """
    Update fields such as status or metadata.
    """
    collection = get_potholes_collection()

    try:
        oid = ObjectId(pothole_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid pothole ID")

    update_doc = {k: v for k, v in updates.model_dump(exclude_none=True).items()}
    if not update_doc:
        raise HTTPException(status_code=400, detail="No fields to update")

    result = collection.find_one_and_update(
        {"_id": oid},
        {"$set": update_doc},
        return_document=True,
    )

    if not result:
        raise HTTPException(status_code=404, detail="Pothole not found")

    return _serialize_pothole(result)

