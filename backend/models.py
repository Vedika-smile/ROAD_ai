from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, HttpUrl


class Location(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    address: Optional[str] = None


class PotholeCreate(BaseModel):
    location: Location
    video_url: HttpUrl
    frame_time: Optional[float] = Field(
        None, description="Time in seconds within the video where the pothole appears."
    )
    confidence: Optional[float] = Field(
        None, ge=0, le=1, description="Model confidence score between 0 and 1."
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional extra data such as bounding boxes, model name, etc.",
    )


class PotholeUpdate(BaseModel):
    status: Optional[str] = Field(
        default=None,
        description="Status of the pothole, e.g. 'new', 'verified', 'fixed'.",
    )
    metadata: Optional[Dict[str, Any]] = None


class PotholeOut(BaseModel):
    id: str = Field(..., description="Stringified MongoDB ObjectId")
    location: Location
    video_url: HttpUrl
    frame_time: Optional[float] = None
    confidence: Optional[float] = None
    status: str
    detected_at: datetime
    metadata: Optional[Dict[str, Any]] = None

