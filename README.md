## Road Pothole Detection Backends

This project now contains **two backend options**:

- **Python (FastAPI)** – in the `backend` folder.
- **Node.js (Express + MongoDB)** – in the `server` folder.

You can use whichever stack you prefer. The API shape is similar for both.

---

### Node.js backend (`server/`)

**Tech stack**

- Express
- MongoDB (via Mongoose)
- dotenv, cors

**Setup**

1. Go to the `server` folder:

   ```bash
   cd server
   ```

2. Install dependencies:

   ```bash
   npm install
   ```

3. Configure MongoDB:

   - Copy `.env.example` to `.env`.
   - Set `MONGO_URI` to your MongoDB connection string.
   - Optionally change `PORT` (default is 5000).

4. Run the API server:

   ```bash
   npm run dev
   ```

   The API will be available at `http://127.0.0.1:5000`.

**API Overview (Node.js)**

- `GET /` – Health check.
- `POST /api/potholes` – Create/store a new pothole detection.
- `GET /api/potholes` – List pothole detections (optional `status`, `limit`, `skip`).
- `GET /api/potholes/:id` – Get a single pothole by ID.
- `PATCH /api/potholes/:id` – Update a pothole (e.g. status, metadata).

**Example request body for `POST /api/potholes`**

```json
{
  "location": {
    "latitude": 12.9716,
    "longitude": 77.5946,
    "address": "Some road, Bangalore"
  },
  "videoUrl": "https://res.cloudinary.com/your_cloud/video/upload/....mp4",
  "frameTime": 5.2,
  "confidence": 0.93,
  "metadata": {
    "bbox": [100, 200, 300, 400],
    "model": "yolov8n"
  }
}
```

In your YOLO Node.js or Python script, send this JSON to `http://127.0.0.1:5000/api/potholes` after uploading the video to Cloudinary.


