const express = require("express");
const Pothole = require("../models/Pothole");

const router = express.Router();

// POST /api/potholes - create a new pothole detection
router.post("/", async (req, res) => {
  try {
    const { location, videoUrl, frameTime, confidence, metadata } = req.body;

    if (!location || !location.latitude || !location.longitude || !videoUrl) {
      return res.status(400).json({
        message: "location.latitude, location.longitude, and videoUrl are required",
      });
    }

    const pothole = await Pothole.create({
      location,
      videoUrl,
      frameTime,
      confidence,
      metadata,
    });

    return res.status(201).json(pothole);
  } catch (err) {
    console.error("Error creating pothole:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

// GET /api/potholes - list potholes (optionally filtered by status)
router.get("/", async (req, res) => {
  try {
    const { status, limit = 100, skip = 0 } = req.query;

    const query = {};
    if (status) {
      query.status = status;
    }

    const potholes = await Pothole.find(query)
      .sort({ detectedAt: -1 })
      .skip(Number(skip))
      .limit(Number(limit));

    return res.json(potholes);
  } catch (err) {
    console.error("Error listing potholes:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

// GET /api/potholes/:id - get one pothole
router.get("/:id", async (req, res) => {
  try {
    const pothole = await Pothole.findById(req.params.id);
    if (!pothole) {
      return res.status(404).json({ message: "Pothole not found" });
    }
    return res.json(pothole);
  } catch (err) {
    console.error("Error getting pothole:", err);
    return res.status(400).json({ message: "Invalid ID" });
  }
});

// PATCH /api/potholes/:id - update status/metadata
router.patch("/:id", async (req, res) => {
  try {
    const { status, metadata } = req.body;

    const update = {};
    if (status !== undefined) update.status = status;
    if (metadata !== undefined) update.metadata = metadata;

    if (Object.keys(update).length === 0) {
      return res.status(400).json({ message: "No fields to update" });
    }

    const pothole = await Pothole.findByIdAndUpdate(
      req.params.id,
      { $set: update },
      { new: true }
    );

    if (!pothole) {
      return res.status(404).json({ message: "Pothole not found" });
    }

    return res.json(pothole);
  } catch (err) {
    console.error("Error updating pothole:", err);
    return res.status(400).json({ message: "Invalid ID" });
  }
});

module.exports = router;

