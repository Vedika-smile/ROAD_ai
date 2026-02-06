const { Schema, model } = require("mongoose");

const LocationSchema = new Schema(
  {
    latitude: {
      type: Number,
      required: true,
      min: -90,
      max: 90,
    },
    longitude: {
      type: Number,
      required: true,
      min: -180,
      max: 180,
    },
    address: {
      type: String,
      required: false,
    },
  },
  { _id: false }
);

const PotholeSchema = new Schema(
  {
    location: {
      type: LocationSchema,
      required: true,
    },
    videoUrl: {
      type: String,
      required: true,
    },
    frameTime: {
      type: Number,
      required: false,
    },
    confidence: {
      type: Number,
      min: 0,
      max: 1,
      required: false,
    },
    status: {
      type: String,
      enum: ["new", "verified", "fixed"],
      default: "new",
    },
    detectedAt: {
      type: Date,
      default: Date.now,
    },
    metadata: {
      type: Schema.Types.Mixed,
      required: false,
    },
  },
  {
    timestamps: true,
  }
);

module.exports = model("Pothole", PotholeSchema);

