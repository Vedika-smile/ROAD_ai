const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");

dotenv.config();

const connectDB = require("./config/db");
const potholeRoutes = require("./routes/potholeRoutes");

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.get("/", (_req, res) => {
  res.json({ message: "Road Pothole Detection API (Node.js) is running" });
});

app.use("/api/potholes", potholeRoutes);

// Start server after DB connection
const PORT = process.env.PORT || 5000;

connectDB().then(() => {
  app.listen(PORT, () => {
    console.log(`🚀 Server listening on port ${PORT}`);
  });
});

