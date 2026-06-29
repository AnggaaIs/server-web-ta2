const WebSocket = require("ws");
const mongoose = require("mongoose");
const SensorData = require("./models/SensorData");
const express = require("express");
const http = require("http");
const cors = require("cors");

const PORT = process.env.PORT || 3030;

const app = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json());

const wss = new WebSocket.Server({ server });
const clients = new Map();
const pingIntervals = new Map();

const MONGODB_URI =
  process.env.MONGODB_URI ||
  "mongodb+srv://tasya:tasya@pupuk-kompos.1pmdy.mongodb.net/?retryWrites=true&w=majority&appName=pupuk-kompos";

mongoose
  .connect(MONGODB_URI, {})
  .then(() => console.log("Terhubung ke MongoDB"))
  .catch((err) => console.error("Error koneksi MongoDB:", err));

app.get("/sensor-data", async (req, res) => {
  try {
    const sensorData = await SensorData.find().sort({ timestamp: -1 });
    res.json({ data: sensorData });
  } catch (err) {
    console.error("Error mengambil data sensor:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

app.get("/", async (req, res) => {
  res.status(200).json({ message: "halo" });
});

function parseNumber(value) {
  const parsed = typeof value === "string" ? Number(value) : value;
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeStatus(value) {
  if (typeof value === "string") {
    return value.toLowerCase() === "on" ? "on" : "off";
  }

  return value ? "on" : "off";
}

function normalizePump(pump = {}) {
  return {
    status: normalizeStatus(pump.status),
    water_level: parseNumber(pump.water_level),
  };
}

function normalizePayload(rawMessage) {
  const rawText = String(rawMessage).replace(/\r\n/g, "");
  const payload = JSON.parse(rawText);

  const kelembaban = parseNumber(payload.kelembaban);
  const suhu = parseNumber(payload.suhu);
  const jarak = parseNumber(payload.jarak ?? payload.kapasitas ?? payload.volume);

  if (kelembaban === null || suhu === null || jarak === null) {
    return null;
  }

  const waterPump1 = normalizePump(payload.water_pump_1);
  const waterPump2 = normalizePump(payload.water_pump_2);
  const stepper = normalizeStatus(payload.stepper);
  const kapasitasPenuh =
    typeof payload.kapasitas_penuh === "boolean"
      ? payload.kapasitas_penuh
      : jarak >= 70;

  return {
    kelembaban,
    suhu,
    jarak,
    kapasitas: jarak,
    water_pump_1: waterPump1,
    water_pump_2: waterPump2,
    stepper,
    kapasitas_penuh: kapasitasPenuh,
    timestamp: new Date(),
  };
}

function broadcastToOtherClients(senderId, dataObj) {
  const payload = JSON.stringify(dataObj);

  clients.forEach((client, id) => {
    if (id !== senderId && client.readyState === WebSocket.OPEN) {
      client.send(payload);
      console.log(`📤 [FORWARD] ke client ${id}`);
    }
  });
}

wss.on("connection", function connection(ws, req) {
  const clientId = Date.now();
  const clientIP = req.socket.remoteAddress;

  clients.set(clientId, ws);
  console.log(
    `🟢 [CONNECT] Klien terhubung | ID: ${clientId} | IP: ${clientIP}`,
  );

  ws.on("message", async function incoming(message) {
    try {
      console.log(`📨 [MESSAGE] dari client ${clientId}: ${message.toString()}`);

      const dataObj = normalizePayload(message);

      if (!dataObj) {
        console.log(
          `⚠️ [SKIP] Data tidak lengkap/tidak valid dari client ${clientId}`,
        );
        return;
      }

      const newData = new SensorData(dataObj);
      await newData.save();

      broadcastToOtherClients(clientId, dataObj);
      console.log("Data sensor berhasil disimpan ke MongoDB");
    } catch (err) {
      console.error(
        `❌ [ERROR] Parsing message dari client ${clientId}: ${err.message}`,
      );
    }
  });

  ws.on("close", (code, reason) => {
    clients.delete(clientId);

    const pingInterval = pingIntervals.get(clientId);
    if (pingInterval) {
      clearInterval(pingInterval);
      pingIntervals.delete(clientId);
    }

    console.log(
      `🔴 [DISCONNECT] Client ${clientId} putus | Code: ${code} | Reason: ${reason}`,
    );
  });

  ws.on("error", (error) => {
    console.error(`❌ [SOCKET ERROR] Client ${clientId}: ${error.message}`);
  });

  ws.on("pong", () => {
    console.log(`🏓 [PONG] dari client ${clientId}`);
  });

  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
      console.log(`📡 [PING] ke client ${clientId}`);
    } else {
      clearInterval(pingInterval);
      pingIntervals.delete(clientId);
    }
  }, 30000);

  pingIntervals.set(clientId, pingInterval);
});

server.listen(PORT, () => {
  console.log(`Server berjalan di port ${PORT}`);
});
