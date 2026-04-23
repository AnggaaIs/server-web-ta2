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

const wss = new WebSocket.Server({ server });

const clients = new Map();

mongoose
  .connect(
    "mongodb+srv://tasya:tasya@pupuk-kompos.1pmdy.mongodb.net/?retryWrites=true&w=majority&appName=pupuk-kompos",
    {},
  )
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

wss.on("connection", function connection(ws, req) {
  const clientId = Date.now();
  let saveInterval;
  let dataObj;

  const clientIP = req.socket.remoteAddress;
  clients.set(clientId, ws);

  console.log(
    `🟢 [CONNECT] Klien terhubung | ID: ${clientId} | IP: ${clientIP}`,
  );

  saveInterval = setInterval(saveDataToMongoDB, 5 * 60_000);

  ws.on("message", function incoming(message) {
    try {
      console.log(`📨 [MESSAGE] dari client ${clientId}: ${message.toString()}`);
      dataObj = JSON.parse(String(message).replace(/\r\n/g, ""));

      if (
        dataObj.kelembaban === undefined ||
        dataObj.suhu === undefined ||
        dataObj.jarak === undefined
      ) {
        console.log(
          `⚠️ [SKIP] Data tidak lengkap dari client ${clientId}`,
          dataObj,
        );
        return;
      }

      // Pastikan format tipe data benar untuk diteruskan ke frontend
      dataObj.kelembaban = parseInt(dataObj.kelembaban);
      dataObj.suhu = parseFloat(dataObj.suhu);
      dataObj.kapasitas = parseInt(dataObj.jarak); // Frontend mengharapkan 'kapasitas'

      // Kirim data ke semua client lain
      clients.forEach((client, id) => {
        if (id !== clientId && client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify(dataObj));
          console.log(`📤 [FORWARD] ke client ${id}`);
        }
      });
    } catch (err) {
      console.error(
        `❌ [ERROR] Parsing message dari client ${clientId}: ${err.message}`,
      );
    }
  });

  ws.on("close", (code, reason) => {
    clients.delete(clientId);
    clearInterval(saveInterval);
    console.log(
      `🔴 [DISCONNECT] Client ${clientId} putus | Code: ${code} | Reason: ${reason}`,
    );
  });

  ws.on("error", (error) => {
    console.error(`❌ [SOCKET ERROR] Client ${clientId}: ${error.message}`);
  });

  // Ping-pong log
  ws.on("pong", () => {
    console.log(`🏓 [PONG] dari client ${clientId}`);
  });

  // Ping setiap 30 detik
  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
      console.log(`📡 [PING] ke client ${clientId}`);
    } else {
      clearInterval(pingInterval);
    }
  }, 30000);

  function saveDataToMongoDB() {
    if (
      !dataObj ||
      isNaN(dataObj.kelembaban) ||
      isNaN(dataObj.suhu) ||
      isNaN(dataObj.jarak)
    )
      return;

    const newData = new SensorData({
      kelembaban: dataObj.kelembaban,
      suhu: dataObj.suhu,
      kapasitas: dataObj.jarak,
      kapasitas_penuh: dataObj.kapasitas_penuh,
    });

    newData
      .save()
      .then((data) =>
        console.log("Data sensor berhasil disimpan ke MongoDB", data),
      )
      .catch((err) => console.error("Error menyimpan data sensor:", err));
  }
});

server.listen(PORT, () => {
  console.log(`Server berjalan di port ${PORT}`);
});
