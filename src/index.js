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
      // console.log(`📨 [MESSAGE] dari client ${clientId}: ${message.toString()}`);
      dataObj = JSON.parse(String(message).replace(/\r\n/g, ""));
      dataObj = cleanJsonData(dataObj);

      if (
        isNaN(dataObj.kelembaban) ||
        dataObj.kelembaban == null ||
        dataObj.suhu === null ||
        isNaN(dataObj.suhu)
        // jangan cek jarak === 0 karena jarak boleh 0
      ) {
        console.log(
          `⚠️ [SKIP] Data tidak valid dari client ${clientId}`,
          dataObj,
        );
        return;
      }

      dataObj.kelembaban = parseInt(dataObj.kelembaban);
      dataObj.suhu = parseFloat(dataObj.suhu);
      dataObj.kapasitas = parseInt(dataObj.jarak);

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

function cleanJsonData(inputJson) {
  const cleanedJson = {};

  // Fungsi untuk mengambil angka pertama dalam string
  const extractFirstNumber = (value) => {
    if (typeof value === "string") {
      const match = value.match(/\d+(\.\d+)?/); // ambil angka integer atau desimal pertama
      return match ? parseFloat(match[0]) : null;
    } else if (typeof value === "number") {
      return value;
    }
    return null;
  };

  // Fungsi untuk mengubah "on" menjadi true dan "off" menjadi false
  const extractOnOff = (value) => {
    if (typeof value === "string") {
      const match = value.match(/\b(on|off)\b/i);
      if (match) {
        return match[0].toLowerCase() === "on";
      }
    }
    return null;
  };

  // Ambil data kelembaban (angka pertama dari string/number)
  cleanedJson.kelembaban = extractFirstNumber(inputJson.kelembaban);

  // Ambil data suhu (angka pertama dari string/number)
  cleanedJson.suhu = extractFirstNumber(inputJson.suhu);

  // Ambil data jarak (kapasitas) (angka pertama)
  cleanedJson.jarak = extractFirstNumber(inputJson.jarak);

  // Motor dan stepper, ambil on/off saja
  cleanedJson.motor_1 = extractOnOff(inputJson.motor_1);
  cleanedJson.motor_2 = extractOnOff(inputJson.motor_2);
  cleanedJson.stepper = extractOnOff(inputJson.stepper);
  cleanedJson.kapasitas_penuh =
    inputJson.kapasitas_penuh === true || inputJson.kapasitas_penuh === "true";

  return cleanedJson;
}
