const WebSocket = require('ws');
const mongoose = require("mongoose");
const SensorData = require("./models/SensorData");
const express = require("express");
const http = require("http");
const cors = require("cors")

const PORT = process.env.PORT || 3030;

const app = express();
const server = http.createServer(app);

app.use(cors())

const wss = new WebSocket.Server({ server });

const clients = new Map();

mongoose.connect('mongodb+srv://tasya:tasya@pupuk-kompos.1pmdy.mongodb.net/?retryWrites=true&w=majority&appName=pupuk-kompos', {
})
    .then(() => console.log('Terhubung ke MongoDB'))
    .catch(err => console.error('Error koneksi MongoDB:', err));


app.get('/sensor-data', async (req, res) => {
    try {
        const sensorData = await SensorData.find().sort({ timestamp: -1 });
        res.json({ data: sensorData });
    } catch (err) {
        console.error('Error mengambil data sensor:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

app.get('/', async (req, res) => {
    res.status(200).json({ message: "halo" })
})

wss.on('connection', function connection(ws) {
    const clientId = Date.now();
    let saveInterval;
    let dataObj; // Variabel untuk menyimpan data terbaru
    clients.set(clientId, ws);
    console.log(`Klien terhubung dengan ID: ${clientId}`);

    // Mulai interval penyimpanan data segera setelah klien terhubung
    saveInterval = setInterval(saveDataToMongoDB, 300_000);

    ws.on('message', function incoming(message) {
        dataObj = JSON.parse(String(message).replace(/\r\n/g, ''));
        dataObj = cleanJsonData(dataObj)
        if (!dataObj.kelembaban || dataObj.suhu == 0 || !dataObj.jarak) return;

        // 2. Membersihkan Data
        dataObj.kelembaban = parseInt(dataObj.kelembaban);
        dataObj.suhu = parseFloat(dataObj.suhu);
        dataObj.kapasitas = parseInt(dataObj.jarak);

        // Meneruskan pesan ke semua klien kecuali pengirim
        clients.forEach((client, id) => {
            if (id !== clientId && client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify(dataObj));
            }
        });
    });

    // Fungsi untuk menyimpan data ke MongoDB
    function saveDataToMongoDB() {
        if (
            !dataObj ||
            isNaN(dataObj.kelembaban) ||
            isNaN(dataObj.suhu) ||
            isNaN(dataObj.jarak)
        ) return;

        const newData = new SensorData({
            kelembaban: dataObj.kelembaban,
            suhu: dataObj.suhu,
            kapasitas: dataObj.jarak
        });

        newData.save()
            .then(() => console.log('Data sensor berhasil disimpan ke MongoDB'))
            .catch(err => console.error('Error menyimpan data sensor:', err));
    }


    ws.on('close', () => {
        clients.delete(clientId);
        console.log(`Klien dengan ID: ${clientId} terputus`);
        clearInterval(saveInterval);
    });

    // Menangani error
    ws.on('error', (error) => {
        console.error(`Error pada klien ${clientId}: ${error.message}`);
    });

    //  Ping klien secara berkala untuk memeriksa koneksi
    const pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.ping();
        } else {
            clearInterval(pingInterval);
        }
    }, 30000); // Ping setiap 30 detik
});

server.listen(PORT, () => {
    console.log(`Server berjalan di port ${PORT}`);
});

function cleanJsonData(inputJson) {
    const cleanedJson = { ...inputJson };

    // Fungsi untuk mengambil angka pertama dalam string
    const extractFirstNumber = (value) => {
        if (typeof value === "string") {
            const match = value.match(/\d+/);
            return match ? parseInt(match[0], 10) : null; // Ambil angka pertama atau null jika tidak ada
        }
        return value; // Jika bukan string, kembalikan nilai aslinya
    };

    // Fungsi untuk mengubah "on" menjadi true dan "off" menjadi false
    const cleanValue = (value) => {
        if (typeof value === "string") {
            const match = value.match(/\b(on|off)\b/);
            if (match) {
                return match[0] === "on"; // true jika "on", false jika "off"
            }
        }
        return null; // Jika tidak ditemukan, kembalikan null
    };

    // Bersihkan data
    cleanedJson.kelembaban = extractFirstNumber(cleanedJson.kelembaban);
    cleanedJson.jarak = extractFirstNumber(cleanedJson.jarak);
    cleanedJson.motor_1 = cleanValue(cleanedJson.motor_1);
    cleanedJson.motor_2 = cleanValue(cleanedJson.motor_2);
    cleanedJson.stepper = cleanValue(cleanedJson.stepper);

    return cleanedJson;
}