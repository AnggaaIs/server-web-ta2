const mongoose = require('mongoose');

const sensorDataSchema = new mongoose.Schema({
    kapasitas: {
        type: Number,
        required: true
    },
    suhu: {
        type: Number,
        required: true
    },
    kelembaban: {
        type: Number,
        required: true
    },
    timestamp: {
        type: Date,
        default: Date.now
    }
});

const SensorData = mongoose.model('SensorData', sensorDataSchema);

module.exports = SensorData;