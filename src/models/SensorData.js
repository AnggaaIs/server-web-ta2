const mongoose = require("mongoose");

const pumpSchema = new mongoose.Schema(
  {
    status: {
      type: String,
      enum: ["on", "off"],
      default: "off",
    },
    water_level: {
      type: Number,
      default: null,
    },
  },
  { _id: false },
);

const sensorDataSchema = new mongoose.Schema({
  kelembaban: {
    type: Number,
    required: true,
  },
  suhu: {
    type: Number,
    required: true,
  },
  jarak: {
    type: Number,
    required: true,
  },
  kapasitas: {
    type: Number,
    required: true,
  },
  water_pump_1: {
    type: pumpSchema,
    default: () => ({}),
  },
  water_pump_2: {
    type: pumpSchema,
    default: () => ({}),
  },
  stepper: {
    type: String,
    enum: ["on", "off"],
    default: "off",
  },
  kapasitas_penuh: {
    type: Boolean,
    default: false,
  },
  timestamp: {
    type: Date,
    default: Date.now,
  },
});

const SensorData = mongoose.model("SensorData", sensorDataSchema);

module.exports = SensorData;
