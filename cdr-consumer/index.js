const express = require("express");
const amqp = require("amqplib");
const mongoose = require("mongoose");

const app = express();

// ---- CONFIG ----
const RABBIT_URL = "amqp://admin:admin@rabbitmq:5672";
const QUEUE = "cdr_queue";
const MONGO_URL = "mongodb://mongo:27017/cdr_db";

// ---- STATE ----
let channel;

// ---- Mongo Model ----
const CDRSchema = new mongoose.Schema({
  caller: String,
  receiver: String,
  duration: Number,
  timestamp: String
});

const CDR = mongoose.model("CDR", CDRSchema);

// ---- CONNECT MONGO ----
async function connectMongo() {
  await mongoose.connect(MONGO_URL);
  console.log("Connected to MongoDB");
}

// ---- CONNECT RABBIT ----
async function connectRabbit(retries = 10) {
  try {
    console.log("Connecting to RabbitMQ...");
    const conn = await amqp.connect(RABBIT_URL);
    channel = await conn.createChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    console.log("Connected to RabbitMQ");

    channel.consume(QUEUE, async (msg) => {
      if (!msg) return;

      const cdr = JSON.parse(msg.content.toString());
      console.log("Received CDR:", cdr);

      try {
        await CDR.create(cdr);
        channel.ack(msg);
        console.log("CDR saved & acknowledged");
      } catch (err) {
        console.error("DB error, message NOT acked", err);
      }
    });
  } catch (err) {
    console.log("RabbitMQ not ready, retrying...", retries);
    if (retries === 0) process.exit(1);
    setTimeout(() => connectRabbit(retries - 1), 5000);
  }
}

// ---- STARTUP ----
async function start() {
  await connectMongo();
  connectRabbit();
}

start();

// ---- Health ----
app.get("/", (req, res) => {
  res.send("CDR Consumer is running");
});

app.listen(3000, () => {
  console.log("Consumer running on port 3000");
});
