const express = require("express");
const amqp = require("amqplib");

const app = express();
app.use(express.json());

const RABBIT_URL = "amqp://admin:admin@rabbitmq:5672";
const QUEUE = "cdr_queue";

let channel = null;

/**
 * Connect to RabbitMQ with retry
 */
async function connectRabbit(retries = 10) {
  try {
    console.log("Connecting to RabbitMQ...");
    const connection = await amqp.connect(RABBIT_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    console.log("Connected to RabbitMQ");
  } catch (err) {
    console.log("RabbitMQ not ready, retrying...", retries);
    if (retries === 0) {
      console.error("RabbitMQ connection failed permanently");
      process.exit(1);
    }
    setTimeout(() => connectRabbit(retries - 1), 5000);
  }
}

// IMPORTANT: start Rabbit connection
connectRabbit();

/**
 * Generate mock telecom CDR
 */
function generateCDR() {
  return {
    caller: "+91" + Math.floor(9000000000 + Math.random() * 1000000000),
    receiver: "+91" + Math.floor(9000000000 + Math.random() * 1000000000),
    duration: Math.floor(Math.random() * 300), // seconds
    timestamp: new Date().toISOString()
  };
}

/**
 * Publish CDR
 */
app.post("/cdr", (req, res) => {
  if (!channel) {
    return res.status(503).json({
      error: "RabbitMQ not ready. Try again shortly."
    });
  }

  const cdr = generateCDR();

  channel.sendToQueue(
    QUEUE,
    Buffer.from(JSON.stringify(cdr)),
    { persistent: true }
  );

  console.log("CDR published:", cdr);
  res.json({ status: "CDR published", cdr });
});

/**
 * Health check
 */
app.get("/", (req, res) => {
  res.send("CDR Producer is running");
});

app.listen(3000, () => {
  console.log("Producer running on port 3000");
});
