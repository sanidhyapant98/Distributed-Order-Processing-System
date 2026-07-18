// src/index.ts

import dotenv from "dotenv";
dotenv.config();

import { connectProducer } from "./kafka/producer.js";
import { startConsumer } from "./kafka/consumer.js";

const start = async () => {
  try {
    console.log("🚀 Starting Inventory Service...");

    await connectProducer();
    console.log("✅ Inventory Service Producer connected to Kafka");

    await startConsumer();

    console.log("✅ Inventory Service running");
    console.log("📥 Listening for payment-events\n");

  } catch (err) {
    console.error("❌ Failed to start Inventory Service:", err);
    process.exit(1);
  }
};

process.on("SIGINT", async () => {
  console.log("\n🛑 Shutting down Inventory Service...");
  process.exit(0);
});

start();