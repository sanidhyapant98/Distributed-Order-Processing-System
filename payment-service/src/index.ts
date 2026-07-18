import dotenv from 'dotenv';
dotenv.config();
import { connectProducer } from "./kafka/producer.js";
import { startConsumer } from "./kafka/consumer.js";

const start = async () => {
  try {
    console.log("🚀 Starting Payment Service...");
    
    await connectProducer();
    console.log("✅ Payment Service Producer connected to Kafka");
    
    await startConsumer();
    console.log("✅ Payment Service Consumer started - listening for order-events");

    console.log("\n💰 Payment Service is running");
    console.log("📥 Consumer: Listening for order-events from Kafka");
    console.log("📤 Producer: Sending payment-events to Kafka\n");
  } catch (err) {
    console.error("❌ Failed to start Payment Service:", err);
    process.exit(1);
  }
};

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down Payment Service...');
  process.exit(0);
});

start();