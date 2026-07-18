import 'dotenv/config';
import app from "./app.js";
import { connectProducer } from "./kafka/producer.js";
import { startOrderConsumer } from "./kafka/consumer.js";
import { startOutboxPoller, stopOutboxPoller } from './kafka/outbox-poller.js';

const PORT = process.env.PORT || 5000;

const startServer = async () => {
  try {
    console.log("🚀 Starting Order Service...");
    
    // Connect producer for sending order events
    await connectProducer();
    console.log("✅ Order Service Producer connected to Kafka");
    
    // Start consumer to listen for payment events
    await startOrderConsumer();
    console.log("✅ Order Service Consumer started - listening for payment events");

    startOutboxPoller();

    app.listen(PORT, () => {
      console.log(`\n🎯 Order Service is running on http://localhost:${PORT}`);
      console.log("📤 Producer: Sending order-events to Kafka");
      console.log("📥 Consumer: Listening for payment-events from Kafka\n");
    });
  } catch (err) {
    console.error("❌ Failed to start Order Service:", err);
    process.exit(1);
  }
};

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down Order Service...');
  stopOutboxPoller();
  process.exit(0);
});

startServer();