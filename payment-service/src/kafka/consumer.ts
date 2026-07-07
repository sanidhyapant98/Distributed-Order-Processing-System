import { Kafka } from "kafkajs";
import { handlePayment } from "../handlers/payment.handler";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "payment-group" });

export const startConsumer = async () => {
  await consumer.connect();

  await consumer.subscribe({
    topic: "order-events",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value!.toString());

        console.log("📥 Payment Service received:", data);

        if (data.type === "ORDER_CREATED") {
          await handlePayment(data);
        } else {
          console.log(`⏭️ Ignored event type: ${data.type}`);
        }
      } catch (err) {
        // handlePayment already guards against most errors internally, but
        // this is a safety net for anything outside it (e.g. malformed
        // JSON), so one bad message can never crash the whole consumer.
        console.error("⚠️  Error processing order event:", err);
      }
    },
  });
};