import { Kafka } from "kafkajs";
import { handlePayment } from "../handlers/payment.handler.js";
import { sendToDlq } from "./dlq.js";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "payment-group" });

const ORDER_EVENTS_TOPIC = "order-events";

export const startConsumer = async () => {
  await consumer.connect();

  await consumer.subscribe({
    topic: ORDER_EVENTS_TOPIC,
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      try {
        const data = JSON.parse(raw ?? "");

        console.log("📥 Payment Service received:", data);

        if (data.type === "ORDER_CREATED") {
          await handlePayment(data);
        } else {
          console.log(`⏭️ Ignored event type: ${data.type}`);
        }
      } catch (err) {
        // handlePayment already routes its own failures to the DLQ. This
        // catch only guards against things outside it — most commonly a
        // malformed/unparseable ("poison") message — so it can never
        // crash the whole consumer, and the raw message still ends up in
        // the DLQ instead of being silently dropped.
        console.error("⚠️  Error processing order event:", err);
        await sendToDlq({
          originalTopic: ORDER_EVENTS_TOPIC,
          failedAt: new Date().toISOString(),
          error: err instanceof Error ? err.message : String(err),
          originalPayload: raw ?? null,
        });
      }
    },
  });
};