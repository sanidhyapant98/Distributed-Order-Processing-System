import { Kafka } from "kafkajs";
import { handleInventory } from "../handlers/inventory.handler.js";
import { sendToDlq } from "./dlq.js";

const kafka = new Kafka({
  clientId: "inventory-service",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "inventory-group" });

const PAYMENT_EVENTS_TOPIC = "payment-events";

export const startConsumer = async () => {
  await consumer.connect();
  console.log("📡 Inventory Consumer connected to Kafka");

  await consumer.subscribe({
    topic: PAYMENT_EVENTS_TOPIC,
    fromBeginning: false,
  });

  console.log("👂 Listening on topic: payment-events");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      try {
        const data = JSON.parse(raw ?? "");

        console.log("\n📥 Inventory Service received event:", data);

        if (data.type === "PAYMENT_SUCCESS") {
          await handleInventory(data);
        } else {
          console.log(`⏭️ Ignored event type: ${data.type}`);
        }

      } catch (err) {
        // handleInventory already routes its own failures to the DLQ.
        // This catch only guards against things outside it — most
        // commonly a malformed/unparseable ("poison") message.
        console.error("⚠️ Error processing message:", err);
        await sendToDlq({
          originalTopic: PAYMENT_EVENTS_TOPIC,
          failedAt: new Date().toISOString(),
          error: err instanceof Error ? err.message : String(err),
          originalPayload: raw ?? null,
        });
      }
    },
  });
};