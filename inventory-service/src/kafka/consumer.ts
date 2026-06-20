// src/kafka/consumer.ts

import { Kafka } from "kafkajs";
import { handleInventory } from "../handlers/inventory.handler";

const kafka = new Kafka({
  clientId: "inventory-service",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "inventory-group" });

export const startConsumer = async () => {
  await consumer.connect();
  console.log("📡 Inventory Consumer connected to Kafka");

  await consumer.subscribe({
    topic: "payment-events",
    fromBeginning: false,
  });
  
  console.log("👂 Listening on topic: payment-events");

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value!.toString());

        console.log("\n📥 Inventory Service received event:", data);

        if (data.type === "PAYMENT_SUCCESS") {
          await handleInventory(data);
        } else {
          console.log(`⏭️ Ignored event type: ${data.type}`);
        }

      } catch (err) {
        console.error("⚠️ Error processing message:", err);
      }
    },
  });
};