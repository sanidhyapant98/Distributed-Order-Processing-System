import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "inventory-service",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

export const producer = kafka.producer();

export const connectProducer = async () => {
  await producer.connect();
};