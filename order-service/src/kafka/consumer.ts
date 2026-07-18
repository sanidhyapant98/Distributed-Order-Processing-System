import { Kafka } from "kafkajs";
import { prisma } from "../prisma.js";
import { withRetry } from "../utils/retry.js";
import { sendToDlq } from "./dlq.js";

const kafka = new Kafka({
  clientId: "order-service-consumer",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "order-service-group" });

const MAX_STATUS_UPDATE_RETRIES = 3;
const PAYMENT_EVENTS_TOPIC = "payment-events";

async function updateOrderStatus(
  orderId: string,
  status: "COMPLETED" | "FAILED",
  eventType: string,
  eventId: string | undefined,
  reason?: string
) {
  await prisma.$transaction(async (tx) => {
    const updatedOrder = await tx.order.update({
      where: {
        id: orderId,
      },
      data: {
        status,
      },
    });

    if (eventId) {
      // upsert (not create) — a retry may re-enter this after a previous
      // attempt's write committed but the attempt failed afterwards.
      await tx.processedPaymentEvent.upsert({
        where: { id: eventId },
        create: {
          id: eventId,
          eventType,
          orderId,
        },
        update: {},
      });
    }

    if (status === "COMPLETED") {
      console.log(`✅ Order ${orderId} marked as COMPLETED`);
    } else {
      console.log(`❌ Order ${orderId} marked as FAILED`);
      console.log(`   Reason:`, reason || "Payment declined");
    }
    console.log(`   Order Details:`, updatedOrder);
  });
}

export const startOrderConsumer = async () => {
  try {
    await consumer.connect();
    console.log("📡 Order Consumer connecting to Kafka...");

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
          console.log("\n📥 Order Service received payment event:", data);

          const eventId: string | undefined = data.eventId;

          if (eventId) {
            // IDEMPOTENCY CHECK: have we already processed this exact event?
            const alreadyProcessed = await prisma.processedPaymentEvent.findUnique({
              where: {
                id: eventId,
              },
            });
            if (alreadyProcessed) {
              console.log(
                `⏭️  Duplicate event detected (eventId: ${eventId}). Skipping — already processed at ${alreadyProcessed.processedAt.toISOString()}`
              );
              return; // ack the offset, do nothing else
            }
          } else {
            // Defensive: if an event somehow has no eventId, we can't
            // dedupe it. Log loudly and process it anyway rather than crash.
            console.warn("⚠️  Event has no eventId — cannot deduplicate. Processing anyway.");
          }

          if (data.type === "PAYMENT_SUCCESS" || data.type === "PAYMENT_FAILED") {
            const status = data.type === "PAYMENT_SUCCESS" ? "COMPLETED" : "FAILED";

            try {
              await withRetry(
                () => updateOrderStatus(data.orderId, status, data.type, eventId, data.reason),
                {
                  retries: MAX_STATUS_UPDATE_RETRIES,
                  baseDelayMs: 300,
                  onRetry: (err, attempt, delayMs) => {
                    console.warn(
                      `🔁 [order ${data.orderId}] retrying order-status update (attempt ${attempt}/${MAX_STATUS_UPDATE_RETRIES}) in ${delayMs}ms — ${
                        err instanceof Error ? err.message : err
                      }`
                    );
                  },
                }
              );
            } catch (err) {
              // Bounded: stop here rather than retrying forever. Instead
              // of just logging and losing this status update (the order
              // would stay PENDING forever), send it to the DLQ so it can
              // be manually reprocessed.
              console.error(
                `🚨 [order ${data.orderId}] Giving up on order-status update after ${MAX_STATUS_UPDATE_RETRIES} retries:`,
                err
              );
              await sendToDlq({
                originalTopic: PAYMENT_EVENTS_TOPIC,
                failedAt: new Date().toISOString(),
                error: err instanceof Error ? err.message : String(err),
                originalPayload: data,
                eventId,
              });
            }
          } else {
            console.log(`⏭️  Ignored event type: ${data.type}`);
          }
        } catch (err) {
          // Guards against things outside the block above — most commonly
          // a malformed/unparseable ("poison") message.
          console.error("⚠️  Error processing payment event:", err);
          await sendToDlq({
            originalTopic: PAYMENT_EVENTS_TOPIC,
            failedAt: new Date().toISOString(),
            error: err instanceof Error ? err.message : String(err),
            originalPayload: raw ?? null,
          });
        }
      },
    });
  } catch (err) {
    console.error("❌ Order consumer error:", err);
    process.exit(1);
  }
};

export const disconnectOrderConsumer = async () => {
  try {
    await consumer.disconnect();
    console.log("🔌 Order consumer disconnected");
  } catch (err) {
    console.error("Error disconnecting order consumer:", err);
  }
};