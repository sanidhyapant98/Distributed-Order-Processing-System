import { prisma } from "../prisma";
import { producer } from "../kafka/producer";
import { randomUUID } from "node:crypto";
import { withRetry } from "../utils/retry";
import { sendToDlq } from "../kafka/dlq";

const MAX_PUBLISH_RETRIES = 3;
const ORDER_EVENTS_TOPIC = "order-events";

export const handlePayment = async (event: any) => {
  const { orderId, userId, productId, eventId } = event;

  try {
    if (eventId) {
      // IDEMPOTENCY CHECK: have we already processed this exact event?
      const alreadyProcessed = await prisma.processedOrderEvent.findUnique({
        where: {
          id: eventId,
        },
      });
      if (alreadyProcessed) {
        console.log(
          `⏭️  Duplicate ORDER_CREATED event detected (eventId: ${eventId}). Skipping — already processed at ${alreadyProcessed.processedAt.toISOString()}`
        );
        return;
      }
    } else {
      console.warn("⚠️  Event has no eventId — cannot deduplicate. Processing anyway.");
    }

    console.log(`\n💳 Processing payment for order: ${orderId}`);
    console.log(`   User: ${userId}, Product: ${productId}`);

    // Generated once, before any retries, so every retry of this same
    // incoming event reuses the same outgoing eventId. Downstream
    // consumers dedupe on this id, so a retried publish never causes
    // double-processing further down the pipeline.
    const paymentEventId = randomUUID();

    // Simulate payment processing (random success/failure - 70% success rate).
    // The outcome is decided once, up front — the retries below only cover
    // infrastructure hiccups (Kafka send / DB write), never re-roll this.
    const isSuccess = Math.random() > 0.3;

    // Add a small delay to simulate processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    const paymentEvent = isSuccess
      ? {
          type: "PAYMENT_SUCCESS",
          eventId: paymentEventId,
          orderId,
          userId,
          productId,
          timestamp: new Date().toISOString(),
        }
      : {
          type: "PAYMENT_FAILED",
          eventId: paymentEventId,
          orderId,
          userId,
          productId,
          timestamp: new Date().toISOString(),
          reason: "Payment declined - insufficient funds",
        };

    if (isSuccess) {
      console.log(`✅ Payment Success for order: ${orderId}`);
    } else {
      console.log(`❌ Payment Failed for order: ${orderId}`);
    }

    try {
      await withRetry(() => publishPaymentResult(paymentEvent, eventId, orderId), {
        retries: MAX_PUBLISH_RETRIES,
        baseDelayMs: 300,
        onRetry: (err, attempt, delayMs) => {
          console.warn(
            `🔁 [order ${orderId}] retrying ${paymentEvent.type} publish (attempt ${attempt}/${MAX_PUBLISH_RETRIES}) in ${delayMs}ms — ${
              err instanceof Error ? err.message : err
            }`
          );
        },
      });

      console.log(`📤 Published ${paymentEvent.type} event to Kafka\n`);
    } catch (err) {
      // Bounded: we tried MAX_PUBLISH_RETRIES times. Instead of just
      // logging and losing this payment result forever (the order would
      // sit as PENDING with no outcome), send it to the DLQ so it can be
      // manually republished later.
      console.error(
        `🚨 [order ${orderId}] Giving up on publishing ${paymentEvent.type} after ${MAX_PUBLISH_RETRIES} retries:`,
        err
      );
      await sendToDlq({
        originalTopic: "payment-events",
        failedAt: new Date().toISOString(),
        error: err instanceof Error ? err.message : String(err),
        originalPayload: paymentEvent,
        eventId: paymentEventId,
      });
    }
  } catch (err) {
    // Safety net for anything unexpected outside the retried block above
    // (e.g. the idempotency check itself failing). This is a message we
    // received but could not process at all, so it goes to the DLQ
    // rather than being silently dropped.
    console.error("❌ Unexpected error handling payment:", err);
    await sendToDlq({
      originalTopic: ORDER_EVENTS_TOPIC,
      failedAt: new Date().toISOString(),
      error: err instanceof Error ? err.message : String(err),
      originalPayload: event,
      eventId,
    });
  }
};

async function publishPaymentResult(
  paymentEvent: Record<string, unknown>,
  eventId: string | undefined,
  orderId: string
) {
  await producer.send({
    topic: "payment-events",
    messages: [
      {
        key: orderId,
        value: JSON.stringify(paymentEvent),
      },
    ],
  });

  if (eventId) {
    // upsert (not create) — a retry may re-run this after a previous
    // attempt's write already succeeded but the attempt failed afterwards.
    await prisma.processedOrderEvent.upsert({
      where: { id: eventId },
      create: {
        id: eventId,
        eventType: "ORDER_CREATED",
        orderId,
      },
      update: {},
    });
    console.log(`📌 Marked eventId ${eventId} as processed`);
  }
}