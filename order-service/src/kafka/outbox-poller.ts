import { prisma } from "@/prisma";
import { producer } from "./producer";
import { withRetry } from "../utils/retry";

const POLL_INTERVAL_MS = 5000;
const MAX_ATTEMPTS = 5;

let pollerTimer: NodeJS.Timeout | null = null;

// Runs one cycle: find unpublished events → publish → mark done
const runOnce = async () => {
  // Find events that haven't been published yet and haven't exceeded max attempts
  const pendingEvents = await prisma.orderEventOutbox.findMany({
    where: {
      publishedAt: null,
      publishAttempts: { lt: MAX_ATTEMPTS },
    },
    orderBy: {
      createdAt: "asc", // oldest first
    },
    take: 10,
  });

  if (pendingEvents.length === 0) {
    return;
  }

  console.log(`📬 Outbox poller: found ${pendingEvents.length} unpublished event(s)`);

  for (const event of pendingEvents) {
    try {
      // Fast, in-process bounded retry for a momentary blip (e.g. a brief
      // Kafka connection hiccup), on top of — not instead of — the
      // slower cross-cycle bound tracked by publishAttempts below.
      await withRetry(
        () =>
          producer.send({
            topic: "order-events",
            messages: [
              {
                key: event.orderId,
                value: JSON.stringify(event.eventPayload),
              },
            ],
          }),
        {
          retries: 2,
          baseDelayMs: 200,
          onRetry: (err, attempt, delayMs) => {
            console.warn(
              `🔁 Outbox: retrying publish for order ${event.orderId} (in-cycle attempt ${attempt}/2) in ${delayMs}ms — ${
                err instanceof Error ? err.message : err
              }`
            );
          },
        }
      );

      // Mark as published
      await prisma.orderEventOutbox.update({
        where: {
          id: event.id,
        },
        data: {
          publishedAt: new Date(),
        },
      });

      console.log(`✅ Published outbox event for order: ${event.orderId}`);
    } catch (err) {
      // Kafka stayed down through the in-cycle retries too — record the
      // attempt and try again next cycle, up to the cross-cycle bound.
      const attemptsSoFar = event.publishAttempts + 1;
      const errorMessage = err instanceof Error ? err.message : String(err);

      await prisma.orderEventOutbox.update({
        where: {
          id: event.id,
        },
        data: {
          publishAttempts: {
            increment: 1,
          },
          lastError: errorMessage,
        },
      });

      console.error(
        `⚠️  Failed to publish event for order ${event.orderId} (cross-cycle attempt ${attemptsSoFar}/${MAX_ATTEMPTS}):`,
        err
      );

      if (attemptsSoFar >= MAX_ATTEMPTS) {
        console.error(
          `🚨 Outbox event for order ${event.orderId} (eventId: ${event.id}) has exhausted its retry bound (${MAX_ATTEMPTS} attempts) and will no longer be retried automatically. Manual investigation required — see the lastError column.`
        );
      }
    }
  }
};

export const startOutboxPoller = () => {
  console.log(`🔄 Outbox poller started (every ${POLL_INTERVAL_MS / 1000}s)`);
  // Run immediately on start, then on interval
  runOnce();
  pollerTimer = setInterval(runOnce, POLL_INTERVAL_MS);
};

export const stopOutboxPoller = () => {
  if (pollerTimer) {
    clearInterval(pollerTimer);
    pollerTimer = null;
    console.log("🛑 Outbox poller stopped");
  }
};