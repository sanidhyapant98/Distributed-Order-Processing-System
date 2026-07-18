import { prisma } from "../prisma.js";
import { producer } from "./producer.js";
import { withRetry } from "../utils/retry.js";

const POLL_INTERVAL_MS = 5000;
const MAX_ATTEMPTS = 5;

let pollerTimer: NodeJS.Timeout | null = null;
let isRunning = false;

const runOnce = async () => {
  // Prevent overlapping executions
  if (isRunning) {
    console.warn("⏳ Previous outbox poll is still running. Skipping this cycle.");
    return;
  }

  isRunning = true;

  try {
    const pendingEvents = await prisma.orderEventOutbox.findMany({
      where: {
        publishedAt: null,
        publishAttempts: {
          lt: MAX_ATTEMPTS,
        },
      },
      orderBy: {
        createdAt: "asc",
      },
      take: 10,
    });

    if (pendingEvents.length === 0) {
      return;
    }

    console.log(
      `📬 Outbox poller: found ${pendingEvents.length} unpublished event(s)`
    );

    for (const event of pendingEvents) {
      try {
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
                `🔁 Retrying publish for order ${event.orderId} (${attempt}/2) in ${delayMs}ms: ${
                  err instanceof Error ? err.message : err
                }`
              );
            },
          }
        );

        try {
          await prisma.orderEventOutbox.update({
            where: {
              id: event.id,
            },
            data: {
              publishedAt: new Date(),
            },
          });

          console.log(`✅ Published outbox event for order ${event.orderId}`);
        } catch (updateErr) {
          console.warn(
            `⚠️ Event ${event.id} was already updated by another poller or no longer exists.`
          );
          console.error(updateErr);
        }
      } catch (err) {
        const attemptsSoFar = event.publishAttempts + 1;
        const errorMessage =
          err instanceof Error ? err.message : String(err);

        try {
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
        } catch (updateErr) {
          console.error(
            `❌ Failed to record retry attempt for event ${event.id}`
          );
          console.error(updateErr);
        }

        console.error(
          `⚠️ Failed to publish event for order ${event.orderId} (${attemptsSoFar}/${MAX_ATTEMPTS})`
        );

        if (attemptsSoFar >= MAX_ATTEMPTS) {
          console.error(
            `🚨 Event ${event.id} exceeded retry limit (${MAX_ATTEMPTS}).`
          );
        }
      }
    }
  } catch (err) {
    console.error("❌ Outbox poller cycle failed:", err);
  } finally {
    isRunning = false;
  }
};

export const startOutboxPoller = () => {
  console.log(
    `🔄 Outbox poller started (every ${POLL_INTERVAL_MS / 1000}s)`
  );

  runOnce().catch((err) => {
    console.error("Initial outbox poll failed:", err);
  });

  pollerTimer = setInterval(() => {
    runOnce().catch((err) => {
      console.error("Outbox poll failed:", err);
    });
  }, POLL_INTERVAL_MS);
};

export const stopOutboxPoller = () => {
  if (pollerTimer) {
    clearInterval(pollerTimer);
    pollerTimer = null;
    console.log("🛑 Outbox poller stopped");
  }
};