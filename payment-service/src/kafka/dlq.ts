import { producer } from "./producer";

/**
 * Dead Letter Queue helper.
 *
 * When a message has exhausted its retries (or can't even be parsed),
 * we don't want to silently drop it and we don't want to block the
 * consumer retrying it forever. Instead we publish it — along with why
 * it failed — to a dedicated "<topic>.dlq" topic, then let the original
 * consumer move on. Nothing is lost, and the partition stays healthy.
 */

export interface DlqMessage {
  /** The topic the original message came from (or was destined for). */
  originalTopic: string;
  /** ISO timestamp of when we gave up. */
  failedAt: string;
  /** Human-readable reason we couldn't process it. */
  error: string;
  /** The message itself — parsed if we got that far, raw string otherwise. */
  originalPayload: unknown;
  /** The event's own id, if it had one, for easier searching in the DLQ. */
  eventId?: string;
}

export const sendToDlq = async (message: DlqMessage) => {
  const dlqTopic = `${message.originalTopic}.dlq`;

  try {
    await producer.send({
      topic: dlqTopic,
      messages: [
        {
          key: message.eventId,
          value: JSON.stringify(message),
        },
      ],
    });
    console.error(
      `☠️  Sent message to DLQ topic "${dlqTopic}" (eventId: ${message.eventId ?? "n/a"}): ${message.error}`
    );
  } catch (dlqErr) {
    // If we can't even reach the DLQ, we must not let the message vanish
    // without a trace. Log everything we have so it can be recovered
    // manually from the logs.
    console.error(
      `🚨 CRITICAL: failed to send message to DLQ topic "${dlqTopic}". Manual recovery required.`,
      { originalMessage: message, dlqError: dlqErr }
    );
  }
};