import { prisma } from "../prisma";
import { withRetry } from "@/utils/retry";
import { sendToDlq } from "../kafka/dlq";

const MAX_UPDATE_RETRIES = 3;
const PAYMENT_EVENTS_TOPIC = "payment-events";

export const handleInventory = async (event: any) => {
  const { orderId, productId, eventId } = event;

  console.log(`\n📦 Inventory processing for order: ${orderId}`);
  console.log(`   Product ID: ${productId}`);

  try{
    if(eventId){
      // Idempotency Check: have we already processed this event?
      const alreadyProcessed= await prisma.processedPaymentEvent.findUnique({
        where: {
          id: eventId,
        }
      })
      if(alreadyProcessed){
        console.log(
          `⏭️  Duplicate PAYMENT_SUCCESS event detected (eventId: ${eventId}). Skipping — already processed at ${alreadyProcessed.processedAt.toISOString()}`
        );
        return;
      }
    }else{
      console.warn("⚠️  Event has no eventId — cannot deduplicate. Processing anyway.");
    }
    try{
      await withRetry(()=> decrementStockAndMarkProcessed(orderId, productId, eventId), {
        retries: MAX_UPDATE_RETRIES,
        baseDelayMs: 300,
        onRetry: (err, attempt, delayMs)=>{
          console.warn(
            `🔁 [order ${orderId}] retrying inventory update (attempt ${attempt}/${MAX_UPDATE_RETRIES}) in ${delayMs}ms — ${
              err instanceof Error ? err.message : err
            }`
          )
        }
      })
    }catch(err){
      // Bounded: stop here rather than retrying forever. Instead of just
      // logging and losing this stock update (the decrement would never
      // happen), send it to the DLQ so it can be manually reprocessed.
      console.error(
        `🚨 [order ${orderId}] Giving up on inventory update after ${MAX_UPDATE_RETRIES} retries:`,
        err
      );
      await sendToDlq({
        originalTopic: PAYMENT_EVENTS_TOPIC,
        failedAt: new Date().toISOString(),
        error: err instanceof Error ? err.message : String(err),
        originalPayload: event,
        eventId,
      });
    }
  }catch(err){
    console.error("❌ Unexpected error updating inventory:", err);
    await sendToDlq({
      originalTopic: PAYMENT_EVENTS_TOPIC,
      failedAt: new Date().toISOString(),
      error: err instanceof Error ? err.message : String(err),
      originalPayload: event,
      eventId,
    });
  }
};

async function decrementStockAndMarkProcessed(
  orderId: string,
  productId: string,
  eventId: string | undefined
){
  await prisma.$transaction(async (tx)=>{
    const product= await tx.product.findUnique({
      where: {
        id: productId
      }
    })
    if(!product){
      console.log(`❌ Product not found: ${productId}`);
      return; // Not a transient failure — retrying won't help. Don't throw.    
    }
    console.log(`📊 Current stock: ${product.stock}`);
    if(product.stock<= 0){
      console.log(`⚠️ Out of stock for product: ${productId}`);
      return; // A real business outcome, not an error — don't retry this
    }
    // Atomic decrement instead of `stock: product.stock - 1` — avoids a
    // lost update if something else touches this row between the read
    // above and this write.
    const updated= await tx.product.update({
      where: {
        id: productId
      },
      data: {
        stock: {
          decrement: 1
        }
      }
    })
    if(eventId){
      // upsert (not create) — a retry may re-enter this function after a
      // previous attempt's write committed but the attempt failed
      // afterwards (e.g. the transaction's commit acknowledgement was lost)
      await tx.processedPaymentEvent.upsert({
        where: {
          id: eventId
        },
        create: {
          id: eventId,
          eventType: "PAYMENT_SUCCESS",
          orderId,
          productId,
        },
        update: {}
      })
      console.log(`📌 Marked eventId ${eventId} as processed`);
    }
    console.log(`✅ Stock updated successfully`);
    console.log(`📉 New stock: ${updated.stock}\n`);
  })
}