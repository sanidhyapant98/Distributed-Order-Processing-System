import { prisma } from "@/prisma";
import { producer } from "./producer";

const POLL_INTERVAL_MS= 5000;
const MAX_ATTEMPTS= 5;

let pollerTimer: NodeJS.Timeout | null= null;

// Runs one cycle: find unpublished events → publish → mark done
const runOnce= async ()=>{
    // Find events that haven't been published yet and haven't exceeded max attempts
    const pendingEvents= await prisma.orderEventOutbox.findMany({
        where: {
            publishedAt: null,
            publishAttempts: { lt: MAX_ATTEMPTS },
        },
        orderBy: {
            createdAt: "asc" // oldest first
        },
        take: 10
    })

    if (pendingEvents.length=== 0){
        return;
    }

    console.log(`📬 Outbox poller: found ${pendingEvents.length} unpublished event(s)`);

    for(const event of pendingEvents){
        try{
            // Publish to kafka
            await producer.send({
                topic: "order-events",
                messages: [
                    {
                        key: event.orderId,
                        value: JSON.stringify(event.eventPayload)
                    }
                ]
            })
            // Mark as published
            await prisma.orderEventOutbox.update({
                where: {
                    id: event.id
                },
                data: {
                    publishedAt: new Date()
                }
            })

            console.log(`✅ Published outbox event for order: ${event.orderId}`);
        }catch(err){
            // Kafka failed — increment attempts and try again next cycle
            await prisma.orderEventOutbox.update({
                where: {
                    id: event.id
                },
                data: {
                    publishAttempts: {
                        increment: 1
                    }
                }
            })
            console.error(`⚠️  Failed to publish event for order ${event.orderId} (attempt ${event.publishAttempts + 1}):`, err);

            if(event.publishAttempts + 1 >= MAX_ATTEMPTS){
                console.error(`🚨 Giving up on event for order ${event.orderId} after ${MAX_ATTEMPTS} attempts. Check outbox table.`);
            }
        }
    }
}

export const startOutboxPoller= ()=>{
    console.log(`🔄 Outbox poller started (every ${POLL_INTERVAL_MS / 1000}s)`);
    // Run immediately on start, then on interval
    runOnce();
    pollerTimer= setInterval(runOnce, POLL_INTERVAL_MS);
}

export const stopOutboxPoller= ()=>{
    if(pollerTimer){
        clearInterval(pollerTimer)
        pollerTimer= null
        console.log("🛑 Outbox poller stopped");
    }
}