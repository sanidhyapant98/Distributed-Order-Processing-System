import { Kafka } from "kafkajs";
import { prisma } from "../prisma";

const kafka = new Kafka({
  clientId: "order-service-consumer",
  brokers: (process.env.KAFKA_BROKER || "localhost:9092").split(","),
});

const consumer = kafka.consumer({ groupId: "order-service-group" });

export const startOrderConsumer = async () => {
  try {
    await consumer.connect();
    console.log("📡 Order Consumer connecting to Kafka...");

    await consumer.subscribe({
      topic: "payment-events",
      fromBeginning: false,
    });

    console.log("👂 Listening on topic: payment-events");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const data = JSON.parse(message.value!.toString());
          console.log("\n📥 Order Service received payment event:", data);

          const eventId: string | undefined= data.eventId;

          if(!eventId){
            // Defensive: if an event somehow has no eventId, we can't
            // dedupe it. Log loudly and process it anyway rather than crash.
            console.warn("⚠️  Event has no eventId — cannot deduplicate. Processing anyway.");
          }else{
            // IDEMPOTENCY CHECK: have we already processed this exact event?
            const alreadyProcessed= await prisma.processedPaymentEvent.findUnique({
              where: {
                id: eventId
              }
            })
            if(alreadyProcessed){
              console.log(`⏭️  Duplicate event detected (eventId: ${eventId}). Skipping — already processed at ${alreadyProcessed.processedAt.toISOString()}`);
              return; // ack the offset, do nothing else
            }
          }

          if (data.type === "PAYMENT_SUCCESS") {
            // Update order status to COMPLETED
            await prisma.$transaction(async(tx)=>{
              const updatedOrder= await tx.order.update({
                where: {
                  id: data.orderId
                },
                data: {
                  status: "COMPLETED"
                }
              })
              if(eventId){
                await tx.processedPaymentEvent.create({
                  data: {
                    id: eventId,
                    eventType: data.type,
                    orderId: data.orderId,
                  }
                })
              }
            console.log(`✅ Order ${data.orderId} marked as COMPLETED`);
            console.log(`   Order Details:`, updatedOrder);
            })
          } 
          else if (data.type === "PAYMENT_FAILED") {
            // Update order status to FAILED
            await prisma.$transaction(async(tx)=>{
              const updatedOrder= await tx.order.update({
                where: {
                  id: data.orderId
                },
                data: {
                  status: "FAILED"
                }
              })
              if(eventId){
                await tx.processedPaymentEvent.create({
                  data: {
                    id: eventId,
                    eventType: data.type,
                    orderId: data.orderId,
                  }
                })
              }
              console.log(`❌ Order ${data.orderId} marked as FAILED`);
              console.log(`   Reason:`, data.reason || "Payment declined");
              console.log(`   Order Details:`, updatedOrder);
            })
          }
          else{
            console.log(`⏭️  Ignored event type: ${data.type}`);
          }
        } catch (err) {
          console.error("⚠️  Error processing payment event:", err);
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