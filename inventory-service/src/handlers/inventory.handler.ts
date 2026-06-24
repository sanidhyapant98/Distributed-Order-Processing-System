import { prisma } from "../prisma";

export const handleInventory = async (event: any) => {
  const { orderId, productId, eventId } = event;

  console.log(`\n📦 Inventory processing for order: ${orderId}`);
  console.log(`   Product ID: ${productId}`);

  if(!eventId){
    console.warn("⚠️  Event has no eventId — cannot deduplicate. Processing anyway.");
  }else{
    // Idempotency Check: have we already processed this event?
    const alreadyProcessed= await prisma.processedPaymentEvent.findUnique({
      where: {
        id: eventId
      }
    })
    if(alreadyProcessed){
      console.log(`⏭️  Duplicate PAYMENT_SUCCESS event detected (eventId: ${eventId}). Skipping — already processed at ${alreadyProcessed.processedAt.toISOString()}`);
      return;
    }
  }

  try {
    const product = await prisma.product.findUnique({
      where: { id: productId },
    });

    if (!product) {
      console.log(`❌ Product not found: ${productId}`);
      return;
    }

    console.log(`📊 Current stock: ${product.stock}`);

    if (product.stock <= 0) {
      console.log(`⚠️ Out of stock for product: ${productId}`);
      return;
    }

    // Business Logic(decrement stock) + dedup record, same transaction
    const updatedProduct= await prisma.$transaction(async(tx)=>{
        const updated = await prisma.product.update({
        where: { id: productId },
        data: {
          stock: product.stock - 1,
        },
      });
      if(eventId){
        await tx.processedPaymentEvent.create({
          data: {
            id: eventId,
            eventType: "PAYMENT_SUCCESS",
            orderId,
            productId,
          }
        })
      }
      return updated;
    })
    
    console.log(`✅ Stock updated successfully`);
    console.log(`📉 New stock: ${updatedProduct.stock}\n`);
    if(eventId){
      console.log(`📌 Marked eventId ${eventId} as processed`);
    }

  } catch (err) {
    console.error("❌ Error updating inventory:", err);
  }
};