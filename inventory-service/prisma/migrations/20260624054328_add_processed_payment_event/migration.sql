-- CreateTable
CREATE TABLE "processed_payment_event" (
    "id" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "productId" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_payment_event_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "processed_payment_event_orderId_idx" ON "processed_payment_event"("orderId");
