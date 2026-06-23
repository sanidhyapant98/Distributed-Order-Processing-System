-- CreateTable
CREATE TABLE "processed_order_event" (
    "id" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_order_event_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "processed_order_event_orderId_idx" ON "processed_order_event"("orderId");
