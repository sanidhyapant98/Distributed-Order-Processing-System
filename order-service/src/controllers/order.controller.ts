import { Request, Response } from "express";
import { prisma } from "../prisma.js";
import { randomUUID } from "node:crypto";

export const createOrder = async (req: Request, res: Response) => {
    try {
        const { userId, productId }: { userId: string; productId: string } = req.body;
        
        // Validation
        if (!userId || !productId) {
            res.status(400).json({ 
                error: "Missing required fields",
                required: ["userId", "productId"]
            });
            return;
        }
        
        const product = await prisma.product.findUnique({
            where: {
                id: productId
            }
        })
        if(!product){
            res.status(400).json({error: "❌ Product not found"})
            return;
        }
        
        console.log(`\n📝 Creating order for userId: ${userId}, productId: ${productId}`);

        // Generate the outbox row's id up front so we can embed it as the
        // event's unique ID (eventId) inside its own payload. Downstream
        // consumers use this eventId to detect and skip duplicate deliveries
        const outboxEventId= randomUUID();

        // THE KEY PART: both writes happen together or not at all
        // If Kafka is down, no problem — the outbox poller will publish later
        const { order } = await prisma.$transaction(async (tx) => {
            // 1. Create the order
            const order= await tx.order.create({
                data: {
                    userId,
                    productId,
                    status: "PENDING",
                },
            }); 
            // 2. Save the event we want to publish(not published yet)
            await tx.orderEventOutbox.create({
                data: {
                    id: outboxEventId,
                    orderId: order.id,
                    eventType: "ORDER_CREATED",
                    eventPayload: {
                        type: "ORDER_CREATED",
                        eventId: outboxEventId,
                        orderId: order.id,
                        userId,
                        productId,
                        timeStamp: new Date().toISOString()
                    },
                },
            });
            return { order };
        });

        console.log(`✅ Order ${order.id} created. Event saved to outbox.`);

        res.status(201).json({
            success: true,
            message: "Order created successfully. Processing payment...",
            order: {
                id: order.id,
                userId: order.userId,
                productId: order.productId,
                status: order.status,
                createdAt: order.createdAt
            }
        });
    } catch (err) {
        console.error("❌ Error creating order:", err);
        res.status(500).json({ 
            error: "Failed to create order",
            details: err instanceof Error ? err.message : "Unknown error"
        });
    }
}

// Get order status
export const getOrder = async (req: Request, res: Response) => {
    try {
        const { orderId } = req.params;

        const order = await prisma.order.findUnique({
            where: { id: orderId }
        });

        if (!order) {
            res.status(404).json({ error: "Order not found" });
            return;
        }

        res.json({
            success: true,
            order: {
                id: order.id,
                userId: order.userId,
                productId: order.productId,
                status: order.status,
                createdAt: order.createdAt
            }
        });
    } catch (err) {
        console.error("❌ Error fetching order:", err);
        res.status(500).json({ error: "Failed to fetch order" });
    }
}
