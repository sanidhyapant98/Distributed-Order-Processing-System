import { Request, Response } from "express";
import { producer } from "../kafka/producer";
import { prisma } from "../prisma";

export const createOrder = async (req: Request, res: Response) => {
    try {
        const { userId, productId } = req.body;
        
        // Validation
        if (!userId || !productId) {
            res.status(400).json({ 
                error: "Missing required fields",
                required: ["userId", "productId"]
            });
            return;
        }

        console.log(`\n📝 Creating order for userId: ${userId}, productId: ${productId}`);

        const product = await prisma.product.findUnique({
            where: {
                id: productId
            }
        })
        if(!product){
            return res.status(400).json({error: "❌ Product not found"})
        }
        // Create order in database
        const order = await prisma.order.create({
            data: {
                userId,
                productId,
                status: "PENDING",
            }
        });

        console.log(`✅ Order created with ID: ${order.id}`);

        // Publish event to Kafka
        await producer.send({
            topic: "order-events",
            messages: [{
                key: order.id,
                value: JSON.stringify({
                    type: "ORDER_CREATED",
                    orderId: order.id,
                    userId,
                    productId,
                    timestamp: new Date().toISOString(),
                })
            }]
        });

        console.log(`� Published ORDER_CREATED event to Kafka for order: ${order.id}`);
        console.log(`⏳ Waiting for payment processing...\n`);

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