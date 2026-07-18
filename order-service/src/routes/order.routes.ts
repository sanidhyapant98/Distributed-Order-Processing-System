import { Router, RequestHandler } from "express";
import { createOrder, getOrder } from "../controllers/order.controller.js";

const router = Router();

// Create a new order
// ensure controller matches Express handler type
// The double cast (unknown → RequestHandler) is usually used to bypass a type mismatch between the actual function signature and what Express expects.
router.post("/orders", createOrder as unknown as RequestHandler);

// Get order status
router.get("/orders/:orderId", getOrder);

export default router;