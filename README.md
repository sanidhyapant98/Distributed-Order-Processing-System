# Distributed Order Processing System

A production-inspired, event-driven microservices project built with Node.js, TypeScript, Kafka, and Prisma.

This repository demonstrates a resilient order lifecycle using asynchronous messaging, transactional outbox publishing, and idempotent consumers.

## Overview

The system contains three services:

- **order-service**: Accepts order API requests, stores order records, writes outbox events, and updates order status based on payment results.
- **payment-service**: Consumes order creation events, simulates payment outcomes, and publishes payment result events.
- **inventory-service**: Consumes successful payment events and decrements product stock.

Kafka is used for inter-service communication. PostgreSQL is used by each service via Prisma.

## Architecture Highlights

- **Asynchronous communication** with Kafka topics.
- **Transactional Outbox Pattern** in order-service to avoid lost events between DB commit and Kafka publish.
- **Idempotent message handling** in payment-service, order-service, and inventory-service using processed-event tables.
- **Bounded retry strategy** (exponential backoff + jitter) in all services.
- **Dead Letter Queue (DLQ)** for poison messages and exhausted retries.
- **Graceful startup/shutdown flow** in all services.

## High-Level Flow

1. Client creates an order via HTTP endpoint in order-service.
2. order-service stores order and outbox row in one DB transaction.
3. outbox-poller publishes ORDER_CREATED to Kafka topic order-events.
4. payment-service consumes ORDER_CREATED and emits PAYMENT_SUCCESS or PAYMENT_FAILED to payment-events.
5. order-service consumes payment-events and updates order status to COMPLETED or FAILED.
6. inventory-service consumes PAYMENT_SUCCESS and decrements stock.

## Architecture Diagram

```mermaid
flowchart TB
    C[Client]

    subgraph O[order-service]
      API[REST API\nPOST /api/orders\nGET /api/orders/:id]
      ODB[(Order DB\nOrder + Outbox\n+ ProcessedPayment)]
      OP[Outbox Poller\n5s interval, max 5 attempts]
      OC[Payment Events Consumer\n3 retries → DLQ]
      ODLQ[DLQ: payment-events.dlq]
    end

    subgraph P[payment-service]
      PC[Order Events Consumer\nfromBeginning: true]
      PH[Payment Handler\n70% success, 500ms sim]
      PDB[(Payment DB\nProcessedOrderEvent)]
      PDLQ1[DLQ: order-events.dlq]
      PDLQ2[DLQ: payment-events.dlq]
    end

    subgraph I[inventory-service]
      IC[Payment Events Consumer\nPAYMENT_SUCCESS only]
      IH[Inventory Handler\natomic decrement]
      IDB[(Inventory DB\nProduct + ProcessedPayment)]
      IDLQ[DLQ: payment-events.dlq]
    end

    subgraph K[Kafka Topics]
      K1[(order-events)]
      K2[(payment-events)]
      K1DLQ[(order-events.dlq)]
      K2DLQ[(payment-events.dlq)]
    end

    C -->|POST /api/orders| API
    API -->|1. findUnique product| ODB
    API -->|2. $transaction Order+Outbox| ODB
    ODB -->|unpublished row| OP
    OP -->|withRetry 2 → publish| K1
    OP -->|exhaust 5 attempts| K1DLQ

    K1 -->|ORDER_CREATED| PC
    PC -->|handlePayment| PH
    PH -->|idempotency check| PDB
    PH -->|withRetry 3 → publish| K2
    PH -->|exhaust 3 retries| PDLQ2
    PH -->|unexpected error| PDLQ1

    K2 -->|payment-events| OC
    K2 -->|PAYMENT_SUCCESS| IC

    OC -->|withRetry 3 → updateOrderStatus| ODB
    OC -->|exhaust 3 retries| ODLQ
    ODLQ -.->|routes to| K2DLQ

    IC -->|withRetry 3 → handleInventory| IH
    IH -->|idempotency check + atomic| IDB
    IH -->|exhaust 3 retries| IDLQ
    IDLQ -.->|routes to| K2DLQ

    API -->|GET /api/orders/:id| ODB
```

## Event Topics

- **order-events**
  - Produced by: order-service (outbox poller)
  - Consumed by: payment-service
  - Key event: ORDER_CREATED

- **payment-events**
  - Produced by: payment-service
  - Consumed by: order-service, inventory-service
  - Key events: PAYMENT_SUCCESS, PAYMENT_FAILED

## Tech Stack

- Node.js + TypeScript
- Express (order-service API)
- Kafka (kafkajs)
- PostgreSQL
- Prisma ORM
- Docker Compose (Kafka + Zookeeper)

## Repository Structure

- `order-service/`
  - REST API, order state management, outbox writer/poller, payment-event consumer
- `payment-service/`
  - order-events consumer, payment result producer
- `inventory-service/`
  - payment-events consumer, stock updates
- `docker-compose.yml`
  - Local Kafka + Zookeeper

## Prerequisites

- Node.js 20+
- npm 10+
- Docker Desktop
- PostgreSQL instance(s)

## Environment Variables

Create a `.env` file in each service directory.

### order-service/.env

- `PORT=5000`
- `DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/order_db?schema=public`
- `KAFKA_BROKER=localhost:9092`

### payment-service/.env

- `DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/payment_db?schema=public`
- `KAFKA_BROKER=localhost:9092`

### inventory-service/.env

- `DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/inventory_db?schema=public`
- `KAFKA_BROKER=localhost:9092`

## Local Setup

### 1. Start Kafka and Zookeeper

From repository root:

```bash
docker compose up -d
```

### 2. Install dependencies

Install for each service:

```bash
cd order-service && npm install
cd ../payment-service && npm install
cd ../inventory-service && npm install
```

### 3. Run Prisma migrations

Run in each service directory:

```bash
npm run prisma:migrate
```

### 4. Start services

Open three terminals:

Terminal 1:

```bash
cd order-service
npm run dev
```

Terminal 2:

```bash
cd payment-service
npm run dev
```

Terminal 3:

```bash
cd inventory-service
npm run dev
```

## API

### Create Order

- **Method**: POST
- **URL**: `http://localhost:5000/api/orders`
- **Body**:

```json
{
  "userId": "USER_ID",
  "productId": "PRODUCT_ID"
}
```

### Get Order Status

- **Method**: GET
- **URL**: `http://localhost:5000/api/orders/:orderId`

## Example Test Calls

Create order:

```bash
curl -X POST http://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"u1","productId":"p1"}'
```

Get status:

```bash
curl http://localhost:5000/api/orders/<ORDER_ID>
```

## Data Notes

- order-service validates product existence in its own Product table before creating an order.
- inventory-service decrements stock in its own Product table after PAYMENT_SUCCESS.
- For meaningful end-to-end tests, ensure product records exist in both order and inventory databases.

## Reliability Patterns Implemented

- **Outbox Poller (order-service)**
  - Poll interval: 5 seconds
  - Max publish attempts: 5

- **Payment Publish Retry (payment-service)**
  - Max retries: 3 (bounded)
  - Backoff-based retries for transient failures
  - Exhausted retries → routes to DLQ

- **Dead Letter Queue (all services)**
  - Poison/unparseable messages → `<topic>.dlq`
  - Exhausted retries → `<topic>.dlq`
  - DLQ messages include metadata: originalTopic, failedAt, error, originalPayload

- **Idempotency**
  - payment-service dedupes ORDER_CREATED by eventId
  - order-service dedupes payment-events by eventId
  - inventory-service dedupes PAYMENT_SUCCESS by eventId

## Scripts

### order-service

- `npm run dev`
- `npm run build`
- `npm run start`
- `npm run prisma:migrate`
- `npm run prisma:generate`
- `npm run prisma:studio`

### payment-service

- `npm run dev`
- `npm run build`
- `npm run start`
- `npm run prisma:migrate`
- `npm run prisma:generate`
- `npm run prisma:studio`

### inventory-service

- `npm run dev`
- `npm run build`
- `npm run start`
- `npm run prisma:migrate`
- `npm run prisma:generate`
- `npm run prisma:studio`

## License

This project is licensed under **AGPL-3.0-only**.

## Future Improvements

- Add health check endpoints and metrics.
- Add integration tests with testcontainers.
- Add CI pipeline for lint, test, build, and migration checks.
- Add outbox DLQ consumer/replay mechanism (manual DLQ → topic replay).
