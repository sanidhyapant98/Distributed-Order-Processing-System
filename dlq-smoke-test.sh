#!/usr/bin/env bash
set -euo pipefail

BROKER="localhost:9092"
KAFKA_CONTAINER="kafka"

echo "== DLQ smoke test =="

# 1) Ensure DLQ topics exist (safe if they already exist)
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists --topic order-events.dlq >/dev/null 2>&1 || true
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists --topic payment-events.dlq >/dev/null 2>&1 || true

# 2) Inject poison JSON to order-events (should route to order-events.dlq by payment-service)
echo "{bad-json" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --bootstrap-server "$BROKER" --topic order-events >/dev/null

# 3) Inject poison JSON to payment-events (should route to payment-events.dlq by order/inventory consumers)
echo "{bad-json" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --bootstrap-server "$BROKER" --topic payment-events >/dev/null

# 4) Inject business-failure event for order-service retry-exhaustion path
TEST_EVENT_ID="dlq-test-$(date +%s)"
echo "{\"type\":\"PAYMENT_SUCCESS\",\"orderId\":\"non-existent-order\",\"eventId\":\"$TEST_EVENT_ID\"}" \
  | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --bootstrap-server "$BROKER" --topic payment-events >/dev/null

echo
echo "Sent test messages. Waiting 5s for consumers..."
sleep 5

echo
echo "== Read last 5 messages from order-events.dlq =="
docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
  --bootstrap-server "$BROKER" \
  --topic order-events.dlq \
  --from-beginning \
  --max-messages 5 || true

echo
echo "== Read last 10 messages from payment-events.dlq =="
docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
  --bootstrap-server "$BROKER" \
  --topic payment-events.dlq \
  --from-beginning \
  --max-messages 10 || true

echo
echo "Check service logs for:"
echo "- 'Sent message to DLQ topic'"
echo "- order-service retry logs then give-up for eventId: $TEST_EVENT_ID"
echo
echo "DONE"