#!/usr/bin/env node
/**
 * RMS Reliability Test
 * ---------------------
 * Fires N concurrent order-creation requests against order-service, optionally
 * pauses the Kafka broker mid-burst to simulate a transient outage, then polls
 * every order until it reaches a terminal status (COMPLETED / FAILED) or times
 * out. Orders that time out are cross-checked against the DLQ topics so that
 * "safely captured for manual recovery" is NOT counted as "lost".
 *
 * Reliability = (COMPLETED + FAILED + capturedInDlq) / totalSubmitted
 *
 * Usage:
 *   npm install
 *   PRODUCT_ID=<id-that-exists-in-order-db-AND-inventory-db> node run-test.js --concurrency=150 --chaos
 *
 * Flags:
 *   --concurrency=N   number of orders to fire concurrently (default 100)
 *   --batch=N
 *   --batchDelay=MS
 *   --timeout=MS      how long to wait per order before declaring TIMEOUT (default 60000)
 *   --chaos           if set, pauses the "kafka" docker container for ~5s mid-burst
 *
 * Requires:
 *   - order-service, payment-service, inventory-service running (npm run dev in each)
 *   - Kafka + Zookeeper running (docker compose up -d from repo root)
 *   - Docker CLI on PATH if using --chaos
 */

const { Kafka } = require("kafkajs");
const { exec } = require("child_process");
const { setTimeout: sleep } = require("timers/promises");

const ORDER_API = process.env.ORDER_API || "http://localhost:5000";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const PRODUCT_ID = process.env.PRODUCT_ID;
const USER_ID = process.env.USER_ID || "load-test-user";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const CONCURRENCY = Number(args.concurrency || 100);
const POLL_TIMEOUT_MS = Number(args.timeout || 120000);
const POLL_INTERVAL_MS = 1000;
const INJECT_CHAOS = !!args.chaos;
const BATCH_SIZE = Number(args.batch || 20);
const BATCH_DELAY_MS = Number(args.batchDelay || 500);

if (!PRODUCT_ID) {
  console.error(
    "❌ Set PRODUCT_ID env var to a product id that exists in BOTH order-service and inventory-service DBs.\n" +
      "   Example: PRODUCT_ID=abc-123 node run-test.js --concurrency=150 --chaos"
  );
  process.exit(1);
}

// ---------- DLQ listener ----------
// Collects orderIds seen in either DLQ topic during the test window, so a
// message that hit its retry bound but was safely dead-lettered is counted
// as "captured", not "lost".
async function startDlqListener() {
  const kafka = new Kafka({ clientId: "reliability-test", brokers: [KAFKA_BROKER] });
  const consumer = kafka.consumer({ groupId: `reliability-test-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: "order-events.dlq", fromBeginning: false });
  await consumer.subscribe({ topic: "payment-events.dlq", fromBeginning: false });

  const dlqOrderIds = new Set();

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const parsed = JSON.parse(message.value?.toString() ?? "{}");
        const payload = parsed.originalPayload;
        const orderId = (payload && typeof payload === "object" && payload.orderId) || undefined;
        if (orderId) {
          dlqOrderIds.add(orderId);
          console.log(`  [DLQ] ${topic} captured order ${orderId} (${parsed.error})`);
        } else {
          console.log(`  [DLQ] ${topic} message with no resolvable orderId (poison message)`);
        }
      } catch {
        // ignore malformed DLQ entries for this test's own bookkeeping
      }
    },
  });

  return { consumer, dlqOrderIds };
}

// ---------- chaos injection ----------
function runShell(cmd) {
  return new Promise((resolve) => {
    exec(cmd, (err, stdout, stderr) => {
      if (err) console.warn(`  [chaos] "${cmd}" failed: ${stderr || err.message}`);
      resolve();
    });
  });
}

async function injectChaos() {
  console.log("💥 Injecting chaos: pausing Kafka broker for 5s...");
  await runShell("docker pause kafka");
  await sleep(5000);
  await runShell("docker unpause kafka");
  console.log("✅ Kafka broker resumed.");
}

// ---------- order creation ----------
async function createOrder(i) {
  try {
    const res = await fetch(`${ORDER_API}/api/orders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ userId: `${USER_ID}-${i}`, productId: PRODUCT_ID }),
    });
    const body = await res.json();
    if (!res.ok || !body.success) {
      return { i, orderId: null, submitError: body.error || `HTTP ${res.status}` };
    }
    return { i, orderId: body.order.id, submitError: null };
  } catch (err) {
    return { i, orderId: null, submitError: err.message };
  }
}

// ---------- polling ----------
async function pollUntilTerminal(orderId) {
  const deadline = Date.now() + POLL_TIMEOUT_MS;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${ORDER_API}/api/orders/${orderId}`);
      if (res.ok) {
        const body = await res.json();
        const status = body.order?.status;
        if (status === "COMPLETED" || status === "FAILED") {
          return status;
        }
      }
    } catch {
      // transient fetch error while polling; keep trying until deadline
    }
    await sleep(POLL_INTERVAL_MS);
  }
  return "TIMEOUT";
}

// ---------- main ----------
async function main() {
  console.log(`\n🚀 Reliability test: ${CONCURRENCY} concurrent orders, chaos=${INJECT_CHAOS}\n`);

  const { consumer, dlqOrderIds } = await startDlqListener();

  const submitResults = [];

if (INJECT_CHAOS) {
  sleep(300).then(injectChaos);
}

console.log(
  `\n📦 Submitting ${CONCURRENCY} orders in batches of ${BATCH_SIZE}\n`
);

for (let start = 0; start < CONCURRENCY; start += BATCH_SIZE) {
  const end = Math.min(start + BATCH_SIZE, CONCURRENCY);

  console.log(
    `🚀 Batch ${Math.floor(start / BATCH_SIZE) + 1}: orders ${start + 1}-${end}`
  );

  const batchPromises = [];

  for (let i = start; i < end; i++) {
    batchPromises.push(createOrder(i));
  }

  const batchResults = await Promise.all(batchPromises);

  submitResults.push(...batchResults);

  const submitted = batchResults.filter(r => r.orderId).length;
  const rejected = batchResults.length - submitted;

  console.log(
    `   ✅ Submitted: ${submitted}, Rejected: ${rejected}`
  );

  if (end < CONCURRENCY) {
    await sleep(BATCH_DELAY_MS);
  }
}

  const submitted = submitResults.filter((r) => r.orderId);
  const submitFailures = submitResults.filter((r) => !r.orderId);

  console.log(
    `📨 Submitted: ${submitted.length}/${CONCURRENCY} (rejected at submit time: ${submitFailures.length})`
  );
  if (submitFailures.length) {
    submitFailures.slice(0, 5).forEach((f) => console.log(`   - submit error: ${f.submitError}`));
  }

  const pollResults = await Promise.all(
    submitted.map(async ({ orderId }) => ({ orderId, status: await pollUntilTerminal(orderId) }))
  );

  // let the DLQ listener catch up on any last messages before we stop it
  await sleep(3000);
  await consumer.disconnect();

  const completed = pollResults.filter((r) => r.status === "COMPLETED").length;
  const failed = pollResults.filter((r) => r.status === "FAILED").length;
  const timedOut = pollResults.filter((r) => r.status === "TIMEOUT");

  const capturedInDlq = timedOut.filter((r) => dlqOrderIds.has(r.orderId));
  const trulyLost = timedOut.filter((r) => !dlqOrderIds.has(r.orderId));

  const total = submitted.length;
  const accountedFor = completed + failed + capturedInDlq.length;
  const reliability = total === 0 ? 0 : (accountedFor / total) * 100;

  console.log("\n📊 RESULTS");
  console.log("──────────────────────────────");
  console.log(`Total orders submitted:      ${total}`);
  console.log(`Resolved COMPLETED:          ${completed}`);
  console.log(`Resolved FAILED:             ${failed}`);
  console.log(`Timed out, but in DLQ:       ${capturedInDlq.length}`);
  console.log(`Timed out, TRULY LOST:       ${trulyLost.length}`);
  console.log("──────────────────────────────");
  console.log(
    `Reliability: ${reliability.toFixed(3)}%  ( (completed + failed + dlq) / submitted )`
  );

  if (trulyLost.length > 0) {
    console.log("\n⚠️  Orders with no resolution and no DLQ trace:");
    trulyLost.forEach((r) => console.log(`   - ${r.orderId}`));
  }

  console.log(
    `\nNote: with ${total} samples, the smallest measurable gap is ${(100 / total).toFixed(
      3
    )} percentage points. For a credible "99.9%" claim, run with --concurrency=1000+ (a few batches is fine).`
  );
}

main().catch((err) => {
  console.error("Test run failed:", err);
  process.exit(1);
});