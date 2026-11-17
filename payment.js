const { Kafka } = require('kafkajs');

// Kafka Client configuration
const kafka = new Kafka({
  clientId: `payment-service-${process.pid}`,
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'payment-group',
});

// Idempotent Producer:
// Guarantees exactly-once write semantics per partition by assigning a producer ID
// and sequence numbers, preventing duplicate messages on network retries.
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 1,
});

// Deduplication Store (In-Memory Set):
// Kafka provides "at-least-once" delivery semantics; network glitches or consumer crashes
// before an offset commit can cause identical messages to be re-delivered.
// Tracking processed identifiers guarantees idempotency at the consumer application level.
const processedOrderIds = new Set();

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
      topic: 'orders',
      fromBeginning: true,
    });

    console.log(`Payment service [PID: ${process.pid}] running with Retry, DLQ, and Deduplication...`);

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const orderData = JSON.parse(message.value.toString());
        const { orderId, amount } = orderData;

        console.log(`\n[Order Received] Partition: ${partition} | Offset: ${message.offset} | OrderId: ${orderId}`);

        // --- 1. Deduplication Check ---
        if (processedOrderIds.has(orderId)) {
          console.warn(`[Duplicate Skipped] Order ${orderId} has already been processed.`);
          // Acknowledge offset immediately so we advance past the duplicate message
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);
          return;
        }

        // --- 2. Retry Logic with Exponential Backoff ---
        // Retry: Handles transient failures (e.g. temporary downstream network/DB dropouts)
        // by attempting execution multiple times before giving up.
        const maxRetries = 3;
        let attempt = 0;
        let processSuccessful = false;
        let lastError = null;

        while (attempt <= maxRetries && !processSuccessful) {
          try {
            // Simulate random 10% transient processing crash/exception
            if (Math.random() < 0.10) {
              throw new Error('Simulated random processing failure (transient crash)');
            }

            // Normal business logic: simulate payment gateway (80% success, 20% decline)
            const isSuccess = Math.random() < 0.8;
            const paymentEvent = {
              orderId,
              status: isSuccess ? 'success' : 'failed',
              amount,
              processedAt: new Date().toISOString(),
            };

            await producer.send({
              topic: 'payments',
              messages: [
                {
                  key: orderId,
                  value: JSON.stringify(paymentEvent),
                },
              ],
            });

            console.log(`[Payment Processed] OrderId: ${orderId} -> Status: ${paymentEvent.status}`);
            processSuccessful = true;
          } catch (error) {
            attempt++;
            lastError = error;
            console.error(`[Retry Attempt ${attempt}/${maxRetries}] Failed for Order ${orderId}: ${error.message}`);

            if (attempt <= maxRetries) {
              // Exponential backoff: 1s, 2s, 4s
              const backoffDelayMs = 1000 * Math.pow(2, attempt - 1);
              console.log(`[Backoff] Waiting ${backoffDelayMs}ms before retrying Order ${orderId}...`);
              await wait(backoffDelayMs);
            }
          }
        }

        // --- 3. Dead Letter Queue (DLQ) & Offset Commit ---
        // DLQ: Poison pill messages or persistent errors are routed to a separate quarantine topic.
        // This isolates bad messages for auditing/replay and allows consumer offset progression
        // without blocking the entire partition.
        if (!processSuccessful) {
          console.error(`[DLQ Route] Order ${orderId} exceeded max retries. Routing to 'orders.dlq'...`);
          await producer.send({
            topic: 'orders.dlq',
            messages: [
              {
                key: orderId,
                value: message.value, // forward original message payload
                headers: {
                  error: String(lastError ? lastError.message : 'Unknown error'),
                  retryCount: String(maxRetries),
                },
              },
            ],
          });
          console.log(`[DLQ Sent] Order ${orderId} successfully forwarded to 'orders.dlq'.`);
        } else {
          // Record successful processing for deduplication
          processedOrderIds.add(orderId);
        }

        // Commit offset to Kafka to prevent poison pills from blocking partition consumption
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);

        console.log(`[Offset Committed] Topic: ${topic} | Partition: ${partition} | Next Offset: ${(BigInt(message.offset) + 1n).toString()}`);
      },
    });
  } catch (error) {
    console.error('Error running Payment service:', error);
    process.exit(1);
  }
};

// Graceful shutdown on SIGINT
const shutdown = async () => {
  console.log(`\nShutting down Payment service [PID: ${process.pid}]...`);
  try {
    await consumer.disconnect();
    await producer.disconnect();
    console.log('Consumer and producer disconnected successfully.');
    process.exit(0);
  } catch (err) {
    console.error('Error during shutdown:', err);
    process.exit(1);
  }
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

run();
