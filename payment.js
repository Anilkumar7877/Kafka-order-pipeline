const { Kafka } = require('kafkajs');

// Kafka Client configuration
const kafka = new Kafka({
  clientId: `payment-service-${process.pid}`,
  brokers: ['localhost:9092'],
});

// Consumer Group: A coordinated group of consumers that share the work of consuming topics.
// Kafka assigns each topic partition to exactly ONE consumer instance within this group.
const consumer = kafka.consumer({
  groupId: 'payment-group',
});

// Producer: Used to publish payment outcome events to downstream topics ("payments").
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 1,
});

// Rebalancing: Triggered when consumer instances join, leave, or partitions change.
// Kafka pauses consumption and reallocates partitions among the active consumers in the group.
consumer.on(consumer.events.GROUP_JOIN, (e) => {
  console.log(`[Rebalance Event] Joined consumer group. Member ID: ${e.payload.memberId}`);
  console.log(`[Rebalance Event] Assigned partitions:`, JSON.stringify(e.payload.memberAssignment));
});

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();

    // Subscribe to "orders". fromBeginning: true reads earliest uncommitted messages on first startup
    await consumer.subscribe({
      topic: 'orders',
      fromBeginning: true,
    });

    console.log(`Payment service [PID: ${process.pid}] running, waiting for orders...`);

    // autoCommit: false allows Manual Offset Commits for at-least-once processing semantics
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const orderData = JSON.parse(message.value.toString());
        const { orderId, amount } = orderData;

        console.log(`\n[Order Received] Partition: ${partition} | Offset: ${message.offset} | OrderId: ${orderId}`);

        // Simulate payment gateway processing: 80% success, 20% failure
        const isSuccess = Math.random() < 0.8;
        const paymentEvent = {
          orderId,
          status: isSuccess ? 'success' : 'failed',
          amount,
          processedAt: new Date().toISOString(),
        };

        // Produce payment result to "payments" topic using orderId as key to maintain partition affinity
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

        // Offset Commit: Acknowledges to Kafka that this message was successfully processed.
        // Kafka tracks committed offsets per consumer group. We commit (offset + 1) to mark
        // the next message to be fetched.
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

// Graceful shutdown: Cleanly disconnect both consumer and producer on SIGINT
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

