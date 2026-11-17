const { Kafka } = require('kafkajs');

// Kafka Client configuration
const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: ['localhost:9092'],
});

// Consumer Group: "notification-group"
// In Kafka, consumer groups enable publish-subscribe (fan-out) patterns.
// Because each consumer group maintains its own set of committed offsets per partition,
// creating a separate consumer group ensures this service receives a complete, independent
// copy of every message published to the topic, completely decoupled from any other service.
const consumer = kafka.consumer({
  groupId: 'notification-group',
});

const run = async () => {
  try {
    await consumer.connect();

    // Subscribe to "payments" topic
    await consumer.subscribe({
      topic: 'payments',
      fromBeginning: true,
    });

    console.log('Notification service running, waiting for payment events...');

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const paymentEvent = JSON.parse(message.value.toString());
        const { orderId, status } = paymentEvent;

        if (status === 'success') {
          console.log(`[Notification] Order ${orderId} confirmed (Partition: ${partition}, Offset: ${message.offset})`);
        } else {
          console.log(`[Notification] Order ${orderId} payment failed (Partition: ${partition}, Offset: ${message.offset})`);
        }
      },
    });
  } catch (error) {
    console.error('Error running Notification service:', error);
    process.exit(1);
  }
};

// Graceful shutdown on SIGINT
const shutdown = async () => {
  console.log('\nShutting down Notification service gracefully...');
  try {
    await consumer.disconnect();
    console.log('Consumer disconnected successfully.');
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

run();

