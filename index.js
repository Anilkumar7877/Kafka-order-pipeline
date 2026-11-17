const express = require('express');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

// Kafka Client: Coordinates broker connections and cluster metadata
const kafka = new Kafka({
  clientId: 'order-service',
  brokers: ['localhost:9092'],
});

// Producer: Sends/publishes event messages to Kafka topics.
// Idempotence: Ensures exactly-once delivery per partition (no duplicate writes on retries).
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 1,
});

app.post('/orders', async (req, res) => {
  const { item, amount, orderId: customOrderId } = req.body;

  if (!item || amount === undefined) {
    return res.status(400).json({ error: 'item and amount are required' });
  }

  const orderId = customOrderId || uuidv4();
  const orderEvent = {
    orderId,
    item,
    amount,
    createdAt: new Date().toISOString(),
  };

  try {
    // Key: Used by Kafka's partitioner (hash of key) to route messages with the same key
    // to the same partition, guaranteeing strict ordering for that entity.
    const recordMetadata = await producer.send({
      topic: 'orders',
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderEvent),
        },
      ],
    });

    // Partition: An ordered, immutable commit log shard within a topic.
    // Offset: A unique, sequential integer ID assigned to each message within a partition.
    const { partition, baseOffset } = recordMetadata[0];

    return res.status(201).json({
      orderId,
      partition,
      offset: baseOffset,
    });
  } catch (error) {
    console.error('Failed to publish order event:', error);
    return res.status(500).json({ error: 'Failed to process order' });
  }
});

const PORT = 3000;

// Connect producer once at startup before accepting traffic
const startServer = async () => {
  try {
    await producer.connect();
    console.log('Connected to Kafka producer successfully');

    app.listen(PORT, () => {
      console.log(`Order service listening on port ${PORT}`);
    });
  } catch (error) {
    console.error('Error connecting to Kafka:', error);
    process.exit(1);
  }
};

// Graceful shutdown: Disconnect producer to flush buffered messages
process.on('SIGINT', async () => {
  console.log('\nGracefully disconnecting Kafka producer...');
  await producer.disconnect();
  process.exit(0);
});

startServer();

