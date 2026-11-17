const http = require('http');

// Total orders to generate (configurable via CLI argument, e.g.: node send-orders.js 100)
const totalOrders = parseInt(process.argv[2], 10) || 50;
const sampleItems = ['Laptop', 'Keyboard', 'Mouse', 'Monitor', 'Headphones', 'Desk Lamp', 'Webcam'];

const sendOrderRequest = (payload) => {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify(payload);

    const requestOptions = {
      hostname: 'localhost',
      port: 3000,
      path: '/orders',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
      },
    };

    const req = http.request(requestOptions, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          resolve({ status: res.statusCode, body: parsed });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });

    req.on('error', (err) => reject(err));
    req.write(postData);
    req.end();
  });
};

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const main = async () => {
  console.log(`Starting to send ${totalOrders} orders + 3 duplicate tests to http://localhost:3000/orders...\n`);

  let duplicateTargetPayload = null;

  for (let i = 1; i <= totalOrders; i++) {
    const randomItem = sampleItems[Math.floor(Math.random() * sampleItems.length)];
    const randomAmount = Math.floor(Math.random() * 500) + 20;

    const payload = {
      item: randomItem,
      amount: randomAmount,
    };

    try {
      const response = await sendOrderRequest(payload);
      console.log(`[Order ${i}/${totalOrders}] Sent: ${payload.item} ($${payload.amount}) -> OrderId: ${response.body.orderId} (Partition: ${response.body.partition}, Offset: ${response.body.offset})`);

      // Pick order #5 to use for the duplicate test
      if (i === 5) {
        duplicateTargetPayload = {
          orderId: response.body.orderId,
          item: payload.item,
          amount: payload.amount,
        };
      }
    } catch (err) {
      console.error(`[Order ${i}/${totalOrders}] Error sending order:`, err.message);
    }

    // Small delay between sends to observe throughput
    await wait(60);
  }

  // --- Duplicate Test ---
  if (duplicateTargetPayload) {
    console.log(`\n======================================================`);
    console.log(`Sending 3 intentional DUPLICATES of OrderId: ${duplicateTargetPayload.orderId}`);
    console.log(`======================================================`);

    for (let duplicateIndex = 1; duplicateIndex <= 3; duplicateIndex++) {
      try {
        const response = await sendOrderRequest(duplicateTargetPayload);
        console.log(`[Duplicate Send #${duplicateIndex}] Re-sent OrderId: ${duplicateTargetPayload.orderId} -> Producer Partition: ${response.body.partition}, Offset: ${response.body.offset}`);
      } catch (err) {
        console.error(`[Duplicate Send #${duplicateIndex}] Error:`, err.message);
      }
      await wait(100);
    }
  }

  console.log('\nAll orders sent successfully.');
};

main();

