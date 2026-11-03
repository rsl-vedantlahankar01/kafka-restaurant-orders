const { Kafka } = require("kafkajs");

// Configure Kafka connection details
const kafka = new Kafka({
  clientId: "restaurant-cli-producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function runProducer() {
  // Read command-line arguments
  // process.argv contains the command run. We slice the first two default elements off.
  const args = process.argv.slice(2);
  if (args.length < 3) {
    console.log(
      "Usage: node producer.js <restaurant_id> <customer_name> <items>",
    );
    console.log(
      "Example: node producer.js RESTAURANT-101 'Vedant' 'Burger, Fries'",
    );
    process.exit(1);
  }

  const restaurantId = args[0];
  const customerName = args[1];
  const items = args[2].split(",").map((item) => item.trim());

  // Create the restaurant order event with the 6 required fields
  const orderEvent = {
    order_id: `ORD-${Date.now()}`,
    restaurant_id: restaurantId,
    customer_name: customerName,
    items: items,
    order_time: new Date().toISOString(),
    order_status: "CREATED",
  };

  try {
    await producer.connect();

    // Send the event to the topic, using restaurant_id as the key
    const result = await producer.send({
      topic: "restaurant.orders.created",
      messages: [
        {
          key: restaurantId,
          value: JSON.stringify(orderEvent), // Convert the JS object to a string for Kafka
        },
      ],
    });

    console.log(
      `Order sent successfully! Partition assigned: ${result[0].partition}`,
    );
    console.log("Event details:", orderEvent);
  } catch (error) {
    console.error("Error producing message:", error);
  } finally {
    await producer.disconnect();
  }
}

runProducer();
