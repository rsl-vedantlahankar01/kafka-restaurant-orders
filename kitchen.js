const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "restaurant-cli",
  brokers: ["localhost:9092"],
});
// Unique Consumer Group for the Kitchen Service
const consumer = kafka.consumer({ groupId: "kitchen-group" });

async function runKitchenService() {
  await consumer.connect();
  // Subscribe to the shared topic
  await consumer.subscribe({
    topic: "restaurant.orders.created",
    fromBeginning: true,
  });

  console.log("🍳 Kitchen Service is running and waiting for orders...");

  // Process the events independently
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log(
        `[Kitchen] Preparing Order ${order.order_id} for ${order.restaurant_id} (Partition: ${partition})`,
      );
    },
  });
}

runKitchenService();
