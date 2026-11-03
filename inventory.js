const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "restaurant-cli",
  brokers: ["localhost:9092"],
});
// Unique Consumer Group for the Inventory Service
const consumer = kafka.consumer({ groupId: "inventory-group" });

async function runInventoryService() {
  await consumer.connect();
  await consumer.subscribe({
    topic: "restaurant.orders.created",
    fromBeginning: true,
  });

  console.log("📦 Inventory Service is running and waiting for orders...");

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log(
        `[Inventory] Deducting stock for Order ${order.order_id} (Partition: ${partition})`,
      );
    },
  });
}

runInventoryService();
