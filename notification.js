const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "restaurant-cli",
  brokers: ["localhost:9092"],
});
// Unique Consumer Group for the Notification Service
const consumer = kafka.consumer({ groupId: "notification-group" });

async function runNotificationService() {
  await consumer.connect();
  await consumer.subscribe({
    topic: "restaurant.orders.created",
    fromBeginning: true,
  });

  console.log("📱 Notification Service is running and waiting for orders...");

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log(
        `[Notification] Texting ${order.customer_name}: Your order is confirmed! (Partition: ${partition})`,
      );
    },
  });
}

runNotificationService();
