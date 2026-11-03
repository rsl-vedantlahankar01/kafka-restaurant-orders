# Restaurant Franchise Order Event Processing System

## Setup Instructions
1. Start Kafka using Docker: `docker compose up -d`
2. Create the topic: `docker exec -it kafka-broker kafka-topics --create --topic restaurant.orders.created --partitions 2 --replication-factor 1 --bootstrap-server localhost:9092`

## Running the Application
1. Install dependencies: `npm install`
2. Start the consumers in three separate terminal tabs:
   - `node kitchen.js`
   - `node inventory.js`
   - `node notification.js`
3. Run the producer to create an order: 
   `node producer.js RESTAURANT-101 "Customer Name" "Items"`

## Demonstrating Kafka Concepts
- **Failure Recovery**: Stop one consumer (`Ctrl+C`), produce a new order, and restart the consumer to observe it resuming from its last committed offset.
- **Replay**: Stop a consumer, reset its offset to the beginning using the Kafka CLI (`--reset-offsets --to-earliest`), and restart it to watch it reprocess all previous events.