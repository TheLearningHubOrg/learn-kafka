import { createKafkaServer } from './server.js';

const port = Number(process.env.PORT) || 3000;
const server = createKafkaServer();

server.listen(port, () => {
  console.log(`[learn-kafka] Distributed Event Streaming Server running on http://localhost:${port}`);
  console.log(`[learn-kafka] Topics & Partitions: http://localhost:${port}/api/v1/topics`);
  console.log(`[learn-kafka] Consumer Lag Monitor: http://localhost:${port}/api/v1/consumer-groups/curriculum-tracker-group`);
});
