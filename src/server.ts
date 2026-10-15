import http from 'node:http';
import { kafka } from './kafka-engine.js';

const port = Number(process.env.PORT) || 3000;

export function createKafkaServer(): http.Server {
  return http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', `http://${req.headers.host}`);

    const json = (data: unknown, status = 200) => {
      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data, null, 2));
    };

    // 1. Health & Cluster Status
    if (url.pathname === '/health' && req.method === 'GET') {
      const mem = process.memoryUsage();
      return json({
        status: 'healthy',
        cluster: 'KRaft Mode Active',
        brokers: [1, 2, 3],
        controller: 1,
        memoryUsageMb: Math.round(mem.heapUsed / 1024 / 1024),
        uptimeSeconds: Math.round(process.uptime())
      });
    }

    // 2. GET /api/v1/topics
    if (url.pathname === '/api/v1/topics' && req.method === 'GET') {
      return json({ success: true, topics: kafka.getAllTopics() });
    }

    // 3. POST /api/v1/produce
    if (url.pathname === '/api/v1/produce' && req.method === 'POST') {
      let body = '';
      req.on('data', (chunk) => (body += chunk));
      req.on('end', () => {
        try {
          const parsed = JSON.parse(body);
          if (!parsed.topic || parsed.value === undefined) {
            return json({ success: false, error: 'Both topic and value are required' }, 400);
          }
          const record = kafka.produce(parsed.topic, parsed.value, parsed.key || null, parsed.headers || {});
          return json({ success: true, record }, 201);
        } catch {
          return json({ success: false, error: 'Malformed JSON payload' }, 400);
        }
      });
      return;
    }

    // 4. GET /api/v1/consume
    if (url.pathname === '/api/v1/consume' && req.method === 'GET') {
      const topic = url.searchParams.get('topic');
      const partition = Number(url.searchParams.get('partition')) || 0;
      const offset = Number(url.searchParams.get('offset')) || 0;
      const limit = Number(url.searchParams.get('limit')) || 10;

      if (!topic) {
        return json({ success: false, error: 'Query parameter topic is required' }, 400);
      }

      const records = kafka.consume(topic, partition, offset, limit);
      return json({ success: true, topic, partition, records, count: records.length });
    }

    // 5. GET /api/v1/consumer-groups/:groupId
    if (url.pathname.startsWith('/api/v1/consumer-groups/') && req.method === 'GET') {
      const groupId = url.pathname.slice('/api/v1/consumer-groups/'.length);
      const groupMetadata = kafka.getConsumerGroupMetadata(groupId);
      return json({ success: true, ...groupMetadata });
    }

    // 6. POST /api/v1/consumer-groups/:groupId/commit
    if (url.pathname.startsWith('/api/v1/consumer-groups/') && url.pathname.endsWith('/commit') && req.method === 'POST') {
      const parts = url.pathname.split('/');
      const groupId = parts[parts.length - 2];

      let body = '';
      req.on('data', (chunk) => (body += chunk));
      req.on('end', () => {
        try {
          const parsed = JSON.parse(body);
          kafka.commitOffset(groupId, parsed.topic, parsed.partition, parsed.offset);
          return json({ success: true, committed: parsed });
        } catch {
          return json({ success: false, error: 'Malformed JSON payload' }, 400);
        }
      });
      return;
    }

    return json({ success: false, error: 'Route not found' }, 404);
  });
}
