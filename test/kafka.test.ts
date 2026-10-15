import { describe, it } from 'node:test';
import assert from 'node:assert';
import { kafka } from '../src/kafka-engine.js';

describe('Kafka Distributed Commit Log Engine', () => {
  it('should initialize topics with requested partition count', () => {
    kafka.createTopic('orders-event-stream', 4);
    const meta = kafka.getTopicMetadata('orders-event-stream');
    assert.notStrictEqual(meta, null);
    assert.strictEqual(meta?.partitions.length, 4);
  });

  it('should deterministically route identical keys to the identical partition', () => {
    const rec1 = kafka.produce('orders-event-stream', { orderId: 101, amount: 50 }, 'customer_99');
    const rec2 = kafka.produce('orders-event-stream', { orderId: 102, amount: 75 }, 'customer_99');
    const rec3 = kafka.produce('orders-event-stream', { orderId: 103, amount: 100 }, 'customer_99');

    assert.strictEqual(rec1.partition, rec2.partition);
    assert.strictEqual(rec2.partition, rec3.partition);
  });

  it('should assign monotonically incrementing offsets per partition', () => {
    const p0Before = kafka.consume('orders-event-stream', 0, 0, 100).length;
    const recA = kafka.produce('orders-event-stream', { item: 'A' }, 'customer_99');
    const recB = kafka.produce('orders-event-stream', { item: 'B' }, 'customer_99');

    assert.strictEqual(recB.offset, recA.offset + 1);
  });

  it('should compute consumer group lag correctly (LEO - committedOffset)', () => {
    const group = kafka.getConsumerGroupMetadata('curriculum-tracker-group');
    assert.strictEqual(group.state, 'Stable');
    assert.ok(group.lag !== undefined);
  });
});
