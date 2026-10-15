import crypto from 'node:crypto';
import type { KafkaRecord, TopicMetadata, ConsumerGroupMetadata } from './types.js';

export class KafkaBrokerEngine {
  // Topic -> Partition -> Array of Records (Append-Only Commit Log)
  private logs = new Map<string, Map<number, KafkaRecord[]>>();
  private partitionCounts = new Map<string, number>();

  // Consumer Group -> Topic-Partition -> Committed Offset
  private committedOffsets = new Map<string, Map<string, number>>();

  constructor() {
    this.createTopic('learning-roadmap-events', 3);
    this.createTopic('telemetry-metrics', 2);
    this.seed();
  }

  createTopic(name: string, partitions = 3) {
    if (this.logs.has(name)) return;
    const partitionMap = new Map<number, KafkaRecord[]>();
    for (let i = 0; i < partitions; i++) {
      partitionMap.set(i, []);
    }
    this.logs.set(name, partitionMap);
    this.partitionCounts.set(name, partitions);
  }

  // Murmur2 partition hashing simulation
  private getPartitionForKey(topic: string, key: string | null): number {
    const numPartitions = this.partitionCounts.get(topic) || 1;
    if (!key) {
      // Round-robin or random
      return Math.floor(Math.random() * numPartitions);
    }
    const hash = crypto.createHash('sha256').update(key).digest().readUInt32BE(0);
    return hash % numPartitions;
  }

  // Producer API
  produce(topic: string, value: unknown, key: string | null = null, headers: Record<string, string> = {}): KafkaRecord {
    if (!this.logs.has(topic)) {
      this.createTopic(topic, 3);
    }

    const partition = this.getPartitionForKey(topic, key);
    const partitionLog = this.logs.get(topic)!.get(partition)!;

    const offset = partitionLog.length;
    const record: KafkaRecord = {
      topic,
      partition,
      offset,
      key,
      value,
      timestamp: Date.now(),
      headers
    };

    partitionLog.push(record);
    return record;
  }

  // Consumer API
  consume(topic: string, partition: number, startOffset = 0, maxRecords = 10): KafkaRecord[] {
    const partitionLog = this.logs.get(topic)?.get(partition);
    if (!partitionLog) return [];
    return partitionLog.slice(startOffset, startOffset + maxRecords);
  }

  commitOffset(groupId: string, topic: string, partition: number, offset: number) {
    if (!this.committedOffsets.has(groupId)) {
      this.committedOffsets.set(groupId, new Map());
    }
    this.committedOffsets.get(groupId)!.set(`${topic}-${partition}`, offset);
  }

  getTopicMetadata(topicName: string): TopicMetadata | null {
    const topic = this.logs.get(topicName);
    if (!topic) return null;

    let total = 0;
    const partitions = Array.from(topic.entries()).map(([id, records]) => {
      total += records.length;
      return {
        id,
        leader: 1,
        replicas: [1, 2, 3],
        isr: [1, 2, 3],
        logEndOffset: records.length,
        highWatermark: records.length
      };
    });

    return {
      name: topicName,
      partitions,
      totalMessages: total
    };
  }

  getAllTopics(): TopicMetadata[] {
    return Array.from(this.logs.keys()).map((name) => this.getTopicMetadata(name)!);
  }

  getConsumerGroupMetadata(groupId: string): ConsumerGroupMetadata {
    const groupOffsets = this.committedOffsets.get(groupId) || new Map();
    const committed: Record<string, number> = {};
    const lag: Record<string, number> = {};

    this.logs.forEach((partitionMap, topic) => {
      partitionMap.forEach((records, partition) => {
        const key = `${topic}-${partition}`;
        const currentOffset = groupOffsets.get(key) || 0;
        committed[key] = currentOffset;
        lag[key] = Math.max(0, records.length - currentOffset);
      });
    });

    return {
      groupId,
      state: 'Stable',
      members: [
        { memberId: 'worker-node-1', clientId: 'consumer-svc', assignedPartitions: [0, 1] },
        { memberId: 'worker-node-2', clientId: 'consumer-svc', assignedPartitions: [2] }
      ],
      committedOffsets: committed,
      lag
    };
  }

  private seed() {
    this.produce('learning-roadmap-events', { stage: 1, event: 'Stage1_Mastered', title: 'Commit Log Foundations' }, 'stage_1');
    this.produce('learning-roadmap-events', { stage: 2, event: 'Stage2_Mastered', title: 'Producer Delivery Guarantees' }, 'stage_2');
    this.produce('learning-roadmap-events', { stage: 3, event: 'Stage3_Mastered', title: 'Consumer Rebalancing' }, 'stage_3');
    this.commitOffset('curriculum-tracker-group', 'learning-roadmap-events', 0, 1);
  }
}

export const kafka = new KafkaBrokerEngine();
