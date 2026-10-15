export interface KafkaRecord<T = unknown> {
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  value: T;
  timestamp: number;
  headers?: Record<string, string>;
}

export interface PartitionMetadata {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  logEndOffset: number;
  highWatermark: number;
}

export interface TopicMetadata {
  name: string;
  partitions: PartitionMetadata[];
  totalMessages: number;
}

export interface ConsumerGroupMetadata {
  groupId: string;
  state: 'Stable' | 'Rebalancing' | 'Dead';
  members: Array<{ memberId: string; clientId: string; assignedPartitions: number[] }>;
  committedOffsets: Record<string, number>; // "topic-partition" -> offset
  lag: Record<string, number>; // "topic-partition" -> lag count
}
