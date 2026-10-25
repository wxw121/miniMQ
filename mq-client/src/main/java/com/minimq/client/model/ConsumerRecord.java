package com.minimq.client.model;

/**
 * 消费者拉取到的单条消息视图，包含 topic、partition、offset、key 和 value。
 */
public record ConsumerRecord(String topic, int partition, long offset, byte[] key, byte[] value) {
}
