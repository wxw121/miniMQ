package com.minimq.client.model;

/**
 * 生产者待发送消息模型，partitionId 为空时由客户端按 key 选择分区。
 */
public record ProducerRecord(String topic, Integer partitionId, byte[] key, byte[] value) {
}
