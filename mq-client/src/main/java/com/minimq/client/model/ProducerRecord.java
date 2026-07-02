package com.minimq.client.model;

public record ProducerRecord(String topic, Integer partitionId, byte[] key, byte[] value) {
}
