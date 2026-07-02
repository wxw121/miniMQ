package com.minimq.client.model;

public record ConsumerRecord(String topic, int partition, long offset, byte[] key, byte[] value) {
}
