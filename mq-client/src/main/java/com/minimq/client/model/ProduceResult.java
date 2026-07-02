package com.minimq.client.model;

public record ProduceResult(String topic, int partitionId, long offset) {
}
