package com.minimq.common.model;

/**
 * Topic 与分区号的组合键，用于定位一条逻辑分区日志。
 */
public record TopicPartition(String topic, int partition) {
}
