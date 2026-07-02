package com.minimq.client.model;

/**
 * 生产请求成功后的返回结果，记录写入 topic、partition 与 base offset。
 */
public record ProduceResult(String topic, int partitionId, long offset) {
}
