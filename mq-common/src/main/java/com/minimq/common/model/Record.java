package com.minimq.common.model;

/**
 * 存储层消息记录，保存持久化 offset 以及可为空的 key/value 字节数组。
 */
public record Record(long offset, byte[] key, byte[] value) {
}
