package com.minimq.common.model;

public record Record(long offset, byte[] key, byte[] value) {
}
