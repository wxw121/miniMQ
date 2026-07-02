package com.minimq.protocol;

/**
 * MiniMQ 二进制帧常量，定义 magic、version 和最大帧大小。
 */
public final class MqFrame {
    public static final short MAGIC = 0x4D51;
    public static final byte VERSION = 1;
    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    private MqFrame() {
    }
}
