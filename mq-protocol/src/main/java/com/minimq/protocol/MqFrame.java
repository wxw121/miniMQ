package com.minimq.protocol;

public final class MqFrame {
    public static final short MAGIC = 0x4D51;
    public static final byte VERSION = 1;
    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    private MqFrame() {
    }
}
