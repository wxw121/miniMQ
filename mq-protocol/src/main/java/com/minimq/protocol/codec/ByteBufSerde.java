package com.minimq.protocol.codec;

import com.minimq.protocol.MqFrame;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * 基于 Netty ByteBuf 的基础字段编解码工具，处理字符串和字节数组的长度前缀格式。
 */
public final class ByteBufSerde {
    private static final int MAX_STRING_BYTES = 0xFFFF;
    private static final int MAX_BYTE_ARRAY_BYTES = MqFrame.MAX_FRAME_BYTES;

    private ByteBufSerde() {
    }

    // 字符串统一使用 int32 长度前缀，-1 表示 null，0 表示空字符串。
    public static void writeString(ByteBuf buf, String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("String is too long: " + bytes.length);
        }
        buf.writeShort(bytes.length);
        buf.writeBytes(bytes);
    }

    public static String readString(ByteBuf buf) {
        int len = buf.readUnsignedShort();
        requireReadable(buf, len, "string");
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeBytes(ByteBuf buf, byte[] value) {
        if (value == null) {
            buf.writeInt(-1);
            return;
        }
        if (value.length > MAX_BYTE_ARRAY_BYTES) {
            throw new IllegalArgumentException("Byte array is too large: " + value.length);
        }
        buf.writeInt(value.length);
        if (value.length > 0) {
            buf.writeBytes(value);
        }
    }

    public static byte[] readBytes(ByteBuf buf) {
        int len = buf.readInt();
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        if (len > MAX_BYTE_ARRAY_BYTES) {
            throw new IllegalArgumentException("Byte array is too large: " + len);
        }
        requireReadable(buf, len, "byte array");
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return bytes;
    }

    private static void requireReadable(ByteBuf buf, int length, String fieldName) {
        if (buf.readableBytes() < length) {
            throw new IllegalArgumentException("Malformed " + fieldName + ": need " + length
                    + " bytes, only " + buf.readableBytes() + " available");
        }
    }
}
