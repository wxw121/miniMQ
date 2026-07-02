package com.minimq.protocol.codec;

import com.minimq.protocol.MqFrame;
import com.minimq.protocol.ProtocolSerdes;
import com.minimq.protocol.message.MqRequestHeader;
import com.minimq.protocol.message.MqResponseHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Netty 入站解码器，校验帧头并把完整二进制帧还原成 payload。
 */
public class MqDecoder extends ByteToMessageDecoder {
    @Override
    // 解码器在数据不足时保留 readerIndex，等待后续 TCP 分片补齐完整帧。
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 7) {
            return;
        }
        in.markReaderIndex();
        short magic = in.readShort();
        if (magic != MqFrame.MAGIC) {
            in.resetReaderIndex();
            throw new IllegalStateException("Invalid magic: " + magic);
        }
        byte version = in.readByte();
        if (version != MqFrame.VERSION) {
            in.resetReaderIndex();
            throw new IllegalStateException("Unsupported version: " + version);
        }
        int length = in.readInt();
        if (length < 0 || length > MqFrame.MAX_FRAME_BYTES) {
            in.resetReaderIndex();
            throw new IllegalArgumentException("Invalid frame length: " + length);
        }
        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }
        ByteBuf payload = in.readRetainedSlice(length);
        out.add(payload);
    }

    public static DecodedRequest decodeRequest(ByteBuf payload) {
        MqRequestHeader header = ProtocolSerdes.decodeRequestHeader(payload);
        Object body = ProtocolSerdes.decodeRequestBody(header.apiKey(), payload);
        return new DecodedRequest(header, body);
    }

    public static DecodedResponse decodeResponse(ByteBuf payload, short apiKey) {
        MqResponseHeader header = ProtocolSerdes.decodeResponseHeader(payload);
        Object body = header.errorCode() == com.minimq.common.error.ErrorCode.NONE
                ? ProtocolSerdes.decodeResponseBody(apiKey, payload)
                : null;
        return new DecodedResponse(header, body);
    }

    public record DecodedRequest(MqRequestHeader header, Object body) {
    }

    public record DecodedResponse(MqResponseHeader header, Object body) {
    }
}
