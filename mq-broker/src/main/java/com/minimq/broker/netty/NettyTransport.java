package com.minimq.broker.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;

public record NettyTransport(
        EventLoopGroup bossGroup,
        EventLoopGroup workerGroup,
        Class<? extends ServerChannel> serverChannelClass,
        NetworkTransportMode selectedMode) {

    public void shutdown() throws InterruptedException {
        bossGroup.shutdownGracefully().sync();
        workerGroup.shutdownGracefully().sync();
    }
}
