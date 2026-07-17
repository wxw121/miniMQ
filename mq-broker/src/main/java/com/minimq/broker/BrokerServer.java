package com.minimq.broker;

import com.minimq.broker.controller.ClusterController;
import com.minimq.broker.controller.GroupCoordinator;
import com.minimq.broker.handler.MqRequestHandler;
import com.minimq.broker.metadata.MetadataStore;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.codec.MqEncoder;
import com.minimq.protocol.message.ProtocolMessages.RegisterBrokerRequest;
import com.minimq.storage.LogManagerConfig;
import com.minimq.storage.LogManager;
import com.minimq.storage.CompactionPolicy;
import com.minimq.storage.LogMaintenanceConfig;
import com.minimq.storage.RetentionPolicy;
import com.minimq.storage.SegmentedLogConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker 网络服务入口，负责启动 Netty、注册 Controller，并暴露 MiniMQ 协议处理链。
 */
public class BrokerServer {
    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

    private final BrokerConfig config;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    private PartitionManager partitionManager;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
    }

    public void start() throws InterruptedException {
        MetadataStore metadataStore = new MetadataStore(config.dataDir(), config.recoverCorruptMetadata());
        ClusterController clusterController = new ClusterController(metadataStore);
        GroupCoordinator groupCoordinator = new GroupCoordinator(clusterController);
        LogManager logManager = new LogManager(
                config.dataDir(),
                new LogManagerConfig(
                        config.flushPolicy(),
                        config.checkpointEveryNAppends(),
                        new SegmentedLogConfig(
                                config.segmentMaxBytes(),
                                config.sparseIndexIntervalRecords()),
                        LogMaintenanceConfig.of(
                                RetentionPolicy.of(config.retentionMs(), config.retentionBytes()),
                                config.compactionEnabled()
                                        ? CompactionPolicy.enabled(
                                                config.compactionMaxSegments(),
                                                config.compactionMaxBytes())
                                        : CompactionPolicy.disabled(),
                                config.maintenanceEveryNAppends(),
                                config.maintenanceWindowStartHour(),
                                config.maintenanceWindowEndHour())));
        partitionManager = new PartitionManager(logManager);

        if (config.controller()) {
            clusterController.registerBroker(new RegisterBrokerRequest(
                    config.brokerId(), config.host(), config.port()));
        } else {
            MetadataSync.registerAndSync(config, clusterController);
        }

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new MqDecoder())
                                .addLast(new MqRequestHandler(
                                        config, clusterController, groupCoordinator, partitionManager))
                                .addLast(new MqEncoder());
                    }
                });

        channelFuture = bootstrap.bind(config.host(), config.port()).sync();
        log.info("Broker {} started on {}:{} controller={}", config.brokerId(), config.host(), config.port(),
                config.controller());
    }

    public void stop() throws InterruptedException {
        if (channelFuture != null) {
            channelFuture.channel().close().sync();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().sync();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().sync();
        }
        if (partitionManager != null) {
            partitionManager.close();
        }
    }
}
