package com.minimq.broker;

import com.minimq.broker.auth.OperatorScope;
import com.minimq.broker.netty.NetworkTransportMode;
import com.minimq.storage.FlushPolicy;
import com.minimq.storage.LogManagerConfig;
import com.minimq.storage.SegmentedLogConfig;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Broker 启动配置，集中保存网络、存储、集群协调及鉴权相关的配置。
 *
 * @param brokerId broker 在集群内的唯一编号
 * @param host broker 服务绑定的监听地址
 * @param port broker 服务绑定的监听端口
 * @param dataDir broker 数据、日志和本地元数据的根目录
 * @param controller 是否启用旧版的内嵌 Controller 模式；生产环境通常通过 etcd 选主
 * @param controllerHost 旧版 Controller 的主机地址，仅兼容模式使用
 * @param controllerPort 旧版 Controller 的服务端口，仅兼容模式使用
 * @param recoverCorruptMetadata 是否允许启动时尝试恢复损坏的本地元数据
 * @param flushPolicy 追加消息后将 WAL 和分区日志强制刷盘的策略
 * @param checkpointEveryNAppends 一次追加完成时，成功追加的累计条数为 N 的倍数才触发检查点。
 *        批量追加按批次结束时的计数判断。{@code 0} 关闭追加触发的检查点，启动恢复、删除主题和关闭时仍会保存
 * @param segmentMaxBytes 日志段滚动阈值，单位为字节。单条记录超过阈值时仍可写入空段
 * @param sparseIndexIntervalRecords 稀疏索引每间隔多少条记录写入一个索引项
 * @param retentionMs 日志段按时间保留的上限，单位为毫秒；{@code 0} 表示不按时间清理
 * @param retentionBytes 日志段按总容量保留的上限，单位为字节；{@code 0} 表示不按容量清理
 * @param compactionEnabled 是否启用日志压缩（合并同一键的历史记录）
 * @param compactionMaxSegments 单次日志压缩最多处理的日志段数量
 * @param compactionMaxBytes 单次日志压缩最多读取的字节数；{@code 0} 表示不设字节上限
 * @param maintenanceWindowStartHour 维护窗口起始小时（UTC，0–23）；{@code -1} 表示关闭窗口限制
 * @param maintenanceWindowEndHour 维护窗口结束小时（UTC，0–23）；{@code -1} 表示关闭窗口限制
 * @param maintenanceEveryNAppends 追加触发维护的计数间隔。批量追加在整批结束后检查触发条件。
 *        {@code 0} 关闭追加触发的维护，仍可手动执行已启用的保留或压缩策略
 * @param fetchTransferMode Fetch 响应传输模式，决定是否可自动使用零拷贝等优化
 * @param fetchTransferTimeoutMs Fetch 数据传输允许持续的最长时间，单位为毫秒
 * @param networkTransportMode 网络传输实现，例如 NIO、EPOLL 或按运行环境自动选择
 * @param maxInFlightRequests 单个 Broker 的所有连接共享的未完成请求数上限，{@code 0} 表示关闭限流
 * @param brokerHeartbeatIntervalMs broker 向 Controller 续租/发送心跳的间隔，单位为毫秒
 * @param brokerSelfFenceTimeoutMs Broker 自我隔离的超时，单位为毫秒，从最近一次成功确认的租约请求发起时起算
 * @param controllerBrokerLeaseTimeoutMs Controller 判定 broker 租约失效的超时时间，单位为毫秒
 * @param clusterId 集群的逻辑标识，用于隔离 etcd 中的协调和元数据键空间
 * @param etcdEndpoints etcd 集群的访问端点列表
 * @param controllerSessionTtlSeconds Controller 在 etcd 中参与选举和保活的会话 TTL，单位为秒
 * @param controllerFailoverGraceMs Controller 接管后等待 broker 恢复/重新注册的宽限期，单位为毫秒
 * @param leaderActivationTimeoutMs Controller 等待候选 Leader 完成当前 epoch 的 LEADER_READY 握手的时间，单位为毫秒。超时会冻结分区
 * @param initializeEmptyCluster 是否允许首次启动时从空的 etcd 元数据初始化集群
 * @param initializeClusterFromLocal 是否允许用本地 Controller 快照初始化空的 etcd 集群
 * @param operatorCredentials 运维令牌及其权限范围的列表，用于非管理员运维接口鉴权
 * @param clusterToken 集群内部请求携带的共享鉴权令牌；未设置时回退为 {@code adminToken}
 * @param allowLegacyUnauthenticatedCreate 是否允许旧客户端不经鉴权地创建资源，仅兼容迁移使用
 * @param allowLegacyInternalApi 是否允许调用旧版内部 API，仅兼容迁移使用
 * @param adminToken 管理员令牌，拥有管理接口的完全访问权限
 * @param topicDeleteTimeoutMs 删除 Topic 时等待各 broker 完成清理的最长时间，单位为毫秒
 * @param maxScheduledRecords 单个 Broker 接收新延迟消息时的待投递记录数上限。
 *        {@code 0} 会拒绝新写入的未来投递记录。恢复或副本复制得到的延迟记录仍会保留
 */
public record BrokerConfig(
        int brokerId,
        String host,
        int port,
        Path dataDir,
        boolean controller,
        String controllerHost,
        int controllerPort,
        boolean recoverCorruptMetadata,
        FlushPolicy flushPolicy,
        int checkpointEveryNAppends,
        long segmentMaxBytes,
        int sparseIndexIntervalRecords,
        long retentionMs,
        long retentionBytes,
        boolean compactionEnabled,
        int compactionMaxSegments,
        long compactionMaxBytes,
        int maintenanceWindowStartHour,
        int maintenanceWindowEndHour,
        int maintenanceEveryNAppends,
        FetchTransferMode fetchTransferMode,
        long fetchTransferTimeoutMs,
        NetworkTransportMode networkTransportMode,
        int maxInFlightRequests,
        long brokerHeartbeatIntervalMs,
        long brokerSelfFenceTimeoutMs,
        long controllerBrokerLeaseTimeoutMs,
        String clusterId,
        List<String> etcdEndpoints,
        int controllerSessionTtlSeconds,
        long controllerFailoverGraceMs,
        long leaderActivationTimeoutMs,
        boolean initializeEmptyCluster,
        boolean initializeClusterFromLocal,
        List<OperatorCredential> operatorCredentials,
        String clusterToken,
        boolean allowLegacyUnauthenticatedCreate,
        boolean allowLegacyInternalApi,
        String adminToken,
        long topicDeleteTimeoutMs,
        int maxScheduledRecords) {

    /**
     * 运维令牌及其被授予的权限范围。
     *
     * @param token 用于鉴权的运维令牌
     * @param scopes 此令牌允许执行的运维操作集合
     */
    public record OperatorCredential(String token, Set<OperatorScope> scopes) {
        public OperatorCredential {
            if (token == null || token.isBlank()) {
                throw new IllegalArgumentException("operator credential token must not be blank");
            }
            if (scopes == null || scopes.isEmpty()) {
                throw new IllegalArgumentException("operator credential scopes must not be empty");
            }
            scopes = Set.copyOf(scopes);
        }
    }

    /** Source compatibility for callers that construct the pre-capacity V4 record directly. */
    public BrokerConfig(
            int brokerId,
            String host,
            int port,
            Path dataDir,
            boolean controller,
            String controllerHost,
            int controllerPort,
            boolean recoverCorruptMetadata,
            FlushPolicy flushPolicy,
            int checkpointEveryNAppends,
            long segmentMaxBytes,
            int sparseIndexIntervalRecords,
            long retentionMs,
            long retentionBytes,
            boolean compactionEnabled,
            int compactionMaxSegments,
            long compactionMaxBytes,
            int maintenanceWindowStartHour,
            int maintenanceWindowEndHour,
            int maintenanceEveryNAppends,
            FetchTransferMode fetchTransferMode,
            long fetchTransferTimeoutMs,
            NetworkTransportMode networkTransportMode,
            int maxInFlightRequests,
            long brokerHeartbeatIntervalMs,
            long brokerSelfFenceTimeoutMs,
            long controllerBrokerLeaseTimeoutMs,
            String clusterId,
            List<String> etcdEndpoints,
            int controllerSessionTtlSeconds,
            long controllerFailoverGraceMs,
            long leaderActivationTimeoutMs,
            boolean initializeEmptyCluster,
            boolean initializeClusterFromLocal,
            List<OperatorCredential> operatorCredentials,
            String clusterToken,
            boolean allowLegacyUnauthenticatedCreate,
            boolean allowLegacyInternalApi,
            String adminToken,
            long topicDeleteTimeoutMs) {
        this(
                brokerId, host, port, dataDir, controller, controllerHost, controllerPort,
                recoverCorruptMetadata, flushPolicy, checkpointEveryNAppends, segmentMaxBytes,
                sparseIndexIntervalRecords, retentionMs, retentionBytes, compactionEnabled,
                compactionMaxSegments, compactionMaxBytes, maintenanceWindowStartHour,
                maintenanceWindowEndHour, maintenanceEveryNAppends, fetchTransferMode,
                fetchTransferTimeoutMs, networkTransportMode, maxInFlightRequests,
                brokerHeartbeatIntervalMs, brokerSelfFenceTimeoutMs, controllerBrokerLeaseTimeoutMs,
                clusterId, etcdEndpoints, controllerSessionTtlSeconds, controllerFailoverGraceMs,
                leaderActivationTimeoutMs, initializeEmptyCluster, initializeClusterFromLocal,
                operatorCredentials, clusterToken, allowLegacyUnauthenticatedCreate,
                allowLegacyInternalApi, adminToken, topicDeleteTimeoutMs,
                DEFAULT_MAX_SCHEDULED_RECORDS);
    }

    /** Source compatibility for callers that construct the pre-V4 record directly. */
    public BrokerConfig(
            int brokerId,
            String host,
            int port,
            Path dataDir,
            boolean controller,
            String controllerHost,
            int controllerPort,
            boolean recoverCorruptMetadata,
            FlushPolicy flushPolicy,
            int checkpointEveryNAppends,
            long segmentMaxBytes,
            int sparseIndexIntervalRecords,
            long retentionMs,
            long retentionBytes,
            boolean compactionEnabled,
            int compactionMaxSegments,
            long compactionMaxBytes,
            int maintenanceWindowStartHour,
            int maintenanceWindowEndHour,
            int maintenanceEveryNAppends,
            FetchTransferMode fetchTransferMode,
            long fetchTransferTimeoutMs,
            NetworkTransportMode networkTransportMode,
            int maxInFlightRequests,
            long brokerHeartbeatIntervalMs,
            long brokerSelfFenceTimeoutMs,
            long controllerBrokerLeaseTimeoutMs,
            String clusterId,
            List<String> etcdEndpoints,
            int controllerSessionTtlSeconds,
            long controllerFailoverGraceMs,
            long leaderActivationTimeoutMs,
            boolean initializeEmptyCluster,
            boolean initializeClusterFromLocal,
            List<OperatorCredential> operatorCredentials,
            String clusterToken,
            boolean allowLegacyUnauthenticatedCreate,
            boolean allowLegacyInternalApi,
            String adminToken) {
        this(
                brokerId, host, port, dataDir, controller, controllerHost, controllerPort,
                recoverCorruptMetadata, flushPolicy, checkpointEveryNAppends, segmentMaxBytes,
                sparseIndexIntervalRecords, retentionMs, retentionBytes, compactionEnabled,
                compactionMaxSegments, compactionMaxBytes, maintenanceWindowStartHour,
                maintenanceWindowEndHour, maintenanceEveryNAppends, fetchTransferMode,
                fetchTransferTimeoutMs, networkTransportMode, maxInFlightRequests,
                brokerHeartbeatIntervalMs, brokerSelfFenceTimeoutMs, controllerBrokerLeaseTimeoutMs,
                clusterId, etcdEndpoints, controllerSessionTtlSeconds, controllerFailoverGraceMs,
                leaderActivationTimeoutMs, initializeEmptyCluster, initializeClusterFromLocal,
                operatorCredentials, clusterToken, allowLegacyUnauthenticatedCreate,
                allowLegacyInternalApi, adminToken, DEFAULT_TOPIC_DELETE_TIMEOUT_MS);
    }

    public BrokerConfig {
        operatorCredentials = operatorCredentials == null ? List.of() : List.copyOf(operatorCredentials);
        if (clusterToken == null || clusterToken.isBlank()) {
            clusterToken = adminToken;
        }
        if (topicDeleteTimeoutMs <= 0) {
            throw new IllegalArgumentException("topicDeleteTimeoutMs must be positive");
        }
        if (maxScheduledRecords < 0) {
            throw new IllegalArgumentException("maxScheduledRecords must be non-negative");
        }
    }

    public boolean usesLegacyControllerMode() {
        return controller || !"localhost".equals(controllerHost) || controllerPort != 9092;
    }

    /** Internal compatibility hook for in-process tests while the public 3.0 CLI uses discovery. */
    public BrokerConfig withLegacyController(boolean controller, String controllerHost, int controllerPort) {
        return new BrokerConfig(
                brokerId, host, port, dataDir, controller, controllerHost, controllerPort,
                recoverCorruptMetadata, flushPolicy, checkpointEveryNAppends, segmentMaxBytes,
                sparseIndexIntervalRecords, retentionMs, retentionBytes, compactionEnabled,
                compactionMaxSegments, compactionMaxBytes, maintenanceWindowStartHour,
                maintenanceWindowEndHour, maintenanceEveryNAppends, fetchTransferMode,
                fetchTransferTimeoutMs, networkTransportMode, maxInFlightRequests,
                brokerHeartbeatIntervalMs, brokerSelfFenceTimeoutMs,
                controllerBrokerLeaseTimeoutMs, clusterId, etcdEndpoints,
                controllerSessionTtlSeconds, controllerFailoverGraceMs, leaderActivationTimeoutMs,
                initializeEmptyCluster, initializeClusterFromLocal,
                operatorCredentials, clusterToken,
                allowLegacyUnauthenticatedCreate, allowLegacyInternalApi, adminToken,
                topicDeleteTimeoutMs, maxScheduledRecords);
    }

    public static final int DEFAULT_COMPACTION_MAX_SEGMENTS = 4;
    public static final int MAINTENANCE_WINDOW_DISABLED = -1;
    public static final FetchTransferMode DEFAULT_FETCH_TRANSFER_MODE = FetchTransferMode.AUTO;
    public static final long DEFAULT_FETCH_TRANSFER_TIMEOUT_MS = 30_000;
    public static final NetworkTransportMode DEFAULT_NETWORK_TRANSPORT_MODE = NetworkTransportMode.NIO;
    public static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 0;
    public static final long DEFAULT_BROKER_HEARTBEAT_INTERVAL_MS = 1_000L;
    public static final long DEFAULT_BROKER_SELF_FENCE_TIMEOUT_MS = 3_000L;
    public static final long DEFAULT_CONTROLLER_BROKER_LEASE_TIMEOUT_MS = 5_000L;
    public static final String DEFAULT_CLUSTER_ID = "minimq";
    public static final List<String> DEFAULT_ETCD_ENDPOINTS = List.of("http://localhost:2379");
    public static final int DEFAULT_CONTROLLER_SESSION_TTL_SECONDS = 15;
    public static final long DEFAULT_CONTROLLER_FAILOVER_GRACE_MS = 10_000L;
    public static final long DEFAULT_LEADER_ACTIVATION_TIMEOUT_MS = 5_000L;
    public static final long DEFAULT_TOPIC_DELETE_TIMEOUT_MS = 10_000L;
    public static final int DEFAULT_MAX_SCHEDULED_RECORDS =
            LogManagerConfig.DEFAULT_MAX_SCHEDULED_RECORDS;
    public static final int MAX_OPERATOR_CREDENTIALS = 1_000;
    public static final int MAX_OPERATOR_CREDENTIAL_BYTES = 256;

    public BrokerConfig(
            int brokerId,
            String host,
            int port,
            Path dataDir,
            boolean controller,
            String controllerHost,
            int controllerPort,
            boolean recoverCorruptMetadata,
            String adminToken) {
        this(
                brokerId,
                host,
                port,
                dataDir,
                controller,
                controllerHost,
                controllerPort,
                recoverCorruptMetadata,
                LogManagerConfig.defaultFlushPolicy(),
                0,
                SegmentedLogConfig.DEFAULT_SEGMENT_MAX_BYTES,
                SegmentedLogConfig.DEFAULT_SPARSE_INDEX_INTERVAL_RECORDS,
                0,
                0,
                false,
                DEFAULT_COMPACTION_MAX_SEGMENTS,
                0,
                MAINTENANCE_WINDOW_DISABLED,
                MAINTENANCE_WINDOW_DISABLED,
                0,
                DEFAULT_FETCH_TRANSFER_MODE,
                DEFAULT_FETCH_TRANSFER_TIMEOUT_MS,
                DEFAULT_NETWORK_TRANSPORT_MODE,
                DEFAULT_MAX_IN_FLIGHT_REQUESTS,
                DEFAULT_BROKER_HEARTBEAT_INTERVAL_MS,
                DEFAULT_BROKER_SELF_FENCE_TIMEOUT_MS,
                DEFAULT_CONTROLLER_BROKER_LEASE_TIMEOUT_MS,
                DEFAULT_CLUSTER_ID,
                DEFAULT_ETCD_ENDPOINTS,
                DEFAULT_CONTROLLER_SESSION_TTL_SECONDS,
                DEFAULT_CONTROLLER_FAILOVER_GRACE_MS,
                DEFAULT_LEADER_ACTIVATION_TIMEOUT_MS,
                false,
                false,
                List.of(),
                adminToken,
                false,
                false,
                adminToken,
                DEFAULT_TOPIC_DELETE_TIMEOUT_MS);
    }

    public BrokerConfig(
            int brokerId,
            String host,
            int port,
            Path dataDir,
            boolean controller,
            String controllerHost,
            int controllerPort,
            boolean recoverCorruptMetadata,
            FlushPolicy flushPolicy,
            int checkpointEveryNAppends,
            long segmentMaxBytes,
            int sparseIndexIntervalRecords,
            long retentionMs,
            long retentionBytes,
            boolean compactionEnabled,
            int compactionMaxSegments,
            long compactionMaxBytes,
            int maintenanceWindowStartHour,
            int maintenanceWindowEndHour,
            int maintenanceEveryNAppends,
            FetchTransferMode fetchTransferMode,
            long fetchTransferTimeoutMs,
            NetworkTransportMode networkTransportMode,
            int maxInFlightRequests,
            String adminToken) {
        this(
                brokerId,
                host,
                port,
                dataDir,
                controller,
                controllerHost,
                controllerPort,
                recoverCorruptMetadata,
                flushPolicy,
                checkpointEveryNAppends,
                segmentMaxBytes,
                sparseIndexIntervalRecords,
                retentionMs,
                retentionBytes,
                compactionEnabled,
                compactionMaxSegments,
                compactionMaxBytes,
                maintenanceWindowStartHour,
                maintenanceWindowEndHour,
                maintenanceEveryNAppends,
                fetchTransferMode,
                fetchTransferTimeoutMs,
                networkTransportMode,
                maxInFlightRequests,
                DEFAULT_BROKER_HEARTBEAT_INTERVAL_MS,
                DEFAULT_BROKER_SELF_FENCE_TIMEOUT_MS,
                DEFAULT_CONTROLLER_BROKER_LEASE_TIMEOUT_MS,
                DEFAULT_CLUSTER_ID,
                DEFAULT_ETCD_ENDPOINTS,
                DEFAULT_CONTROLLER_SESSION_TTL_SECONDS,
                DEFAULT_CONTROLLER_FAILOVER_GRACE_MS,
                DEFAULT_LEADER_ACTIVATION_TIMEOUT_MS,
                false,
                false,
                List.of(),
                adminToken,
                false,
                false,
                adminToken,
                DEFAULT_TOPIC_DELETE_TIMEOUT_MS);
    }

    public static BrokerConfig parse(String[] args) {
        int brokerId = 0;
        String host = "localhost";
        int port = 9092;
        Path dataDir = Path.of("./data/broker-0");
        boolean controller = false;
        String controllerHost = "localhost";
        int controllerPort = 9092;
        boolean recoverCorruptMetadata = false;
        String flushPolicyName = "every-n-appends";
        int flushEveryNAppends = LogManagerConfig.DEFAULT_FLUSH_EVERY_N_APPENDS;
        long flushIntervalMs = 1_000;
        int checkpointEveryNAppends = 0;
        long segmentMaxBytes = SegmentedLogConfig.DEFAULT_SEGMENT_MAX_BYTES;
        int sparseIndexIntervalRecords = SegmentedLogConfig.DEFAULT_SPARSE_INDEX_INTERVAL_RECORDS;
        long retentionMs = 0;
        long retentionBytes = 0;
        boolean compactionEnabled = false;
        int compactionMaxSegments = DEFAULT_COMPACTION_MAX_SEGMENTS;
        long compactionMaxBytes = 0;
        int maintenanceWindowStartHour = MAINTENANCE_WINDOW_DISABLED;
        int maintenanceWindowEndHour = MAINTENANCE_WINDOW_DISABLED;
        int maintenanceEveryNAppends = 0;
        FetchTransferMode fetchTransferMode = DEFAULT_FETCH_TRANSFER_MODE;
        long fetchTransferTimeoutMs = DEFAULT_FETCH_TRANSFER_TIMEOUT_MS;
        NetworkTransportMode networkTransportMode = DEFAULT_NETWORK_TRANSPORT_MODE;
        int maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        long brokerHeartbeatIntervalMs = DEFAULT_BROKER_HEARTBEAT_INTERVAL_MS;
        long brokerSelfFenceTimeoutMs = DEFAULT_BROKER_SELF_FENCE_TIMEOUT_MS;
        long controllerBrokerLeaseTimeoutMs = DEFAULT_CONTROLLER_BROKER_LEASE_TIMEOUT_MS;
        String clusterId = DEFAULT_CLUSTER_ID;
        List<String> etcdEndpoints = DEFAULT_ETCD_ENDPOINTS;
        int controllerSessionTtlSeconds = DEFAULT_CONTROLLER_SESSION_TTL_SECONDS;
        long controllerFailoverGraceMs = DEFAULT_CONTROLLER_FAILOVER_GRACE_MS;
        long leaderActivationTimeoutMs = DEFAULT_LEADER_ACTIVATION_TIMEOUT_MS;
        long topicDeleteTimeoutMs = DEFAULT_TOPIC_DELETE_TIMEOUT_MS;
        int maxScheduledRecords = DEFAULT_MAX_SCHEDULED_RECORDS;
        boolean initializeEmptyCluster = false;
        boolean initializeClusterFromLocal = false;
        List<OperatorCredential> operatorCredentials = new ArrayList<>();
        String clusterToken = null;
        boolean allowLegacyUnauthenticatedCreate = false;
        boolean allowLegacyInternalApi = false;
        String adminToken = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--broker-id" -> brokerId = Integer.parseInt(value(args, ++i, "--broker-id"));
                case "--host" -> host = value(args, ++i, "--host");
                case "--port" -> port = Integer.parseInt(value(args, ++i, "--port"));
                case "--data-dir" -> dataDir = Path.of(value(args, ++i, "--data-dir"));
                case "--cluster-id" -> clusterId = value(args, ++i, "--cluster-id");
                case "--etcd-endpoints" -> etcdEndpoints =
                        parseEtcdEndpoints(value(args, ++i, "--etcd-endpoints"));
                case "--controller-session-ttl-seconds" -> controllerSessionTtlSeconds =
                        parsePositiveInt(
                                value(args, ++i, "--controller-session-ttl-seconds"),
                                "--controller-session-ttl-seconds");
                case "--controller-failover-grace-ms" -> controllerFailoverGraceMs =
                        parsePositiveLong(
                                value(args, ++i, "--controller-failover-grace-ms"),
                                "--controller-failover-grace-ms");
                case "--leader-activation-timeout-ms" -> leaderActivationTimeoutMs =
                        parsePositiveLong(
                                value(args, ++i, "--leader-activation-timeout-ms"),
                                "--leader-activation-timeout-ms");
                case "--topic-delete-timeout-ms" -> topicDeleteTimeoutMs =
                        parsePositiveLong(
                                value(args, ++i, "--topic-delete-timeout-ms"),
                                "--topic-delete-timeout-ms");
                case "--max-scheduled-records" -> maxScheduledRecords =
                        parseNonNegativeInt(
                                value(args, ++i, "--max-scheduled-records"),
                                "--max-scheduled-records");
                case "--initialize-empty-cluster" -> initializeEmptyCluster = true;
                case "--initialize-cluster-from-local" -> initializeClusterFromLocal = true;
                case "--recover-corrupt-metadata" -> recoverCorruptMetadata = true;
                case "--flush-policy" -> flushPolicyName = value(args, ++i, "--flush-policy");
                case "--flush-every-n-appends" -> flushEveryNAppends =
                        parsePositiveInt(value(args, ++i, "--flush-every-n-appends"), "--flush-every-n-appends");
                case "--flush-interval-ms" -> flushIntervalMs =
                        parsePositiveLong(value(args, ++i, "--flush-interval-ms"), "--flush-interval-ms");
                case "--checkpoint-every-n-appends" -> checkpointEveryNAppends =
                        parseNonNegativeInt(
                                value(args, ++i, "--checkpoint-every-n-appends"),
                                "--checkpoint-every-n-appends");
                case "--segment-max-bytes" -> segmentMaxBytes =
                        parsePositiveLong(value(args, ++i, "--segment-max-bytes"), "--segment-max-bytes");
                case "--sparse-index-interval-records" -> sparseIndexIntervalRecords =
                        parsePositiveInt(
                                value(args, ++i, "--sparse-index-interval-records"),
                                "--sparse-index-interval-records");
                case "--retention-ms" -> retentionMs =
                        parseNonNegativeLong(value(args, ++i, "--retention-ms"), "--retention-ms");
                case "--retention-bytes" -> retentionBytes =
                        parseNonNegativeLong(value(args, ++i, "--retention-bytes"), "--retention-bytes");
                case "--compaction-enabled" -> compactionEnabled = true;
                case "--compaction-max-segments" -> compactionMaxSegments =
                        parsePositiveInt(value(args, ++i, "--compaction-max-segments"), "--compaction-max-segments");
                case "--compaction-max-bytes" -> compactionMaxBytes =
                        parseNonNegativeLong(value(args, ++i, "--compaction-max-bytes"), "--compaction-max-bytes");
                case "--maintenance-window-start-hour" -> maintenanceWindowStartHour =
                        parseMaintenanceWindowHour(
                                value(args, ++i, "--maintenance-window-start-hour"),
                                "--maintenance-window-start-hour");
                case "--maintenance-window-end-hour" -> maintenanceWindowEndHour =
                        parseMaintenanceWindowHour(
                                value(args, ++i, "--maintenance-window-end-hour"),
                                "--maintenance-window-end-hour");
                case "--maintenance-every-n-appends" -> maintenanceEveryNAppends =
                        parseNonNegativeInt(
                                value(args, ++i, "--maintenance-every-n-appends"),
                                "--maintenance-every-n-appends");
                case "--fetch-transfer-mode" -> fetchTransferMode =
                        FetchTransferMode.parse(value(args, ++i, "--fetch-transfer-mode"));
                case "--fetch-transfer-timeout-ms" -> fetchTransferTimeoutMs =
                        parsePositiveLong(
                                value(args, ++i, "--fetch-transfer-timeout-ms"),
                                "--fetch-transfer-timeout-ms");
                case "--network-transport" -> networkTransportMode =
                        NetworkTransportMode.parse(value(args, ++i, "--network-transport"));
                case "--max-in-flight-requests" -> maxInFlightRequests =
                        parseNonNegativeInt(
                                value(args, ++i, "--max-in-flight-requests"),
                                "--max-in-flight-requests");
                case "--broker-heartbeat-interval-ms" -> brokerHeartbeatIntervalMs =
                        parsePositiveLong(
                                value(args, ++i, "--broker-heartbeat-interval-ms"),
                                "--broker-heartbeat-interval-ms");
                case "--broker-self-fence-timeout-ms" -> brokerSelfFenceTimeoutMs =
                        parsePositiveLong(
                                value(args, ++i, "--broker-self-fence-timeout-ms"),
                                "--broker-self-fence-timeout-ms");
                case "--controller-broker-lease-timeout-ms" -> controllerBrokerLeaseTimeoutMs =
                        parsePositiveLong(
                                value(args, ++i, "--controller-broker-lease-timeout-ms"),
                                "--controller-broker-lease-timeout-ms");
                case "--operator-credential" -> operatorCredentials.add(
                        parseOperatorCredential(value(args, ++i, "--operator-credential")));
                case "--cluster-token" -> clusterToken = parseToken(
                        value(args, ++i, "--cluster-token"), "--cluster-token");
                case "--allow-legacy-unauthenticated-create" -> allowLegacyUnauthenticatedCreate = true;
                case "--allow-legacy-internal-api" -> allowLegacyInternalApi = true;
                case "--admin-token" -> adminToken = parseToken(
                        value(args, ++i, "--admin-token"), "--admin-token");
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        validateClusterId(clusterId);
        if (initializeEmptyCluster && initializeClusterFromLocal) {
            throw new IllegalArgumentException(
                    "--initialize-empty-cluster and --initialize-cluster-from-local are mutually exclusive");
        }
        validateMaintenanceWindowPair(maintenanceWindowStartHour, maintenanceWindowEndHour);
        validateBrokerLeaseTiming(
                brokerHeartbeatIntervalMs,
                brokerSelfFenceTimeoutMs,
                controllerBrokerLeaseTimeoutMs);
        validateControlPlaneTiming(
                controllerBrokerLeaseTimeoutMs,
                controllerFailoverGraceMs,
                controllerSessionTtlSeconds);
        if (adminToken == null || adminToken.isBlank()) {
            throw new IllegalArgumentException("--admin-token is required and must not be blank");
        }
        validateOperatorCredentials(operatorCredentials, adminToken);
        if (clusterToken == null) {
            clusterToken = adminToken;
        }
        return new BrokerConfig(
                brokerId,
                host,
                port,
                dataDir,
                controller,
                controllerHost,
                controllerPort,
                recoverCorruptMetadata,
                parseFlushPolicy(flushPolicyName, flushEveryNAppends, flushIntervalMs),
                checkpointEveryNAppends,
                segmentMaxBytes,
                sparseIndexIntervalRecords,
                retentionMs,
                retentionBytes,
                compactionEnabled,
                compactionMaxSegments,
                compactionMaxBytes,
                maintenanceWindowStartHour,
                maintenanceWindowEndHour,
                maintenanceEveryNAppends,
                fetchTransferMode,
                fetchTransferTimeoutMs,
                networkTransportMode,
                maxInFlightRequests,
                brokerHeartbeatIntervalMs,
                brokerSelfFenceTimeoutMs,
                controllerBrokerLeaseTimeoutMs,
                clusterId,
                List.copyOf(etcdEndpoints),
                controllerSessionTtlSeconds,
                controllerFailoverGraceMs,
                leaderActivationTimeoutMs,
                initializeEmptyCluster,
                initializeClusterFromLocal,
                List.copyOf(operatorCredentials),
                clusterToken,
                allowLegacyUnauthenticatedCreate,
                allowLegacyInternalApi,
                adminToken,
                topicDeleteTimeoutMs,
                maxScheduledRecords);
    }

    private static OperatorCredential parseOperatorCredential(String value) {
        int firstEquals = value.indexOf('=');
        if (firstEquals <= 0 || firstEquals != value.lastIndexOf('=')) {
            throw new IllegalArgumentException(
                    "--operator-credential must use token=scope,scope");
        }
        String token = parseToken(value.substring(0, firstEquals), "--operator-credential token");
        String scopesText = value.substring(firstEquals + 1);
        if (scopesText.isBlank()) {
            throw new IllegalArgumentException("--operator-credential scopes must not be blank");
        }
        Set<OperatorScope> scopes = EnumSet.noneOf(OperatorScope.class);
        for (String rawScope : scopesText.split(",", -1)) {
            String normalized = rawScope.trim();
            if (normalized.isEmpty()) {
                throw new IllegalArgumentException("--operator-credential scopes must not contain blanks");
            }
            if (!rawScope.equals(normalized)) {
                throw new IllegalArgumentException("--operator-credential scopes must not contain surrounding whitespace");
            }
            OperatorScope scope = OperatorScope.parse(normalized);
            if (!scopes.add(scope)) {
                throw new IllegalArgumentException("Duplicate operator scope: " + normalized);
            }
        }
        return new OperatorCredential(token, scopes);
    }

    private static String parseToken(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (value.indexOf('=') >= 0 || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " contains an invalid delimiter");
        }
        int bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (bytes < 1 || bytes > MAX_OPERATOR_CREDENTIAL_BYTES) {
            throw new IllegalArgumentException(
                    name + " must encode to 1.." + MAX_OPERATOR_CREDENTIAL_BYTES + " bytes");
        }
        return value;
    }

    private static void validateOperatorCredentials(
            List<OperatorCredential> operatorCredentials,
            String adminToken) {
        if (operatorCredentials.size() > MAX_OPERATOR_CREDENTIALS) {
            throw new IllegalArgumentException(
                    "operator credentials must not exceed " + MAX_OPERATOR_CREDENTIALS);
        }
        Set<String> tokens = new HashSet<>();
        for (OperatorCredential credential : operatorCredentials) {
            if (!tokens.add(credential.token())) {
                throw new IllegalArgumentException("Duplicate operator credential");
            }
            if (credential.token().equals(adminToken)) {
                throw new IllegalArgumentException(
                        "operator credential must not duplicate --admin-token");
            }
        }
    }

    private static List<String> parseEtcdEndpoints(String value) {
        List<String> endpoints = Arrays.stream(value.split(",", -1))
                .map(String::trim)
                .toList();
        if (endpoints.isEmpty() || endpoints.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("--etcd-endpoints must contain non-blank endpoints");
        }
        return endpoints;
    }

    private static void validateClusterId(String clusterId) {
        if (!clusterId.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("--cluster-id must match [a-zA-Z0-9._-]+");
        }
    }

    private static void validateControlPlaneTiming(long leaseMs, long graceMs, int sessionTtlSeconds) {
        long sessionTtlMs = sessionTtlSeconds * 1_000L;
        if (leaseMs > graceMs || graceMs >= sessionTtlMs) {
            throw new IllegalArgumentException(
                    "control-plane timing must satisfy controller lease <= failover grace < session TTL");
        }
    }

    private static String value(String[] args, int index, String name) {
        if (index >= args.length || args[index].startsWith("--")) {
            throw new IllegalArgumentException("Missing value for " + name);
        }
        return args[index];
    }

    private static FlushPolicy parseFlushPolicy(String value, int flushEveryNAppends, long flushIntervalMs) {
        return switch (value) {
            case "always" -> FlushPolicy.always();
            case "every-n-appends" -> FlushPolicy.everyNAppends(flushEveryNAppends);
            case "interval-ms" -> FlushPolicy.intervalMs(flushIntervalMs);
            case "manual-on-close" -> FlushPolicy.manualOnClose();
            default -> throw new IllegalArgumentException("Unknown flush policy: " + value);
        };
    }

    private static int parsePositiveInt(String value, String name) {
        int parsed = Integer.parseInt(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return parsed;
    }

    private static long parsePositiveLong(String value, String name) {
        long parsed = Long.parseLong(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return parsed;
    }

    private static void validateBrokerLeaseTiming(long heartbeatMs, long selfFenceMs, long leaseMs) {
        if (heartbeatMs >= selfFenceMs || selfFenceMs >= leaseMs) {
            throw new IllegalArgumentException(
                    "broker lease timing must satisfy heartbeat < self-fence < controller lease");
        }
    }

    private static int parseNonNegativeInt(String value, String name) {
        int parsed = Integer.parseInt(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return parsed;
    }

    private static long parseNonNegativeLong(String value, String name) {
        long parsed = Long.parseLong(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return parsed;
    }

    private static int parseMaintenanceWindowHour(String value, String name) {
        int parsed = Integer.parseInt(value);
        if (parsed < 0 || parsed > 23) {
            throw new IllegalArgumentException(name + " must be between 0 and 23");
        }
        return parsed;
    }

    private static void validateMaintenanceWindowPair(int startHour, int endHour) {
        boolean startDisabled = startHour == MAINTENANCE_WINDOW_DISABLED;
        boolean endDisabled = endHour == MAINTENANCE_WINDOW_DISABLED;
        if (startDisabled != endDisabled) {
            throw new IllegalArgumentException(
                    "--maintenance-window-start-hour and --maintenance-window-end-hour must be configured together");
        }
    }
}
