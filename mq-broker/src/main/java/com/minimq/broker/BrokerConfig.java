package com.minimq.broker;

import com.minimq.storage.FlushPolicy;
import com.minimq.storage.SegmentedLogConfig;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Broker 启动配置，集中保存 brokerId、监听地址、数据目录和 Controller 模式。
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
        String adminToken) {

    public static final int DEFAULT_COMPACTION_MAX_SEGMENTS = 4;
    public static final int MAINTENANCE_WINDOW_DISABLED = -1;
    public static final FetchTransferMode DEFAULT_FETCH_TRANSFER_MODE = FetchTransferMode.AUTO;
    public static final long DEFAULT_FETCH_TRANSFER_TIMEOUT_MS = 30_000;
    public static final long DEFAULT_BROKER_HEARTBEAT_INTERVAL_MS = 1_000L;
    public static final long DEFAULT_BROKER_SELF_FENCE_TIMEOUT_MS = 3_000L;
    public static final long DEFAULT_CONTROLLER_BROKER_LEASE_TIMEOUT_MS = 5_000L;
    public static final String DEFAULT_CLUSTER_ID = "minimq";
    public static final List<String> DEFAULT_ETCD_ENDPOINTS = List.of("http://localhost:2379");
    public static final int DEFAULT_CONTROLLER_SESSION_TTL_SECONDS = 15;
    public static final long DEFAULT_CONTROLLER_FAILOVER_GRACE_MS = 10_000L;
    public static final long DEFAULT_LEADER_ACTIVATION_TIMEOUT_MS = 5_000L;

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
                FlushPolicy.always(),
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
                adminToken);
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
                adminToken);
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
        String flushPolicyName = "always";
        int flushEveryNAppends = 1;
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
        long brokerHeartbeatIntervalMs = DEFAULT_BROKER_HEARTBEAT_INTERVAL_MS;
        long brokerSelfFenceTimeoutMs = DEFAULT_BROKER_SELF_FENCE_TIMEOUT_MS;
        long controllerBrokerLeaseTimeoutMs = DEFAULT_CONTROLLER_BROKER_LEASE_TIMEOUT_MS;
        String clusterId = DEFAULT_CLUSTER_ID;
        List<String> etcdEndpoints = DEFAULT_ETCD_ENDPOINTS;
        int controllerSessionTtlSeconds = DEFAULT_CONTROLLER_SESSION_TTL_SECONDS;
        long controllerFailoverGraceMs = DEFAULT_CONTROLLER_FAILOVER_GRACE_MS;
        long leaderActivationTimeoutMs = DEFAULT_LEADER_ACTIVATION_TIMEOUT_MS;
        boolean initializeEmptyCluster = false;
        boolean initializeClusterFromLocal = false;
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
                case "--admin-token" -> adminToken = value(args, ++i, "--admin-token");
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
                adminToken);
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
