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
