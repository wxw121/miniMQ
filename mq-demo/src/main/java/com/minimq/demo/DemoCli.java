package com.minimq.demo;

import com.minimq.client.Consumer;
import com.minimq.client.ControllerAwareNetworkClient;
import com.minimq.client.NetworkClient;
import com.minimq.client.Producer;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.client.model.ProducerRecord;
import com.minimq.common.acks.ProduceAcks;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicAdminResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceRunRequest;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.RuntimeMetricsResponse;
import com.minimq.protocol.message.ProtocolMessages.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * 演示用命令行客户端，串联 create-topic、produce、consume 等常见操作。
 */
public final class DemoCli {
    private static final Duration DEFAULT_OPERATOR_REQUEST_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration MAX_OPERATOR_REQUEST_TIMEOUT = Duration.ofMinutes(10);

    // main 只负责命令分发，具体操作拆到私有方法，便于 CLI 测试覆盖单个流程。
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "broker" -> BrokerMain.main(copyTail(args));
            case "create-topic" -> createTopic(args);
            case "produce" -> produce(args);
            case "consume" -> consume(args);
            case "maintenance-report" -> maintenanceReport(args);
            case "maintenance-run" -> maintenanceRun(args);
            case "maintenance-metrics" -> maintenanceMetrics(args);
            case "runtime-metrics" -> runtimeMetrics(args);
            case "ops-status" -> opsStatus(args);
            case "group-status" -> groupStatus(args);
            case "list-topics" -> listTopics(args);
            case "describe-topic" -> describeTopic(args);
            case "delete-topic" -> deleteTopic(args);
            case "increase-partitions" -> increasePartitions(args);
            case "alter-topic-config" -> alterTopicConfig(args);
            case "reassign-partitions" -> reassignPartitions(args);
            case "elect-preferred-leaders" -> electPreferredLeaders(args);
            case "failover-audit" -> failoverAudit(args);
            case "auth-status" -> authStatus(args);
            case "configure-topic-reliability" -> configureTopicReliability(args);
            case "broker-offline" -> brokerOffline(args);
            case "retry-produce" -> retryProduce(args);
            case "dead-letter-produce" -> deadLetterProduce(args);
            case "delayed-produce" -> delayedProduce(args);
            case "benchmark-fetch" -> FetchBenchmark.run(args, System.out);
            default -> printUsage();
        }
    }

    private static void createTopic(String[] args) throws InterruptedException {
        String brokers = arg(args, "--brokers", "localhost:9092");
        String adminToken = arg(args, "--admin-token", null);
        String name = arg(args, "--name", "orders");
        int partitions = Integer.parseInt(arg(args, "--partitions", "3"));
        int replicationFactor = Integer.parseInt(arg(args, "--replication-factor", "1"));
        int minInSyncReplicas = Integer.parseInt(arg(args, "--min-insync-replicas", "1"));
        try (ControllerAwareNetworkClient client =
                     new ControllerAwareNetworkClient(brokers, "demo-admin")) {
            client.connect();
            MqDecoder.DecodedResponse response = client.send(
                    ApiKeys.CREATE_TOPIC_ADMIN,
                    new CreateTopicAdminRequest(
                            adminToken, name, partitions, replicationFactor, minInSyncReplicas));
            client.ensureSuccess(response);
            CreateTopicAdminResponse body = (CreateTopicAdminResponse) response.body();
            System.out.println("Created topic: " + body.topicName()
                    + " partitions=" + partitions
                    + " minInSyncReplicas=" + minInSyncReplicas);
        }
    }

    private static void produce(String[] args) throws Exception {
        String brokers = arg(args, "--brokers", "localhost:9092");
        String topic = arg(args, "--topic", "orders");
        String key = optionalArg(args, "--key");
        String message = arg(args, "--message", "hello");
        ProduceAcks acks = parseProduceAcks(arg(args, "--acks", "leader"));
        int ackTimeoutMs = Integer.parseInt(arg(args, "--ack-timeout-ms",
                Integer.toString(ProduceAcks.DEFAULT_TIMEOUT_MS)));
        try (Producer producer = new Producer(brokers, "demo-producer")) {
            producer.start();
            var result = producer.send(new ProducerRecord(
                    topic, null, key != null ? key.getBytes(StandardCharsets.UTF_8) : null,
                    message.getBytes(StandardCharsets.UTF_8)), acks, ackTimeoutMs);
            System.out.printf("Produced to %s-%d offset=%d%n", result.topic(), result.partitionId(), result.offset());
        }
    }

    private static void consume(String[] args) throws Exception {
        consumeForPolls(args, -1);
    }

    static void consumeForPolls(String[] args, int maxPolls) throws Exception {
        String brokers = arg(args, "--brokers", "localhost:9092");
        String topic = arg(args, "--topic", "orders");
        String group = arg(args, "--group", "demo-group");
        int sessionTimeoutMs = Integer.parseInt(arg(args, "--session-timeout-ms", "10000"));
        try (Consumer consumer = new Consumer(brokers, group, "demo-consumer", sessionTimeoutMs)) {
            consumer.subscribe(topic);
            int polls = 0;
            while (maxPolls < 0 || polls < maxPolls) {
                List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord record : records) {
                    System.out.printf("Consumed %s-%d offset=%d value=%s%n",
                            record.topic(), record.partition(), record.offset(),
                            new String(record.value(), StandardCharsets.UTF_8));
                    consumer.commitSync(record.topic(), record.partition(), record.offset() + 1);
                }
                polls++;
            }
        }
    }

    private static void maintenanceReport(String[] args) throws Exception {
        String adminToken = arg(args, "--admin-token", null);
        MaintenanceReportResponse report = (MaintenanceReportResponse) sendAdmin(
                args,
                ApiKeys.MAINTENANCE_REPORT,
                new MaintenanceReportRequest(adminToken),
                "maintenance report").body();
        printMaintenanceReport(report);
    }

    private static void maintenanceRun(String[] args) throws Exception {
        String adminToken = arg(args, "--admin-token", null);
        MaintenanceReportResponse report = (MaintenanceReportResponse) sendAdmin(
                args,
                ApiKeys.MAINTENANCE_RUN,
                new MaintenanceRunRequest(adminToken),
                "maintenance run").body();
        printMaintenanceReport(report);
    }

    private static void maintenanceMetrics(String[] args) throws Exception {
        String adminToken = arg(args, "--admin-token", null);
        MaintenanceMetricsResponse metrics = (MaintenanceMetricsResponse) sendAdmin(
                args,
                ApiKeys.MAINTENANCE_METRICS,
                new MaintenanceMetricsRequest(adminToken),
                "maintenance metrics").body();
        System.out.println("Maintenance metrics");
        System.out.printf(
                "runs=%d dryRuns=%d deletedBytes=%d deletedSegments=%d compactedBytes=%d compactedSegments=%d skippedDueToReaders=%d cleanupFailures=%d lastRunStatus=%s%n",
                metrics.runs(),
                metrics.dryRuns(),
                metrics.deletedBytes(),
                metrics.deletedSegments(),
                metrics.compactedBytes(),
                metrics.compactedSegments(),
                metrics.skippedDueToReaders(),
                metrics.cleanupFailures(),
                metrics.lastRunStatus());
    }

    private static void runtimeMetrics(String[] args) throws Exception {
        String adminToken = arg(args, "--admin-token", null);
        RuntimeMetricsV2Response metrics = (RuntimeMetricsV2Response) sendBroker(
                args,
                ApiKeys.RUNTIME_METRICS_V2,
                new RuntimeMetricsV2Request(adminToken, 100, 100, 100),
                "runtime metrics").body();
        System.out.println("Runtime metrics");
        long requestCount = metrics.requestsByApiKey().items().stream()
                .mapToLong(RuntimeMetricCount::count)
                .sum();
        double requestRate = metrics.uptimeMs() == 0L
                ? 0.0
                : requestCount * 1_000.0 / metrics.uptimeMs();
        System.out.printf(
                "startedAtEpochMs=%d snapshotAtEpochMs=%d uptimeMs=%d requestRatePerSecond=%.3f produceBytes=%d fetchBytes=%d auditOverwrittenCount=%d%n",
                metrics.startedAtEpochMs(), metrics.snapshotAtEpochMs(), metrics.uptimeMs(),
                requestRate, metrics.produceBytes(), metrics.fetchBytes(), metrics.auditOverwrittenCount());
        System.out.println("requests=" + bounded(metrics.requestsByApiKey()));
        System.out.println("responses=" + bounded(metrics.responsesByErrorCode()));
        System.out.println("latencies=" + bounded(metrics.latencyByApiKey()));
        System.out.println("throttled=" + bounded(metrics.throttledByApiKey()));
        System.out.println("lagMetrics=" + bounded(metrics.lagMetrics()));
    }

    private static void opsStatus(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        OpsStatusResponse status = (OpsStatusResponse) sendBroker(
                args,
                ApiKeys.OPS_STATUS,
                new OpsStatusRequest(
                        credential,
                        Integer.parseInt(arg(args, "--broker-limit", "100")),
                        Integer.parseInt(arg(args, "--partition-limit", "100"))),
                "ops status").body();
        System.out.printf(
                "Ops status targetBrokerId=%d startedAtEpochMs=%d snapshotAtEpochMs=%d "
                        + "role=%s brokerEpoch=%d controllerEpoch=%d metadataVersion=%d "
                        + "metadataFormatVersion=%d transportMode=%s maintenanceState=%s%n",
                status.targetBrokerId(), status.startedAtEpochMs(), status.snapshotAtEpochMs(),
                status.role(), status.brokerEpoch(), status.controllerEpoch(), status.metadataVersion(),
                status.metadataFormatVersion(), status.transportMode(), status.maintenanceState());
        System.out.println("brokers=" + bounded(status.brokers()));
        System.out.println("partitions=" + bounded(status.partitions()));
    }

    private static void groupStatus(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        GroupStatusResponse status = (GroupStatusResponse) sendController(
                args,
                ApiKeys.GROUP_STATUS,
                new GroupStatusRequest(
                        credential,
                        arg(args, "--group", null),
                        optionalArg(args, "--topic"),
                        Integer.parseInt(arg(args, "--member-limit", "100")),
                        Integer.parseInt(arg(args, "--partition-limit", "100"))),
                "group status").body();
        System.out.printf("Group status group=%s topic=%s generation=%d%n",
                status.groupId(), status.topicName(), status.generationId());
        System.out.println("members=" + bounded(status.members()));
        System.out.println("partitionLags=" + bounded(status.partitionLags()));
    }

    private static void listTopics(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        ListTopicsResponse response = (ListTopicsResponse) sendController(
                args, ApiKeys.LIST_TOPICS,
                new ListTopicsRequest(credential, Integer.parseInt(arg(args, "--limit", "100"))),
                "list topics").body();
        System.out.println("Topics " + bounded(response.topicNames()));
    }

    private static void describeTopic(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        DescribeTopicResponse response = (DescribeTopicResponse) sendController(
                args, ApiKeys.DESCRIBE_TOPIC,
                new DescribeTopicRequest(
                        credential,
                        arg(args, "--topic", null),
                        Integer.parseInt(arg(args, "--partition-limit", "100")),
                        hasFlag(args, "--include-deleted")),
                "describe topic").body();
        System.out.printf(
                "Topic %s active=%s metadataVersion=%d retry=%s deadLetter=%s minInSyncReplicas=%d%n",
                response.topicName(), response.active(), response.metadataVersion(),
                response.retryTopic(), response.deadLetterTopic(), response.minInSyncReplicas());
        System.out.println("partitions=" + bounded(response.partitions()));
        if (response.tombstone() != null) {
            System.out.printf("tombstoneState=%s pendingBrokers=%s%n",
                    response.tombstone().state(), bounded(response.tombstone().pendingBrokerIds()));
        }
    }

    private static void deleteTopic(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        DeleteTopicResponse response = (DeleteTopicResponse) sendController(
                args, ApiKeys.DELETE_TOPIC,
                new DeleteTopicRequest(credential, arg(args, "--topic", null)),
                "delete topic").body();
        System.out.printf("Topic deletion topic=%s state=%s tombstoneVersion=%d pendingBrokers=%s%n",
                response.topicName(), response.tombstoneState(), response.tombstoneVersion(),
                bounded(response.pendingBrokerIds()));
    }

    private static void increasePartitions(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        IncreasePartitionsResponse response = (IncreasePartitionsResponse) sendController(
                args, ApiKeys.INCREASE_PARTITIONS,
                new IncreasePartitionsRequest(
                        credential, arg(args, "--topic", null),
                        Integer.parseInt(arg(args, "--target-count", null))),
                "increase partitions").body();
        System.out.printf("Partitions increased topic=%s previous=%d current=%d metadataVersion=%d%n",
                response.topicName(), response.previousPartitionCount(), response.newPartitionCount(),
                response.metadataVersion());
    }

    private static void alterTopicConfig(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        AlterTopicConfigResponse response = (AlterTopicConfigResponse) sendController(
                args, ApiKeys.ALTER_TOPIC_CONFIG,
                new AlterTopicConfigRequest(
                        credential,
                        arg(args, "--topic", null),
                        Integer.parseInt(arg(args, "--min-insync-replicas", null))),
                "alter topic config").body();
        System.out.printf("Topic config topic=%s minInSyncReplicas=%d metadataVersion=%d%n",
                response.topicName(), response.minInSyncReplicas(), response.metadataVersion());
    }

    private static void reassignPartitions(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        List<PartitionReplicaAssignment> assignments = new java.util.ArrayList<>();
        for (String value : repeatedArg(args, "--assignment")) {
            assignments.add(parseAssignment(value));
        }
        ReassignPartitionsResponse response = (ReassignPartitionsResponse) sendController(
                args, ApiKeys.REASSIGN_PARTITIONS,
                new ReassignPartitionsRequest(
                        credential, arg(args, "--topic", null), assignments),
                "reassign partitions").body();
        System.out.printf("Partitions reassigned topic=%s changed=%s metadataVersion=%d%n",
                response.topicName(), response.changedPartitionIds(), response.metadataVersion());
    }

    private static void electPreferredLeaders(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        List<Integer> partitionIds = new java.util.ArrayList<>();
        for (String value : repeatedArg(args, "--partition")) {
            partitionIds.add(Integer.parseInt(value));
        }
        ElectPreferredLeadersResponse response = (ElectPreferredLeadersResponse) sendController(
                args, ApiKeys.ELECT_PREFERRED_LEADERS,
                new ElectPreferredLeadersRequest(
                        credential, arg(args, "--topic", null), partitionIds),
                "elect preferred leaders").body();
        System.out.printf("Preferred leaders elected topic=%s elected=%s metadataVersion=%d%n",
                response.topicName(), response.electedPartitionIds(), response.metadataVersion());
    }

    private static void failoverAudit(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        FailoverAuditResponse response = (FailoverAuditResponse) sendBroker(
                args, ApiKeys.FAILOVER_AUDIT,
                new FailoverAuditRequest(credential, Integer.parseInt(arg(args, "--limit", "100"))),
                "failover audit").body();
        System.out.printf("Failover audit brokerId=%d overwritten=%d events=%s%n",
                response.brokerId(), response.overwrittenCount(), bounded(response.events()));
    }

    private static void authStatus(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        AuthStatusResponse response = (AuthStatusResponse) sendBroker(
                args, ApiKeys.AUTH_STATUS, new AuthStatusRequest(credential), "auth status").body();
        System.out.printf("Auth status targetBrokerId=%d scopes=%s%n",
                response.targetBrokerId(), bounded(response.scopes()));
    }

    private static void configureTopicReliability(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        String retryTopic = optionalArg(args, "--retry-topic");
        String deadLetterTopic = optionalArg(args, "--dead-letter-topic");
        ConfigureTopicReliabilityResponse response = (ConfigureTopicReliabilityResponse) sendController(
                args,
                ApiKeys.CONFIGURE_TOPIC_RELIABILITY,
                new ConfigureTopicReliabilityRequest(
                        credential,
                        arg(args, "--source-topic", null),
                        retryTopic != null,
                        retryTopic,
                        hasFlag(args, "--clear-retry"),
                        deadLetterTopic != null,
                        deadLetterTopic,
                        hasFlag(args, "--clear-dead-letter")),
                "configure topic reliability").body();
        System.out.printf("Reliability configured source=%s retry=%s deadLetter=%s metadataVersion=%d%n",
                response.sourceTopicName(), response.retryTopicName(), response.deadLetterTopicName(),
                response.metadataVersion());
    }

    private static void brokerOffline(String[] args) throws Exception {
        String credential = arg(args, "--admin-token", null);
        MarkBrokerOfflineAdminResponse response = (MarkBrokerOfflineAdminResponse) sendController(
                args, ApiKeys.MARK_BROKER_OFFLINE_ADMIN,
                new MarkBrokerOfflineAdminRequest(
                        credential, Integer.parseInt(arg(args, "--broker-id", null))),
                "mark broker offline").body();
        System.out.printf("Broker offline brokerId=%d metadataVersion=%d%n",
                response.brokerId(), response.metadataVersion());
    }

    private static void delayedProduce(String[] args) throws Exception {
        String brokers = arg(args, "--brokers", "localhost:9092");
        try (Producer producer = new Producer(brokers, "demo-delayed-producer")) {
            producer.start();
            var result = producer.sendDelayed(recordFromArgs(args, "--topic"),
                    Long.parseLong(arg(args, "--deliver-at-epoch-ms", null)));
            System.out.printf("Delayed produce topic=%s partition=%d offset=%d%n",
                    result.topic(), result.partitionId(), result.offset());
        }
    }

    private static void retryProduce(String[] args) throws Exception {
        String brokers = arg(args, "--brokers", "localhost:9092");
        String source = arg(args, "--source-topic", null);
        try (Producer producer = new Producer(brokers, "demo-retry-producer")) {
            producer.start();
            var result = producer.sendToRetry(
                    source,
                    recordFromArgs(args, "--source-topic"),
                    Long.parseLong(arg(args, "--deliver-at-epoch-ms", null)));
            System.out.printf("Retry produce topic=%s partition=%d offset=%d%n",
                    result.topic(), result.partitionId(), result.offset());
        }
    }

    private static void deadLetterProduce(String[] args) throws Exception {
        String brokers = arg(args, "--brokers", "localhost:9092");
        String source = arg(args, "--source-topic", null);
        try (Producer producer = new Producer(brokers, "demo-dead-letter-producer")) {
            producer.start();
            var result = producer.sendToDeadLetter(source, recordFromArgs(args, "--source-topic"));
            System.out.printf("Dead-letter produce topic=%s partition=%d offset=%d%n",
                    result.topic(), result.partitionId(), result.offset());
        }
    }

    private static ProducerRecord recordFromArgs(String[] args, String topicFlag) {
        String key = optionalArg(args, "--key");
        return new ProducerRecord(
                arg(args, topicFlag, null),
                null,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                arg(args, "--message", null).getBytes(StandardCharsets.UTF_8));
    }

    private static String bounded(V4BoundedList<?> values) {
        return "total=" + values.total() + " truncated=" + values.truncated()
                + " items=" + values.items();
    }

    private static MqDecoder.DecodedResponse sendAdmin(
            String[] args,
            short apiKey,
            Object request,
            String operation) throws InterruptedException {
        String broker = arg(args, "--broker", null);
        String[] hp = parseSingleHostPort(broker);
        System.out.println("Maintenance target: " + hp[0] + ":" + hp[1]);
        try (NetworkClient client = new NetworkClient("demo-admin")) {
            client.connect(hp[0], Integer.parseInt(hp[1]));
            MqDecoder.DecodedResponse response = client.send(apiKey, request);
            client.ensureSuccess(response, operation);
            return response;
        }
    }

    private static MqDecoder.DecodedResponse sendBroker(
            String[] args, short apiKey, Object request, String operation) throws InterruptedException {
        String broker = arg(args, "--broker", null);
        String[] hp = parseSingleHostPort(broker);
        try (NetworkClient client = new NetworkClient("demo-operator")) {
            client.connect(hp[0], Integer.parseInt(hp[1]));
            MqDecoder.DecodedResponse response = client.send(apiKey, request);
            client.ensureSuccess(response, operation);
            return response;
        }
    }

    private static MqDecoder.DecodedResponse sendController(
            String[] args, short apiKey, Object request, String operation) throws InterruptedException {
        String brokers = arg(args, "--brokers", "localhost:9092");
        try (ControllerAwareNetworkClient client =
                     new ControllerAwareNetworkClient(
                             brokers, "demo-controller-operator", operatorRequestTimeout(args))) {
            client.connect();
            MqDecoder.DecodedResponse response = client.send(apiKey, request);
            client.ensureSuccess(response, operation);
            return response;
        }
    }

    private static void printMaintenanceReport(MaintenanceReportResponse report) {
        System.out.println("Maintenance report");
        System.out.printf(
                "dryRun=%s status=%s deletedBytes=%d deletedSegments=%d compactedBytes=%d compactedSegments=%d skippedDueToReaders=%d cleanupFailures=%d%n",
                report.dryRun(),
                report.status(),
                report.deletedBytes(),
                report.deletedSegments(),
                report.compactedBytes(),
                report.compactedSegments(),
                report.skippedDueToReaders(),
                report.cleanupFailures());
        if (report.segmentReports().isEmpty()) {
            System.out.println("segments=0");
            return;
        }
        System.out.println("segments=" + report.segmentReports().size());
        for (var segment : report.segmentReports()) {
            System.out.printf(
                    "%s action=%s baseOffset=%d firstOffset=%d lastOffset=%d sizeBytes=%d%n",
                    segment.partition(),
                    segment.action(),
                    segment.baseOffset(),
                    segment.firstOffset(),
                    segment.lastOffset(),
                    segment.sizeBytes());
        }
    }

    static ProduceAcks parseProduceAcks(String value) {
        return switch (value) {
            case "leader" -> ProduceAcks.LEADER;
            case "all" -> ProduceAcks.ALL;
            default -> throw new IllegalArgumentException("--acks must be leader or all");
        };
    }

    static String arg(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(name)) {
                if (i == args.length - 1 || args[i + 1].startsWith("--")) {
                    throw new IllegalArgumentException("Missing " + name);
                }
                return args[i + 1];
            }
        }
        if (defaultValue == null) {
            throw new IllegalArgumentException("Missing " + name);
        }
        return defaultValue;
    }

    static String optionalArg(String[] args, String name) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(name)) {
                if (i == args.length - 1 || args[i + 1].startsWith("--")) {
                    throw new IllegalArgumentException("Missing " + name);
                }
                return args[i + 1];
            }
        }
        return null;
    }

    static List<String> repeatedArg(String[] args, String name) {
        List<String> values = new java.util.ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(name)) {
                if (i == args.length - 1 || args[i + 1].startsWith("--")) {
                    throw new IllegalArgumentException("Missing " + name);
                }
                values.add(args[++i]);
            }
        }
        return values;
    }

    static PartitionReplicaAssignment parseAssignment(String value) {
        if (value == null || !value.contains(":")) {
            throw new IllegalArgumentException(
                    "--assignment must be partitionId:brokerId[,brokerId...]");
        }
        String[] parts = value.split(":", 2);
        int partitionId;
        try {
            partitionId = Integer.parseInt(parts[0].trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("--assignment partition id must be an integer", ex);
        }
        List<Integer> replicas = new java.util.ArrayList<>();
        for (String token : parts[1].split(",")) {
            try {
                replicas.add(Integer.parseInt(token.trim()));
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("--assignment replica ids must be integers", ex);
            }
        }
        return new PartitionReplicaAssignment(partitionId, replicas);
    }

    static boolean hasFlag(String[] args, String name) {
        for (String value : args) {
            if (value.equals(name)) {
                return true;
            }
        }
        return false;
    }

    static Duration operatorRequestTimeout(String[] args) {
        String configured = arg(
                args,
                "--request-timeout-ms",
                Long.toString(DEFAULT_OPERATOR_REQUEST_TIMEOUT.toMillis()));
        long timeoutMs;
        try {
            timeoutMs = Long.parseLong(configured);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    "--request-timeout-ms must be a whole number of milliseconds", ex);
        }
        if (timeoutMs <= 0 || timeoutMs > MAX_OPERATOR_REQUEST_TIMEOUT.toMillis()) {
            throw new IllegalArgumentException(
                    "--request-timeout-ms must be between 1 and "
                            + MAX_OPERATOR_REQUEST_TIMEOUT.toMillis());
        }
        return Duration.ofMillis(timeoutMs);
    }

    static String[] parseFirstHostPort(String brokers) {
        String[] hp = brokers.split(",", 2)[0].split(":", 2);
        if (hp.length != 2 || hp[0].isBlank() || hp[1].isBlank()) {
            throw new IllegalArgumentException("--brokers must contain host:port");
        }
        return hp;
    }

    static String[] parseSingleHostPort(String broker) {
        if (broker.contains(",")) {
            throw new IllegalArgumentException("--broker must contain exactly one host:port");
        }
        String[] hp = broker.split(":", 2);
        if (hp.length != 2 || hp[0].isBlank() || hp[1].isBlank()) {
            throw new IllegalArgumentException("--broker must be host:port");
        }
        int port = Integer.parseInt(hp[1]);
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("--broker port must be between 1 and 65535");
        }
        return new String[] {hp[0], Integer.toString(port)};
    }

    static String[] copyTail(String[] args) {
        String[] tail = new String[args.length - 1];
        System.arraycopy(args, 1, tail, 0, tail.length);
        return tail;
    }

    private static void printUsage() {
        System.out.println("""
                Usage:
                  java -jar mq-demo.jar broker --broker-id 0 --port 9092 --data-dir ./data/broker-0 --etcd-endpoints http://localhost:2379 --initialize-empty-cluster --admin-token <secret>
                  java -jar mq-demo.jar create-topic --brokers localhost:9092 --admin-token <secret> --name orders --partitions 3 [--min-insync-replicas 1]
                  java -jar mq-demo.jar produce --brokers localhost:9092 --topic orders --key user-1 --message hello [--acks leader|all] [--ack-timeout-ms 5000]
                  java -jar mq-demo.jar consume --brokers localhost:9092 --topic orders --group demo-group [--session-timeout-ms 10000]
                  java -jar mq-demo.jar maintenance-report --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar maintenance-run --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar maintenance-metrics --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar runtime-metrics --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar ops-status --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar group-status --brokers localhost:9092 --admin-token <secret> --group demo-group [--topic orders]
                  java -jar mq-demo.jar list-topics --brokers localhost:9092 --admin-token <secret> [--limit 100]
                  java -jar mq-demo.jar describe-topic --brokers localhost:9092 --admin-token <secret> --topic orders [--include-deleted]
                  java -jar mq-demo.jar delete-topic --brokers localhost:9092 --admin-token <secret> --topic orders [--request-timeout-ms 15000]
                  java -jar mq-demo.jar increase-partitions --brokers localhost:9092 --admin-token <secret> --topic orders --target-count 6
                  java -jar mq-demo.jar alter-topic-config --brokers localhost:9092 --admin-token <secret> --topic orders --min-insync-replicas 2
                  java -jar mq-demo.jar reassign-partitions --brokers localhost:9092 --admin-token <secret> --topic orders --assignment 0:1,2,0
                  java -jar mq-demo.jar elect-preferred-leaders --brokers localhost:9092 --admin-token <secret> --topic orders [--partition 0]
                  java -jar mq-demo.jar failover-audit --broker localhost:9092 --admin-token <secret> [--limit 100]
                  java -jar mq-demo.jar auth-status --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar configure-topic-reliability --brokers localhost:9092 --admin-token <secret> --source-topic orders [--retry-topic orders-retry] [--dead-letter-topic orders-dlq]
                  java -jar mq-demo.jar broker-offline --brokers localhost:9092 --admin-token <secret> --broker-id 2
                  java -jar mq-demo.jar retry-produce --brokers localhost:9092 --source-topic orders --message retry --deliver-at-epoch-ms 0
                  java -jar mq-demo.jar dead-letter-produce --brokers localhost:9092 --source-topic orders --message failed
                  java -jar mq-demo.jar delayed-produce --brokers localhost:9092 --topic orders --message later --deliver-at-epoch-ms 0
                  java -jar mq-demo.jar benchmark-fetch --records 256 --value-bytes 32768 --fetch-bytes 8388608 --warmup 10 --iterations 50 --segment-bytes 1048576 --network-transport nio
                """);
    }
}
