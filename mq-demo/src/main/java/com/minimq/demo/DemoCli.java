package com.minimq.demo;

import com.minimq.client.Consumer;
import com.minimq.client.NetworkClient;
import com.minimq.client.Producer;
import com.minimq.client.model.ConsumerRecord;
import com.minimq.client.model.ProducerRecord;
import com.minimq.protocol.ApiKeys;
import com.minimq.protocol.codec.MqDecoder;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicRequest;
import com.minimq.protocol.message.ProtocolMessages.CreateTopicResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceMetricsResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportRequest;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceReportResponse;
import com.minimq.protocol.message.ProtocolMessages.MaintenanceRunRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * 演示用命令行客户端，串联 create-topic、produce、consume 等常见操作。
 */
public final class DemoCli {
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
            case "benchmark-fetch" -> FetchBenchmark.run(args, System.out);
            default -> printUsage();
        }
    }

    private static void createTopic(String[] args) throws InterruptedException {
        String brokers = arg(args, "--brokers", "localhost:9092");
        String name = arg(args, "--name", "orders");
        int partitions = Integer.parseInt(arg(args, "--partitions", "3"));
        String[] hp = parseFirstHostPort(brokers);
        try (NetworkClient client = new NetworkClient("demo-admin")) {
            client.connect(hp[0], Integer.parseInt(hp[1]));
            MqDecoder.DecodedResponse response = client.send(
                    ApiKeys.CREATE_TOPIC, new CreateTopicRequest(name, partitions));
            client.ensureSuccess(response);
            CreateTopicResponse body = (CreateTopicResponse) response.body();
            System.out.println("Created topic: " + body.topicName() + " partitions=" + partitions);
        }
    }

    private static void produce(String[] args) throws Exception {
        String brokers = arg(args, "--brokers", "localhost:9092");
        String topic = arg(args, "--topic", "orders");
        String key = arg(args, "--key", null);
        String message = arg(args, "--message", "hello");
        try (Producer producer = new Producer(brokers, "demo-producer")) {
            producer.start();
            var result = producer.send(new ProducerRecord(
                    topic, null, key != null ? key.getBytes(StandardCharsets.UTF_8) : null,
                    message.getBytes(StandardCharsets.UTF_8)));
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
        try (Consumer consumer = new Consumer(brokers, group, "demo-consumer")) {
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
                  java -jar mq-demo.jar broker --broker-id 0 --port 9092 --data-dir ./data/broker-0 --controller
                  java -jar mq-demo.jar create-topic --brokers localhost:9092 --name orders --partitions 3
                  java -jar mq-demo.jar produce --brokers localhost:9092 --topic orders --key user-1 --message hello
                  java -jar mq-demo.jar consume --brokers localhost:9092 --topic orders --group demo-group
                  java -jar mq-demo.jar maintenance-report --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar maintenance-run --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar maintenance-metrics --broker localhost:9092 --admin-token <secret>
                  java -jar mq-demo.jar benchmark-fetch --records 256 --value-bytes 32768 --fetch-bytes 8388608 --warmup 10 --iterations 50 --segment-bytes 1048576
                """);
    }
}
