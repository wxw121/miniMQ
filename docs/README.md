# MiniMQ Project Context Index / 项目上下文索引

> Last updated / 最后更新：2026-07-19
> Audience / 适用对象（英文）：new agents, new contributors, reviewers, interview readers
> 适用对象：新 agent 会话、新贡献者、代码审查者、面试/作品集读者

## 1. What This Project Is / 项目是什么

MiniMQ is a compact Java 17 message queue inspired by Kafka-style concepts. It is not a production Kafka replacement. It is a learning-oriented and interview-oriented implementation that demonstrates binary protocol design, append-only storage, broker/controller coordination, producer and consumer SDKs, consumer group assignment, offset commit/fetch, and a small CLI demo.

MiniMQ 是一个 Java 17 实现的轻量消息队列项目，概念上参考 Kafka：二进制协议、追加写日志、Broker/Controller、Producer/Consumer SDK、Consumer Group、Offset 提交与拉取。它不是生产级 Kafka 替代品，而是一个便于理解分布式消息系统核心机制的作品级/学习级项目。

The current implementation focuses on a single-leader partition model. Broker 0 is usually the controller in demo mode. Topics are split into partitions, each partition has a leader broker, and clients use metadata to route produce/fetch requests to the right broker.

当前实现采用单 leader 分区模型。Demo 中通常由 Broker 0 作为 Controller。Topic 被拆分为多个 Partition，每个 Partition 有一个 Leader Broker，客户端先向 Controller 拉取元数据，再把生产和拉取请求路由到对应 Broker。

## 2. Reading Order / 推荐阅读顺序

| Order / 顺序 | Document / 文档 | Purpose / 用途 |
| ---: | --- | --- |
| 1 | [Project Architecture](architecture.md) / [项目架构](architecture.md) | Understand modules, runtime topology, dependency graph, and data/control flows. / 理解模块、运行时拓扑、依赖图、数据流与控制流。 |
| 2 | [System Design](design.md) / [系统设计](design.md) | Understand protocol format, storage format, metadata model, group coordination, and client behavior. / 理解协议格式、存储格式、元数据模型、消费组协调与客户端行为。 |
| 3 | [Testing Guide](testing.md) / [测试文档](testing.md) | Understand tests, coverage, commands, report locations, and quality gates. / 理解测试、覆盖率、命令、报告位置与质量门槛。 |
| 4 | [Roadmap](roadmap.md) / [代办与路线图](roadmap.md) | Understand known limitations, TODOs, V2 features, and recommended next tasks. / 理解已知限制、待办事项、V2 功能与推荐下一步任务。 |
| 5 | [Code Review Fix Log](code-review-2026-07-02.md) / [代码审查修复记录](code-review-2026-07-02.md) | See recently fixed bugs and why they mattered. / 查看近期修复的问题以及它们的重要性。 |
| 5b | [V2.3 Code Review Fix Log](code-review-2026-07-17-v2.3-replication.md) / [V2.3 代码审查修复记录](code-review-2026-07-17-v2.3-replication.md) | See V2.3 replication review findings, `2.3.0` fixes, and `2.3.1` backlog. / 查看 V2.3 副本审查问题、`2.3.0` 修复与 `2.3.1` 待办。 |
| 6 | [Test Report](test-report-2026-07-10-v2-storage.md) / [测试报告](test-report-2026-07-10-v2-storage.md) | See the V2.0 storage recovery closeout results. / 查看 V2.0 存储恢复收口测试结果。 |
| 7 | [V2.3 Replication Test Report](test-report-2026-07-17-v2.3-replication.md) / [V2.3 副本机制测试报告](test-report-2026-07-17-v2.3-replication.md) | See the V2.3 replication release verification. / 查看 V2.3 副本机制发布验证。 |
| 7b | [V2.3.1 Replication Hardening Test Report](test-report-2026-07-18-v2.3.1-replication-hardening.md) | See the V2.3.1 replication hardening release verification. |
| 7c | [V2.4.0 Maintenance Operations Test Report](test-report-2026-07-19-v2.4.0-maintenance-ops.md) | See the V2.4.0 admin protocol and operator CLI verification. |
| 8 | [Troubleshooting Guide](troubleshooting.md) / [故障排查指南](troubleshooting.md) | Resolve common Maven, port, Windows, test report, and GitHub/SSH issues. / 排查 Maven、端口、Windows、测试报告、GitHub/SSH 常见问题。 |

## 3. Current Snapshot / 当前快照

| Item / 项目 | Value / 值 |
| --- | --- |
| Language / 语言 | Java 17 |
| Project version | 2.4.0 |
| Build tool / 构建工具 | Maven multi-module project / Maven 多模块项目 |
| Network framework / 网络框架 | Netty 4.1 |
| Serialization / 序列化 | Custom binary protocol over Netty `ByteBuf` / 基于 Netty `ByteBuf` 的自定义二进制协议 |
| Metadata persistence / 元数据持久化 | JSON via Jackson / 通过 Jackson 写入 JSON |
| Storage / 存储 | WAL-backed segmented log with retention, compaction, dry-run reports, maintenance metrics, zero-copy FETCH sendfile, and ISR-aware replication / 基于 WAL 的分段日志，支持保留策略、压缩、dry-run 报告、维护指标、零拷贝 FETCH sendfile 与 ISR 感知副本 |
| Tests / 测试 | JUnit 5 + Surefire |
| Coverage / 覆盖率 | JaCoCo |
| Latest milestone tag | `v2.3.2-replica-fetch-pooling` |
| Latest release | V2.4.0 maintenance operator interface |
| Latest evidence | [V2.4.0 maintenance operations test report](test-report-2026-07-19-v2.4.0-maintenance-ops.md) |

## 4. Module Map / 模块地图

```mermaid
flowchart LR
    common[mq-common<br/>Models / 模型<br/>Errors / 错误码<br/>Assignor / 分配器<br/>Hash / 哈希]
    storage[mq-storage<br/>Append-only partition log / 追加写分区日志]
    protocol[mq-protocol<br/>Frame / 帧<br/>Codec / 编解码<br/>Messages / 消息]
    broker[mq-broker<br/>Server / 服务端<br/>Controller / 控制器<br/>Handler / 处理器]
    client[mq-client<br/>Producer / 生产者<br/>Consumer / 消费者<br/>NetworkClient / 网络客户端]
    demo[mq-demo<br/>CLI / 命令行<br/>E2E demo / 端到端演示]

    common --> protocol
    common --> broker
    common --> client
    common --> storage
    storage --> broker
    protocol --> broker
    protocol --> client
    broker --> demo
    client --> demo
```

## 5. Fast Commands / 常用命令

Build and test / 构建并运行测试：

```powershell
mvn clean install -gs .mvn/settings.xml -s .mvn/settings.xml
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Generate coverage / 生成覆盖率：

```powershell
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

JaCoCo HTML reports are generated under / JaCoCo HTML 报告生成在以下位置：

```text
mq-common/target/site/jacoco/index.html
mq-protocol/target/site/jacoco/index.html
mq-storage/target/site/jacoco/index.html
mq-broker/target/site/jacoco/index.html
mq-client/target/site/jacoco/index.html
mq-demo/target/site/jacoco/index.html
```

Start a three-broker demo cluster / 启动三节点 Broker 演示集群：

```powershell
java -jar mq-demo/target/mq-demo-2.4.0.jar broker --broker-id 0 --port 9092 --data-dir ./data/broker-0 --controller --admin-token <secret>
java -jar mq-demo/target/mq-demo-2.4.0.jar broker --broker-id 1 --port 9093 --data-dir ./data/broker-1 --controller-addr localhost:9092 --admin-token <secret>
java -jar mq-demo/target/mq-demo-2.4.0.jar broker --broker-id 2 --port 9094 --data-dir ./data/broker-2 --controller-addr localhost:9092 --admin-token <secret>
```

Create, produce, and consume / 创建 Topic、生产消息、消费消息：

```powershell
java -jar mq-demo/target/mq-demo-2.4.0.jar create-topic --brokers localhost:9092 --name orders --partitions 3
java -jar mq-demo/target/mq-demo-2.4.0.jar produce --brokers localhost:9092 --topic orders --key user-1 --message hello
java -jar mq-demo/target/mq-demo-2.4.0.jar consume --brokers localhost:9092 --topic orders --group order-processors
```

Inspect and run storage maintenance / 查看并执行存储维护：

```powershell
java -jar mq-demo/target/mq-demo-2.4.0.jar maintenance-report --broker localhost:9092 --admin-token <secret>
java -jar mq-demo/target/mq-demo-2.4.0.jar maintenance-run --broker localhost:9092 --admin-token <secret>
java -jar mq-demo/target/mq-demo-2.4.0.jar maintenance-metrics --broker localhost:9092 --admin-token <secret>
```

All brokers require the shared admin token. Invalid maintenance credentials return `UNAUTHORIZED`, while an overlapping destructive run returns `MAINTENANCE_BUSY`; report/run work executes on the broker's dedicated maintenance executor.

Benchmark FETCH heap versus sendfile / 对比 HEAP 与 sendfile 的 FETCH 基准：

```powershell
java -jar mq-demo/target/mq-demo-2.4.0.jar benchmark-fetch --records 256 --value-bytes 32768 --fetch-bytes 8388608 --warmup 10 --iterations 50 --segment-bytes 1048576
```

## 6. Glossary / 专业名词解释

| Term / 术语 | 中文解释 | English explanation / 英文解释 |
| --- | --- | --- |
| Broker | 消息队列服务节点，负责接收生产请求、保存日志、响应消费请求。 | A server node that stores records and serves produce/fetch requests. |
| Controller | 管理集群元数据的角色，例如 Broker 注册、Topic 创建、Partition leader 分配。 | The node responsible for cluster metadata and leadership decisions. |
| Topic | 消息的逻辑分类，例如 `orders`、`payments`。 | A logical stream/category of messages. |
| Partition | Topic 的物理分片，用于并行写入和消费。 | A shard of a topic, enabling parallelism and routing. |
| Offset | 分区内消息的递增位置编号。 | A monotonically increasing position in a partition log. |
| Consumer Group | 一组消费者共享同一个消费进度和分区分配。 | A group of consumers coordinating partition ownership and offsets. |
| Assignment | Consumer Group 中“哪个成员消费哪些分区”的结果。 | The mapping from consumer members to partitions. |
| High Watermark | 当前分区下一条消息的 offset，也可理解为已写入尾部位置。 | The next offset after the last appended record. |
| Append-only Log | 只追加、不原地修改的日志文件。 | A log file that only appends new records. |
| Frame | 网络协议中的完整消息包，包含 magic、version、length、payload。 | A complete network packet with magic, version, length, and payload. |

## 7. New Agent Checklist / 新 Agent 接手清单

1. Read this index first. / 先阅读本索引。
2. Read `architecture.md` for module boundaries and runtime topology. / 阅读 `architecture.md`，理解模块边界和运行时拓扑。
3. Read `design.md` before changing protocol, storage, metadata, or consumer group logic. / 修改协议、存储、元数据或消费组逻辑前先阅读 `design.md`。
4. Run `mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml` before and after changes. / 修改前后都运行完整 Maven 测试命令。
5. Check `docs/test-report-2026-07-09.md` for the V1.3 closeout baseline. / 查看 `docs/test-report-2026-07-09.md` 了解 V1.3 收口基线。
6. Do not treat target files as source of truth; source lives under `src/main/java` and `src/test/java`. / 不要把 `target` 目录当作事实来源，源码位于 `src/main/java` 和 `src/test/java`。
7. Keep JaCoCo overall line coverage at or above 90%. / 保持 JaCoCo 总体行覆盖率不低于 90%。
8. If changing protocol fields, update both `ProtocolSerdesTest` and the design document. / 如果修改协议字段，同时更新 `ProtocolSerdesTest` 和设计文档。
9. If changing topic/partition routing, update `ProducerIntegrationTest`, `ProduceFetchIntegrationTest`, and architecture diagrams. / 如果修改 Topic/Partition 路由，同时更新 `ProducerIntegrationTest`、`ProduceFetchIntegrationTest` 和架构图。
10. If changing CLI behavior, update `DemoCliTest` and command examples in docs. / 如果修改 CLI 行为，同时更新 `DemoCliTest` 和文档中的命令示例。

