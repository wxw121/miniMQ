# MiniMQ Architecture / MiniMQ 项目架构文档

> Goal: help a new reader understand what runs where, how modules depend on one another, and how records move through the system.  
> 目标：让新读者快速理解系统由哪些模块组成、运行时拓扑如何形成、消息如何在客户端、Broker、存储之间流动。

## 1. Executive Summary / 架构总览

MiniMQ is a modular Java message queue. It separates shared models, protocol encoding, local storage, broker runtime, client SDK, and demo CLI into independent Maven modules. The system is intentionally small, but it preserves the central shape of a partitioned message broker: producers write records to topic partitions, consumers fetch records by offset, and a controller maintains metadata so clients can route requests to the correct leader broker.

MiniMQ 是一个多模块 Java 消息队列系统。它把公共模型、协议编解码、本地存储、Broker 运行时、客户端 SDK、Demo CLI 拆成独立 Maven 模块。项目规模不大，但保留了分区消息系统的核心结构：Producer 按 Topic/Partition 写入消息，Consumer 按 Offset 拉取消息，Controller 维护元数据并指导客户端把请求路由到正确的 Leader Broker。

The architecture is currently a single-replica architecture. A partition has one leader and no follower replica. That means MiniMQ is easy to reason about, but it does not yet provide production durability guarantees such as ISR, quorum replication, leader election, or log truncation after failover.

当前架构是单副本架构。每个分区只有一个 Leader，没有 Follower 副本。因此 MiniMQ 容易理解和调试，但还不具备生产系统常见的 ISR、副本同步、Controller 选举、故障转移后的日志截断等能力。

## 2. Module Architecture / 模块架构

```mermaid
flowchart TB
    subgraph Foundation[Foundation / 基础层]
        common[mq-common<br/>模型、错误码、分区算法、分配算法]
        storage[mq-storage<br/>追加写 PartitionLog]
        protocol[mq-protocol<br/>二进制协议与 Netty 编解码]
    end

    subgraph Runtime[Runtime / 运行层]
        broker[mq-broker<br/>BrokerServer、Controller、Handler]
        client[mq-client<br/>Producer、Consumer、NetworkClient]
    end

    subgraph Entry[Entry / 入口层]
        demo[mq-demo<br/>BrokerMain、DemoCli、E2E]
    end

    common --> storage
    common --> protocol
    common --> broker
    common --> client
    storage --> broker
    protocol --> broker
    protocol --> client
    broker --> demo
    client --> demo
```

| Module | Responsibility / 职责 | Key classes / 关键类 |
| --- | --- | --- |
| `mq-common` | Shared domain objects, error codes, Murmur2 partitioning, range assignment, assignment serialization. / 公共领域对象、错误码、Murmur2 分区、Range 分配、分配结果序列化。 | `ErrorCode`, `Record`, `Murmur2Partitioner`, `RangeAssignor`, `AssignmentSerde` |
| `mq-storage` | Local append-only partition log. / 本地追加写分区日志。 | `PartitionLog`, `FilePartitionLog`, `LogManager` |
| `mq-protocol` | Binary frame format, request/response headers, API messages, Netty decoder/encoder. / 二进制帧格式、请求/响应头、API 消息、Netty 编解码器。 | `MqFrame`, `ProtocolSerdes`, `MqDecoder`, `MqEncoder`, `ProtocolMessages` |
| `mq-broker` | Server runtime, metadata controller, group coordinator, request handling, metadata sync. / 服务端运行时、元数据 Controller、消费组协调器、请求处理、元数据同步。 | `BrokerServer`, `MqRequestHandler`, `ClusterController`, `GroupCoordinator`, `MetadataSync` |
| `mq-client` | Producer and Consumer SDK. / 生产者和消费者 SDK。 | `NetworkClient`, `Producer`, `Consumer` |
| `mq-demo` | CLI command entry and end-to-end demo. / 命令行入口和端到端演示。 | `DemoCli`, `BrokerMain`, `MiniMqE2ETest` |

## 3. Runtime Topology / 运行时拓扑

The common demo topology uses three brokers. Broker 0 starts as controller. Broker 1 and Broker 2 register with the controller and fetch metadata. Topic partitions are assigned round-robin across online brokers when the topic is created.

常见 Demo 拓扑为 3 个 Broker。Broker 0 作为 Controller 启动。Broker 1 和 Broker 2 向 Controller 注册，并拉取元数据。创建 Topic 时，Controller 会把分区按照在线 Broker 列表做轮询分配。

```mermaid
flowchart LR
    producer[Producer SDK<br/>生产者]
    consumer1[Consumer SDK<br/>消费者 A]
    consumer2[Consumer SDK<br/>消费者 B]

    subgraph Cluster[MiniMQ Cluster / 集群]
        b0[Broker 0<br/>Controller / 控制器<br/>Partition Leader / 分区 Leader]
        b1[Broker 1<br/>Partition Leader / 分区 Leader]
        b2[Broker 2<br/>Partition Leader / 分区 Leader]
    end

    disk0[(broker-0 data<br/>数据目录)]
    disk1[(broker-1 data<br/>数据目录)]
    disk2[(broker-2 data<br/>数据目录)]

    producer -- metadata/create-topic/group APIs<br/>元数据/建 Topic/消费组 API --> b0
    producer -- produce to leader<br/>向 leader 生产 --> b0
    producer -- produce to leader<br/>向 leader 生产 --> b1
    producer -- produce to leader<br/>向 leader 生产 --> b2

    consumer1 -- join/sync/offset APIs<br/>加组/同步/offset API --> b0
    consumer2 -- join/sync/offset APIs<br/>加组/同步/offset API --> b0
    consumer1 -- fetch assigned partitions<br/>拉取已分配分区 --> b0
    consumer1 -- fetch assigned partitions<br/>拉取已分配分区 --> b1
    consumer2 -- fetch assigned partitions<br/>拉取已分配分区 --> b2

    b0 --> disk0
    b1 --> disk1
    b2 --> disk2
```

## 4. Request Flow / 请求流

### 4.1 Produce Flow / 生产流程

```mermaid
sequenceDiagram
    participant P as Producer / 生产者
    participant C as Controller Broker / 控制器 Broker
    participant L as Leader Broker / 分区 Leader Broker
    participant Log as PartitionLog / 分区日志

    P->>C: METADATA(topic) / 拉取 Topic 元数据
    C-->>P: brokers + partition leaders / Broker 列表与分区 leader
    P->>P: choose partition by explicit id or Murmur2(key) / 按显式分区或 Murmur2(key) 选分区
    P->>L: PRODUCE(topic, partition, records) / 生产消息
    L->>L: ensureLeader(topic, partition) / 校验本节点是 leader
    L->>Log: append(key, value) / 追加写日志
    Log-->>L: baseOffset / 起始 offset
    L-->>P: ProduceResponse(topic, partition, baseOffset) / 生产响应
```

Producer 首先向 Controller 拉取 Topic 元数据。如果消息指定了 `partitionId`，Producer 直接使用该分区；如果未指定但有 key，则通过 Kafka 风格 Murmur2 hash 计算分区；如果没有 key，则默认分区 0。随后 Producer 根据分区 leader 连接对应 Broker 并发送 `PRODUCE` 请求。

The producer first asks the controller for metadata. If the record has an explicit partition id, that partition is used. If no partition id is present but a key exists, the producer uses Murmur2 hashing. If no key exists, partition 0 is used. The request is then routed to the leader broker.

### 4.2 Fetch Flow / 拉取流程

```mermaid
sequenceDiagram
    participant C as Consumer / 消费者
    participant Ctrl as Controller / 控制器
    participant L as Leader Broker / 分区 Leader Broker
    participant Log as PartitionLog / 分区日志

    C->>Ctrl: JOIN_GROUP(group, member, topic) / 加入消费组
    Ctrl-->>C: generation, memberId, leaderMemberId, members / 代数、成员 ID、leader 成员、成员列表
    C->>Ctrl: SYNC_GROUP(group, generation, assignment) / 同步分配
    Ctrl-->>C: assigned partitions / 已分配分区
    C->>Ctrl: OFFSET_FETCH(group, topic, partition) / 拉取已提交 offset
    Ctrl-->>C: committed offset or -1 / 已提交 offset 或 -1
    C->>L: FETCH(topic, partition, offset, maxBytes) / 拉取消息
    L->>Log: read(fromOffset, maxBytes) / 从指定 offset 读取
    Log-->>L: records + highWatermark / 消息与高水位
    L-->>C: FetchResponse / 拉取响应
```

Consumer 订阅 Topic 时会加入 Consumer Group。组内 leader 负责根据成员列表和 Topic 分区数计算 Range Assignment，然后通过 `SYNC_GROUP` 写回 Controller。普通成员通过 `SYNC_GROUP` 获取自己的分区列表。消费后，客户端通过 `OFFSET_COMMIT` 提交进度。

When a consumer subscribes, it joins a consumer group. The group leader computes a range assignment and submits it through `SYNC_GROUP`. Every member receives its partition list from the coordinator. After processing records, the consumer commits offsets through `OFFSET_COMMIT`.

## 5. Data Plane And Control Plane / 数据面与控制面

MiniMQ separates requests into two conceptual planes:

MiniMQ 可以从概念上拆成两个面：

| Plane / 平面 | APIs / API | Description / 说明 |
| --- | --- | --- |
| Control plane / 控制面 | `CREATE_TOPIC`, `METADATA`, `JOIN_GROUP`, `SYNC_GROUP`, `HEARTBEAT`, `OFFSET_COMMIT`, `OFFSET_FETCH`, `REGISTER_BROKER` | Handles metadata, group membership, assignments, offsets, and broker registration. / 处理元数据、组成员、分配、offset 和 Broker 注册。 |
| Data plane / 数据面 | `PRODUCE`, `FETCH` | Handles record writes and reads against partition logs. / 处理分区日志上的消息写入与读取。 |

The controller broker currently serves all control-plane APIs. Data-plane APIs are served by the leader broker of each partition. This is a clean split for readers: controller code lives mainly in `ClusterController` and `GroupCoordinator`; data access flows through `MqRequestHandler`, `PartitionManager`, `LogManager`, and `FilePartitionLog`.

Controller Broker 当前承载所有控制面 API。数据面 API 则由对应分区的 Leader Broker 承载。对读代码的人来说，这条边界很清楚：控制面主要在 `ClusterController` 与 `GroupCoordinator`，数据面主要经过 `MqRequestHandler`、`PartitionManager`、`LogManager`、`FilePartitionLog`。

## 6. Storage Architecture / 存储架构

Each topic-partition has one local log file named:

每个 Topic-Partition 对应一个本地日志文件：

```text
{dataDir}/{topic}-{partitionId}.log
```

The broker does not keep segment files yet. It writes all records for one partition into a single append-only file. This is simple and good for understanding offset recovery, but V2 should introduce segmented logs to support deletion, compaction, and faster recovery.

当前还没有 Segment 文件，一个分区的所有消息都写入同一个 append-only 文件。这有利于理解 offset 恢复，但后续版本需要引入日志分段，以支持删除、压缩和更快恢复。

```mermaid
flowchart LR
    handler[MqRequestHandler<br/>请求处理器]
    pm[PartitionManager<br/>分区管理器]
    lm[LogManager<br/>日志管理器]
    fpl[FilePartitionLog<br/>文件分区日志]
    file[(orders-0.log<br/>分区日志文件)]

    handler --> pm
    pm --> lm
    lm --> fpl
    fpl --> file
```

## 7. Deployment Shape / 部署形态

MiniMQ can be launched as a fat jar from `mq-demo`. The first broker is usually started with `--controller`; followers are started with `--controller-addr host:port`.

MiniMQ 可以通过 `mq-demo` 打出的 fat jar 启动。第一个 Broker 通常加 `--controller`，其他 Broker 通过 `--controller-addr host:port` 指向 Controller。

| Broker / 节点 | Example role / 示例角色 | Example port / 示例端口 | Data dir / 数据目录 |
| --- | --- | ---: | --- |
| Broker 0 | Controller + leader candidate / Controller 与 leader 候选节点 | 9092 | `./data/broker-0` |
| Broker 1 | Broker + leader candidate / Broker 与 leader 候选节点 | 9093 | `./data/broker-1` |
| Broker 2 | Broker + leader candidate / Broker 与 leader 候选节点 | 9094 | `./data/broker-2` |

## 8. Architectural Constraints / 架构约束

1. There is no replication yet. If a broker loses its data directory, records on its partitions are lost. / 目前还没有副本机制。如果 Broker 丢失数据目录，该节点负责分区上的消息会丢失。
2. There is no controller election. If the controller exits, the cluster does not elect a replacement. / 目前没有 Controller 选举。如果 Controller 退出，集群不会自动选出替代节点。
3. Metadata is persisted as one JSON document. This is easy to inspect but not a scalable metadata log. / 元数据以单个 JSON 文档持久化，易读但不是可扩展的元数据日志。
4. Consumer group rebalancing is simplified. There is no full Kafka-style rebalance protocol, session management, or cooperative assignment. / 消费组再均衡经过简化，还没有完整 Kafka 风格的 rebalance 协议、会话管理或协作式分配。
5. The client SDK keeps simple broker connections and does not yet implement reconnect/backoff policies. / 客户端 SDK 只维护简单 Broker 连接，还没有重连和退避策略。
6. The append-only log has no retention, compaction, or segment rolling. / 追加写日志还没有保留策略、压缩或分段滚动。

## 9. Architecture Glossary / 架构名词表

| Term / 术语 | 中文解释 | English explanation / 英文解释 |
| --- | --- | --- |
| Data plane | 处理消息读写的路径，例如 `PRODUCE` 和 `FETCH`。 | The path that handles record reads and writes. |
| Control plane | 处理元数据、组成员、分配和 offset 的路径。 | The path that manages metadata, groups, assignments, and offsets. |
| Leader broker | 某个分区当前负责读写的 Broker。 | The broker that serves reads/writes for a partition. |
| Controller broker | 管理集群元数据和消费组状态的 Broker。 | The broker managing cluster metadata and group state. |
| Fat jar | 包含项目类和依赖的可执行 jar。 | An executable jar including project classes and dependencies. |
| Topology | 系统运行时节点和连接关系。 | The runtime shape of nodes and their connections. |
