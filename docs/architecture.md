# MiniMQ Architecture / MiniMQ 项目架构文档

> Goal: help a new reader understand what runs where, how modules depend on one another, and how records move through the system.
> 目标：让新读者快速理解系统由哪些模块组成、运行时拓扑如何形成、消息如何在客户端、Broker、存储之间流动。

## 1. Executive Summary / 架构总览

MiniMQ is a modular Java message queue. It separates shared models, protocol encoding, local storage, broker runtime, client SDK, and demo CLI into independent Maven modules. The system is intentionally small, but it preserves the central shape of a partitioned message broker: producers write records to topic partitions, consumers fetch records by offset, and a controller maintains metadata so clients can route requests to the correct leader broker.

MiniMQ 是一个多模块 Java 消息队列系统。它把公共模型、协议编解码、本地存储、Broker 运行时、客户端 SDK、Demo CLI 拆成独立 Maven 模块。项目规模不大，但保留了分区消息系统的核心结构：Producer 按 Topic/Partition 写入消息，Consumer 按 Offset 拉取消息，Controller 维护元数据并指导客户端把请求路由到正确的 Leader Broker。

The architecture supports partition replicas, physical and visible ISR-aware watermarks, persisted replica state, Broker heartbeat leases, etcd-backed Controller election, exact data-plane Leader epochs, crash-safe failover truncation, and automatic clean Leader failover. It remains an educational architecture rather than a production quorum system: unclean election, transactions, and exactly-once semantics are not implemented. V5 produce can wait for the current ISR (`acks=ALL`) or return after the Leader append (`acks=LEADER`). `ALL` waits run on a dedicated executor and require `isr.size() >= min.insync.replicas`. Consumers leave with `LEAVE_GROUP` and expire on a per-member session timeout. Operators can alter `min.insync.replicas`, reassign partition replicas, and elect the preferred replica as leader.

当前架构支持分区副本、ISR 感知的物理/可见 high watermark、持久化副本状态、Broker 心跳租约、etcd Controller 选举、精确数据面 Leader epoch、崩溃安全故障转移截断和 clean Leader 自动故障转移。它仍是教学型架构，不是生产级 quorum 系统：unclean election、事务和 exactly-once 语义尚未实现。

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
| `mq-storage` | Local append-only partition log with WAL-backed segmented storage, retention, and sealed-segment compaction. / 基于 WAL 的本地追加写分段分区日志、保留策略和 sealed segment 压缩。 | `PartitionLog`, `SegmentedPartitionLog`, `SegmentFile`, `SparseOffsetIndex`, `LogManager` |
| `mq-protocol` | Binary frame format, request/response headers, API messages, Netty decoder/encoder. / 二进制帧格式、请求/响应头、API 消息、Netty 编解码器。 | `MqFrame`, `ProtocolSerdes`, `MqDecoder`, `MqEncoder`, `ProtocolMessages` |
| `mq-broker` | Server runtime, metadata controller, group coordinator, request handling, metadata sync. / 服务端运行时、元数据 Controller、消费组协调器、请求处理、元数据同步。 | `BrokerServer`, `MqRequestHandler`, `ClusterController`, `GroupCoordinator`, `MetadataSync` |
| `mq-client` | Producer and Consumer SDK, including per-partition linger auto-batch. / 生产者和消费者 SDK，含按分区 linger 攒批。 | `NetworkClient`, `Producer`, `ProducerConfig`, `Consumer` |
| `mq-demo` | CLI command entry and end-to-end demo. / 命令行入口和端到端演示。 | `DemoCli`, `BrokerMain`, `MiniMqE2ETest` |

## 3. Runtime Topology / 运行时拓扑

The common demo topology uses three brokers and a three-member etcd cluster. Every Broker campaigns for the lease-bound Controller role; Broker 0 only initializes a new empty cluster and is not a permanently assigned Controller. Clients use any bootstrap Broker to discover the current Controller. Topic partitions are assigned round-robin across online brokers when the topic is created.

常见 Demo 拓扑为 3 个 Broker 和 3 个 etcd 成员。每个 Broker 都竞选 lease-bound Controller；Broker 0 只负责初始化全新空集群，并不是固定 Controller。客户端通过任意 bootstrap Broker 发现当前 Controller。创建 Topic 时，Controller 会把分区按照在线 Broker 列表做轮询分配。

```mermaid
flowchart LR
    producer[Producer SDK<br/>生产者]
    consumer1[Consumer SDK<br/>消费者 A]
    consumer2[Consumer SDK<br/>消费者 B]

    subgraph Cluster[MiniMQ Cluster / 集群]
        b0[Broker 0<br/>Controller candidate / 候选<br/>Partition Leader / 分区 Leader]
        b1[Broker 1<br/>Controller candidate / 候选<br/>Partition Leader / 分区 Leader]
        b2[Broker 2<br/>Controller candidate / 候选<br/>Partition Leader / 分区 Leader]
        etcd[(3-member etcd<br/>election + metadata CAS)]
    end

    disk0[(broker-0 data<br/>数据目录)]
    disk1[(broker-1 data<br/>数据目录)]
    disk2[(broker-2 data<br/>数据目录)]

    producer -- bootstrap discovery + routed control APIs<br/>发现并路由控制面 API --> b0
    producer -- produce to leader<br/>向 leader 生产 --> b0
    producer -- produce to leader<br/>向 leader 生产 --> b1
    producer -- produce to leader<br/>向 leader 生产 --> b2

    consumer1 -- discovery + join/sync/offset APIs<br/>发现并路由组 API --> b0
    consumer2 -- discovery + join/sync/offset APIs<br/>发现并路由组 API --> b0
    consumer1 -- fetch assigned partitions<br/>拉取已分配分区 --> b0
    consumer1 -- fetch assigned partitions<br/>拉取已分配分区 --> b1
    consumer2 -- fetch assigned partitions<br/>拉取已分配分区 --> b2

    b0 --> disk0
    b1 --> disk1
    b2 --> disk2
    b0 <--> etcd
    b1 <--> etcd
    b2 <--> etcd
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
    P->>P: enqueue by partition; linger or offset()/flush() drains / 按分区入队；linger 或 offset()/flush() 刷出
    P->>L: PRODUCE(topic, partition, records, acks) / 生产消息（可多条）
    L->>L: ensureLeader(topic, partition) / 校验本节点是 leader
    L->>Log: appendBatch(records) / 批量追加写日志
    Log-->>L: baseOffset / 起始 offset
    L-->>P: ProduceResponse(topic, partition, baseOffset) / 生产响应
```

Producer 首先向 Controller 拉取 Topic 元数据。如果消息指定了 `partitionId`，Producer 直接使用该分区；如果未指定但有 key，则通过 Kafka 风格 Murmur2 hash 计算分区；如果没有 key，则默认分区 0。`send(record)` 按分区入队并立即返回；同一分区在 `batchSize=50` / `lingerMs=5` 内共享一次 `PRODUCE`，直到调用方读取 `offset()`、调用 `flush()`/`close()`，或 linger 到期。`send().offset()` 仍是单条往返。`acks=ALL` 时 Leader 在 ISR 高水位追上后再响应。随后 Producer 根据分区 leader 连接对应 Broker。

The producer first asks the controller for metadata. If the record has an explicit partition id, that partition is used. If no partition id is present but a key exists, the producer uses Murmur2 hashing. If no key exists, partition 0 is used. Sequential `send(record)` calls enqueue per partition and share one Produce until `offset()`, `flush()`, linger, or `close()`. The request is then routed to the leader broker.

### 4.2 Fetch Flow / 拉取流程

```mermaid
sequenceDiagram
    participant C as Consumer / 消费者
    participant Ctrl as Controller / 控制器
    participant L as Leader Broker / 分区 Leader Broker
    participant Log as PartitionLog / 分区日志

    C->>Ctrl: JOIN_GROUP(group, member, topic, sessionTimeoutMs) / 加入消费组
    Ctrl-->>C: generation, memberId, leaderMemberId, members / 代数、成员 ID、leader 成员、成员列表
    C->>Ctrl: SYNC_GROUP(group, generation, assignment) / 同步分配
    Ctrl-->>C: assigned partitions / 已分配分区
    C->>Ctrl: OFFSET_FETCH(group, topic, partition) / 拉取已提交 offset
    Ctrl-->>C: committed offset or -1 / 已提交 offset 或 -1
    C->>L: FETCH_V2(topic, partition, offset, maxBytes) / 拉取消息
    L->>Log: read(fromOffset, maxBytes) / 从指定 offset 读取
    Log-->>L: records + visible/physical watermarks / 消息与可见/物理水位
    L-->>C: FetchV2Response / 拉取响应
```

Consumer 订阅 Topic 时会加入 Consumer Group。组内 leader 负责根据成员列表和 Topic 分区数计算 Range Assignment，然后通过 `SYNC_GROUP` 写回 Controller。普通成员通过 `SYNC_GROUP` 获取自己的分区列表。消费后，客户端通过 `OFFSET_COMMIT` 提交进度。每个成员使用 `JOIN_GROUP` 上的 `sessionTimeoutMs`（默认 10 秒）；超时未心跳会被踢出并触发新一代 rebalance。`Consumer.close()` 发送 `LEAVE_GROUP`。`HEARTBEAT` / `OFFSET_COMMIT` 编码带 `generationId`（commit 还带 `memberId`）；解码仍接受省略这些字段的旧 body，但旧客户端在 join 之后会因 generation 0 被协调器拒绝。

When a consumer subscribes, it joins a consumer group. The group leader computes a range assignment and submits it through `SYNC_GROUP`. Every member receives its partition list from the coordinator. After processing records, the consumer commits offsets through `OFFSET_COMMIT`. Each member uses the `sessionTimeoutMs` from `JOIN_GROUP` (default 10 seconds); missed heartbeats evict that member and advance the group generation. `Consumer.close()` sends `LEAVE_GROUP`. Encode writes `generationId` on `HEARTBEAT` and `memberId` plus `generationId` on `OFFSET_COMMIT`; decode still accepts the shorter legacy bodies, but those defaulted fields fail the coordinator check after a real join.

## 5. Data Plane And Control Plane / 数据面与控制面

MiniMQ separates requests into two conceptual planes:

MiniMQ 可以从概念上拆成两个面：

| Plane / 平面 | APIs / API | Description / 说明 |
| --- | --- | --- |
| Control plane / 控制面 | `CREATE_TOPIC`, `METADATA`, `JOIN_GROUP`, `SYNC_GROUP`, `HEARTBEAT`, `LEAVE_GROUP`, `OFFSET_COMMIT`, `OFFSET_FETCH`, `REGISTER_BROKER`, `ALTER_TOPIC_CONFIG`, `REASSIGN_PARTITIONS`, `ELECT_PREFERRED_LEADERS` | Handles metadata, group membership, assignments, offsets, broker registration, topic config, and replica placement. / 处理元数据、组成员、分配、offset、Broker 注册、Topic 配置与副本布局。 |
| Data plane / 数据面 | `PRODUCE`, `PRODUCE_V2`, `FETCH`, `FETCH_V2` | Handles record writes and reads against partition logs. Consumer SDK uses `FETCH_V2`. / 处理分区日志上的消息写入与读取。Consumer SDK 使用 `FETCH_V2`。 |

The currently elected Controller Broker serves controller-scoped APIs. Controller-aware clients discover it through the complete bootstrap list and retry a stale route once. Broker-local status, maintenance, audit, auth, and runtime-metrics APIs target exactly one Broker. Data-plane APIs are served by each partition's Leader. Controller code lives mainly in `ControllerRuntime`, `ClusterController`, and `GroupCoordinator`; data access flows through `MqRequestHandler`, `PartitionManager`, `LogManager`, the WAL/checkpoint layer, and `SegmentedPartitionLog`.

当前选举出的 Controller Broker 承载 Controller-scoped API。Controller-aware 客户端通过完整 bootstrap 列表发现它，并对过期路由重试一次。Broker-local 状态、维护、审计、鉴权和 runtime metrics API 只访问一个明确 Broker。数据面 API 由对应分区的 Leader Broker 承载。控制面主要在 `ControllerRuntime`、`ClusterController` 与 `GroupCoordinator`，数据面主要经过 `MqRequestHandler`、`PartitionManager`、`LogManager`、WAL/checkpoint 层和 `SegmentedPartitionLog`。

## 6. Storage Architecture / 存储架构

Each topic-partition now has one segmented directory under the broker data directory:

每个 Topic-Partition 现在对应 broker 数据目录下的一个分段目录：

```text
{dataDir}/segments/{topic}-{partitionId}/
  catalog.json
  00000000000000000000.log
  00000000000000000000.index
  00000000000000000128.log
  00000000000000000128.index
```

`LogManager` still accepts old single-file `{topic}-{partitionId}.log` data during startup and migrates it into the segmented directory. WAL replay and checkpoint validation run before request handling, so the broker opens request traffic only after the partition logs have caught up or been replayed idempotently.

`LogManager` 在启动时仍会接受旧单文件 `{topic}-{partitionId}.log` 数据，并迁移到分段目录。WAL 重放和 checkpoint 校验会在开放请求处理之前完成，因此 Broker 只有在分区日志追上或完成幂等重放后才对外处理流量。

V4 delayed records use WAL type 8 (`SCHEDULED_DATA_APPEND`) and the ordinary segmented log. A persisted delivery schedule exposes one contiguous visible prefix per partition. Client fetches never cross that prefix, while replica fetches continue to copy physical records and delivery timestamps. Retention and compaction protect only segments containing not-yet-visible records, so unrelated sealed segments remain maintainable.

V4 延迟消息使用 WAL type 8（`SCHEDULED_DATA_APPEND`）和普通分段日志。持久化 delivery schedule 为每个分区维护连续可见前缀；客户端 FETCH 不会越过该前缀，副本 FETCH 仍复制物理记录及投递时间。Retention/compaction 只保护含未到期记录的 segment，不会阻塞无关 sealed segment 的维护。

V2.1.1 maintenance runs after recovery on the same segmented catalog. Retention removes only sealed segments by age or total sealed bytes, compaction rewrites only sealed segments by latest key, and read snapshots delay physical deletion until in-flight fetches leave their segment view. WAL replay skips records below `logStartOffset`, so retained-away offsets are not restored on restart.

V2.1.1 日志维护在恢复完成后基于同一份分段 catalog 执行。保留策略只按时间或 sealed segment 总字节数删除 sealed segment，压缩只按最新 key 重写 sealed segment，读快照会延迟物理删除直到正在进行的 fetch 离开自己的 segment 视图。WAL replay 会跳过低于 `logStartOffset` 的记录，因此已被保留策略移除的 offset 不会在重启时恢复。

V2.1.2 adds one shared retention-then-compaction plan for preview and execution. Dry-run reports segment actions, original log bytes, and offset ranges without mutation. Real maintenance attempts reader quiescence before scanning compaction inputs; an active read snapshot returns `SKIPPED_READERS` immediately, while a successful acquisition blocks new reads only for the bounded planning and mutation interval. `PartitionManager` exposes Java entrypoints for manual execution, preview, and in-memory metric snapshots.

V2.1.2 为预览和真实执行增加统一的“先 retention、后 compaction”规划。Dry-run 会报告 segment 动作、原始日志字节数和 offset 范围，但不修改数据。真实维护会在扫描 compaction 输入前尝试获取 reader quiescence；存在活跃读快照时立即返回 `SKIPPED_READERS`，成功获取后只在受单次预算约束的规划与修改期间阻止新读取。`PartitionManager` 暴露手动执行、预览和内存指标快照的 Java 入口。

```mermaid
flowchart LR
    handler[MqRequestHandler<br/>请求处理器]
    pm[PartitionManager<br/>分区管理器]
    lm[LogManager<br/>日志管理器]
    wal[WriteAheadLog<br/>预写日志]
    spl[SegmentedPartitionLog<br/>分段分区日志]
    catalog[(catalog.json<br/>分段目录)]
    seg[(00000000000000000000.log/.index<br/>segment 与稀疏索引)]

    handler --> pm
    pm --> lm
    lm --> wal
    lm --> spl
    spl --> catalog
    spl --> seg
```

## 7. Deployment Shape / 部署形态

MiniMQ 4.0 is launched with etcd endpoints on every Broker. Brokers campaign for one lease-bound Controller role; only the first node of a new cluster uses `--initialize-empty-cluster` (or `--initialize-cluster-from-local` for an explicit 2.x import).

MiniMQ 4.0 的每个 Broker 都配置 etcd endpoints，并竞选唯一的 lease-bound Controller。全新集群只有首节点使用 `--initialize-empty-cluster`；显式导入 2.x 本地元数据时改用 `--initialize-cluster-from-local`。

| Broker / 节点 | Example role / 示例角色 | Example port / 示例端口 | Data dir / 数据目录 |
| --- | --- | ---: | --- |
| Broker 0 | Cluster initializer + Controller/leader candidate / 集群初始化节点与 Controller/leader 候选节点 | 9092 | `./data/broker-0` |
| Broker 1 | Broker + leader candidate / Broker 与 leader 候选节点 | 9093 | `./data/broker-1` |
| Broker 2 | Broker + leader candidate / Broker 与 leader 候选节点 | 9094 | `./data/broker-2` |

Each Broker owns a `BrokerLeaseService` and `BrokerAvailabilityGuard`. Startup binds the client port while state is `STARTING`, installs an initial metadata revision, reconciles every installed topic tombstone against local storage, registers, and confirms the epoch with a heartbeat before moving to `ACTIVE`. A tombstone-cleanup failure keeps a sticky storage fence. Loss of Controller contact moves the Broker to `FENCED` before the Controller lease expires; recovery uses `RECOVERING` and must obtain a newer valid registration before returning to `ACTIVE`.

每个 Broker 拥有一个 `BrokerLeaseService` 和 `BrokerAvailabilityGuard`。启动时先绑定客户端端口并保持 `STARTING`，安装初始 metadata revision、将其中全部 Topic tombstone 与本地存储对账、完成注册和 epoch 确认心跳后才进入 `ACTIVE`。tombstone 清理失败会保留 sticky storage fence。与 Controller 失联时，Broker 会在 Controller 租约到期前进入 `FENCED`；恢复过程使用 `RECOVERING`，只有重新获得有效注册后才能回到 `ACTIVE`。

The timing contract is `broker heartbeat interval < broker self-fence timeout < controller broker lease timeout`, configured by `--broker-heartbeat-interval-ms`, `--broker-self-fence-timeout-ms`, and `--controller-broker-lease-timeout-ms`.

## 8. Architectural Constraints / 架构约束

1. Replication and clean ISR failover exist, including crash-safe truncation before Leader readiness. MiniMQ still does not perform unclean election; a partition with no online ISR remains frozen. / 已有副本、ISR 内 clean failover 和 Leader ready 前的崩溃安全截断，但仍不执行 unclean election；没有在线 ISR 的分区保持 frozen。
2. Controller election uses etcd lease/election, a monotonically increasing Controller epoch, leader-identity comparison, and metadata revision CAS. / Controller 选举使用 etcd lease/election、单调 Controller epoch、Leader 身份比较和元数据 revision CAS。
3. Shared metadata is persisted in etcd and installed by a monotonic watch. The local JSON document remains only as the explicit 2.x import source. / 共享元数据持久化在 etcd 中，并由单调 watch 安装；本地 JSON 仅保留为显式 2.x 导入源。
4. Consumer group rebalancing is simplified: each member expires after its `JOIN_GROUP.sessionTimeoutMs` without a heartbeat (default 10 seconds, range 1s..300s), but there is no full Kafka-style rebalance protocol or cooperative assignment. / 消费组再均衡经过简化：每个成员超过自己的 `sessionTimeoutMs` 未心跳会被踢出（默认 10 秒，范围 1s..300s），但没有完整 Kafka 风格的 rebalance 协议或协作式分配。
5. The client SDK cycles the complete bootstrap list, discovers the current Controller through any Broker, rejects older metadata versions, carries the routed Leader epoch, and retries one stale route. The SDK has no etcd dependency. / 客户端 SDK 轮询完整 bootstrap 列表、通过任意 Broker 发现当前 Controller、拒绝较旧 metadata version、携带路由时的 Leader epoch，并对过期路由重试一次；SDK 不依赖 etcd。
6. Maintenance report, execution, and metrics are exposed through token-protected admin APIs and single-target CLI commands. Destructive maintenance remains opt-in and Broker-local. / 维护报告、执行和指标已通过令牌保护的管理 API 与单目标 CLI 命令开放；破坏性维护仍需显式触发，并且只作用于本地 Broker。
7. Keys 0-19 keep their original field order. `HEARTBEAT` and `OFFSET_COMMIT` append optional trailing membership fields: decode accepts legacy bodies, encode always writes the current layout, and a legacy client that omits generation fails coordinator checks after join. V3/V4 mixed Broker clusters are unsupported because only V4 internal replication preserves delayed-delivery state. / API 0-19 保留原字段顺序；`HEARTBEAT` 与 `OFFSET_COMMIT` 在末尾追加可选成员字段：解码接受旧 body，编码始终写当前布局，省略 generation 的旧客户端在 join 后会被协调器拒绝。V3/V4 Broker 混合集群不受支持，只有 V4 内部复制会保留延迟投递状态。
8. Topic names are portable path-safe identifiers: 1..249 characters from `[A-Za-z0-9._-]`, excluding `.` and `..`. Deletion is a durable `DELETING -> DELETED` state machine, and tombstoned names remain reserved. / Topic 名称是可移植且路径安全的标识符：长度 1..249，只能使用 `[A-Za-z0-9._-]`，且不能是 `.` 或 `..`；删除是持久化的 `DELETING -> DELETED` 状态机，tombstone 名称永久保留。

## 9. Architecture Glossary / 架构名词表

| Term / 术语 | 中文解释 | English explanation / 英文解释 |
| --- | --- | --- |
| Data plane | 处理消息读写的路径，例如 `PRODUCE` 和 `FETCH`。 | The path that handles record reads and writes. |
| Control plane | 处理元数据、组成员、分配和 offset 的路径。 | The path that manages metadata, groups, assignments, and offsets. |
| Leader broker | 某个分区当前负责读写的 Broker。 | The broker that serves reads/writes for a partition. |
| Controller broker | 管理集群元数据和消费组状态的 Broker。 | The broker managing cluster metadata and group state. |
| Fat jar | 包含项目类和依赖的可执行 jar。 | An executable jar including project classes and dependencies. |
| Topology | 系统运行时节点和连接关系。 | The runtime shape of nodes and their connections. |
