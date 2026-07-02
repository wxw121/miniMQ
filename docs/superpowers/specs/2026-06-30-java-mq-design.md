# Java MQ（Kafka 风格）设计规格

**日期：** 2026-06-30  
**状态：** 已批准  
**目标：** 面试/作品集 — 用 Java + Netty 实现 Kafka 风格分布式消息队列 MVP

---

## 1. 背景与目标

### 1.1 项目定位

构建一个 **Kafka 风格日志型消息队列**，用于作品集与面试展示。重点体现：

- 分布式架构（多 Broker 分区分布）
- 高性能网络通信（Netty + 自研二进制协议）
- 消息持久化与顺序消费（Append-Only Log + Offset）
- Consumer Group 协调与再均衡

### 1.2 非目标（V1 不做）

| 能力 | 说明 | 计划版本 |
|------|------|----------|
| 分区副本 / ISR | Leader/Follower 同步 | V2 |
| Controller 选举 HA | Raft/ZK 选主 | V2 |
| Log Segment 分段 | 多 segment + 稀疏索引 | V2 |
| 消息保留策略 | 按时间/大小清理 | V2 |
| 事务消息 / 延迟消息 | RocketMQ 特性 | 不计划 |

### 1.3 需求摘要

| 维度 | 决策 |
|------|------|
| 目标 | 面试/作品集 |
| 消息模型 | Kafka 日志型（Topic + Partition + Offset） |
| 部署 | 3 Broker 集群，分区分布在不同 Broker |
| 技术栈 | Java 17、Maven、Netty 4.x、自研二进制协议 |
| V1 必做 | Producer 发送、Consumer Group + Offset |
| 项目名 | **MiniMQ** |

---

## 2. 架构方案

### 2.1 选定方案：Distributed Lite

3 个独立 Broker 进程；**Broker-0** 兼任 Controller，负责元数据管理与 Consumer Group 协调。Producer/Consumer 通过 Netty 二进制协议通信。

V1 中每个分区仅有一个 Leader Broker（无副本）。多 Broker 体现为 **分区水平分布**，而非高可用副本。

### 2.2 系统架构图

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Producer   │     │  Consumer   │     │  Consumer   │
│   Client    │     │  (group-A)  │     │  (group-A)  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │ Netty             │ Netty             │ Netty
       ▼                   ▼                   ▼
┌──────────────────────────────────────────────────────┐
│              Broker Cluster (3 nodes)                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐      │
│  │  Broker-0   │ │  Broker-1   │ │  Broker-2   │      │
│  │ (Controller)│ │             │ │             │      │
│  │ P0,P3 Leader│ │ P1,P4 Leader│ │ P2 Leader   │      │
│  └─────────────┘ └─────────────┘ └─────────────┘      │
└──────────────────────────────────────────────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
   data/P0.log         data/P1.log         data/P2.log
```

### 2.3 Maven 多模块结构

```
minimq/
├── pom.xml                    # 父 POM
├── mq-common/                 # 公共模型与工具
├── mq-protocol/               # 二进制协议编解码
├── mq-storage/                # Append-Only 存储引擎
├── mq-broker/                 # Broker 服务端
├── mq-client/                 # Java Client SDK
└── mq-demo/                   # 端到端 Demo 与启动脚本
```

### 2.4 模块职责

| 模块 | 职责 |
|------|------|
| `mq-common` | `Message`、`Record`、`TopicMetadata`、`PartitionMetadata`、`BrokerConfig` 等公共模型；常量与工具类 |
| `mq-protocol` | 帧格式、Request/Response 定义、Netty 编解码器（`MqDecoder`/`MqEncoder`） |
| `mq-storage` | 分区级 Append-Only Log：append、按 offset fetch、持久化 |
| `mq-broker` | Netty Server、请求路由、Controller 逻辑、Consumer Group 协调、分区 Leader 管理 |
| `mq-client` | `Producer`、`Consumer` API；连接池、请求发送、响应处理 |
| `mq-demo` | 3 Broker 启动类、CLI 工具、README 演示步骤 |

---

## 3. 二进制协议设计

### 3.1 帧格式

所有通信使用 Length-Prefixed Frame：

```
┌──────────┬──────────┬──────────┬─────────────────┐
│ Magic(2) │ Version  │ Length   │   Payload       │
│  0x4D51  │   (1)    │  (4)     │   (变长)         │
│  ("MQ")  │          │          │                 │
└──────────┴──────────┴──────────┴─────────────────┘
```

- **Magic:** `0x4D51`（"MQ"）
- **Version:** `1`（V1 协议版本）
- **Length:** Payload 字节数（不含 Magic/Version/Length 本身）
- **Payload:** Request 或 Response  body

### 3.2 Request Header / 请求头

```
┌────────────┬────────────┬──────────────────────┐
│ ApiKey(2)  │ CorrId(4)  │ ClientId (2 + bytes) │
└────────────┴────────────┴──────────────────────┘
```

- **ApiKey:** API 类型标识
- **CorrId:** 关联 ID，用于匹配 Request/Response
- **ClientId:** 长度前缀 UTF-8 字符串

### 3.3 Response Header / 响应头

```
┌────────────┬────────────┬────────────┐
│ CorrId(4)  │ ErrorCode  │ Body...    │
│            │   (2)      │            │
└────────────┴────────────┴────────────┘
```

- **ErrorCode:** `0` = 成功，非 0 为错误码（见 3.6）

### 3.4 V1 API 列表

| ApiKey | 名称 | 方向 | 说明 |
|--------|------|------|------|
| 0 | CREATE_TOPIC | Client → Controller (Broker-0) | 创建 Topic，指定分区数 |
| 1 | METADATA | Client → 任意 Broker | 获取 Topic 分区及 Leader Broker 列表 |
| 2 | PRODUCE | Client → Leader Broker | 发送消息到指定 Topic/Partition |
| 3 | FETCH | Client → Leader Broker | 按 offset 拉取消息 |
| 4 | JOIN_GROUP | Client → Controller | Consumer 加入消费组 |
| 5 | SYNC_GROUP | Client → Controller | 获取分区分配结果 |
| 6 | HEARTBEAT | Client → Controller | 维持组成员存活 |
| 7 | OFFSET_COMMIT | Client → Controller | 提交消费位点 |
| 8 | OFFSET_FETCH | Client → Controller | 获取已提交位点 |

### 3.5 关键 Request/Response Body

#### CREATE_TOPIC (ApiKey=0)

**Request / 请求：**
```
TopicName (2 + bytes) | NumPartitions (4)
```

**Response / 响应：**
```
ErrorCode (2) | TopicName (2 + bytes)
```

#### METADATA (ApiKey=1)

**Request / 请求：**
```
TopicName (2 + bytes)   # 空字符串表示获取全部
```

**Response / 响应：**
```
ErrorCode (2)
| BrokerCount (4)
| [ BrokerId(4) + Host(2+bytes) + Port(4) ] * N
| TopicCount (4)
| [ TopicName(2+bytes) + PartitionCount(4)
  | [ PartitionId(4) + LeaderBrokerId(4) ] * M ] * K
```

#### PRODUCE (ApiKey=2)

**Request / 请求：**
```
TopicName (2 + bytes)
| PartitionId (4)
| RecordCount (4)
| [ KeyLen(4) + Key + ValueLen(4) + Value ] * N
```

**Response / 响应：**
```
ErrorCode (2)
| TopicName (2 + bytes)
| PartitionId (4)
| BaseOffset (8)    # 第一条记录的 offset
```

#### FETCH (ApiKey=3)

**Request / 请求：**
```
TopicName (2 + bytes)
| PartitionId (4)
| FetchOffset (8)
| MaxBytes (4)
```

**Response / 响应：**
```
ErrorCode (2)
| TopicName (2 + bytes)
| PartitionId (4)
| HighWatermark (8)
| RecordCount (4)
| [ Offset(8) + KeyLen(4) + Key + ValueLen(4) + Value ] * N
```

#### JOIN_GROUP (ApiKey=4)

**Request / 请求：**
```
GroupId (2 + bytes)
| MemberId (2 + bytes)     # 首次为空，再均衡时带旧 MemberId
| TopicName (2 + bytes)
```

**Response / 响应：**
```
ErrorCode (2)
| GenerationId (4)
| MemberId (2 + bytes)     # Controller 分配的新 MemberId
| LeaderMemberId (2 + bytes)
| MemberCount (4)
| [ MemberId(2+bytes) ] * N
```

#### SYNC_GROUP (ApiKey=5)

**Request / 请求：**
```
GroupId (2 + bytes)
| GenerationId (4)
| MemberId (2 + bytes)
| Assignment (2 + bytes)   # Leader 提交分配方案；Follower 为空
```

**Response / 响应：**
```
ErrorCode (2)
| Assignment (2 + bytes)   # 二进制分配：PartitionCount(4) + [PartitionId(4)]*N
```

#### OFFSET_COMMIT (ApiKey=7)

**Request / 请求：**
```
GroupId (2 + bytes)
| TopicName (2 + bytes)
| PartitionId (4)
| Offset (8)
```

**Response / 响应：**
```
ErrorCode (2)
```

#### OFFSET_FETCH (ApiKey=8)

**Request / 请求：**
```
GroupId (2 + bytes)
| TopicName (2 + bytes)
| PartitionId (4)
```

**Response / 响应：**
```
ErrorCode (2)
| Offset (8)    # -1 表示无已提交位点
```

### 3.6 错误码

| Code | 名称 | 说明 |
|------|------|------|
| 0 | NONE | 成功 |
| 1 | UNKNOWN_TOPIC | Topic 不存在 |
| 2 | UNKNOWN_PARTITION | 分区不存在 |
| 3 | NOT_LEADER | 当前 Broker 不是该分区 Leader |
| 4 | INVALID_REQUEST | 请求格式错误 |
| 5 | GROUP_NOT_FOUND | 消费组不存在 |
| 6 | REBALANCE_IN_PROGRESS | 再均衡进行中，需重试 |

---

## 4. 存储引擎设计

### 4.1 V1 策略：单文件 Append-Only Log

每个分区对应一个 log 文件：

```
{broker-data-dir}/{topic}-{partitionId}.log
```

示例：`data/orders-0.log`、`data/orders-1.log`

### 4.2 记录格式

每条 Record 在文件中连续追加：

```
┌──────────┬──────────┬──────────┬──────────┬─────────┐
│ Offset   │ KeyLen   │ Key      │ ValueLen │ Value   │
│  (8)     │  (4)     │ (变长)   │  (4)     │ (变长)  │
└──────────┴──────────┴──────────┴──────────┴─────────┘
```

- **Offset:** 分区内单调递增，从 0 开始；由 Broker 在 append 时分配
- **KeyLen/ValueLen:** 字节长度；`-1` 表示 null（Key 可为 null）

### 4.3 核心接口

```java
public interface PartitionLog {
    /** 追加记录，返回分配的 offset */
    long append(byte[] key, byte[] value);

    /** 从 offset 起读取，最多 maxBytes，返回 Record 列表 */
    List<Record> read(long fromOffset, int maxBytes);

    /** 当前 high watermark（下一条将分配的 offset） */
    long highWatermark();
}
```

### 4.4 持久化策略

- 每次 `append` 后 `FileChannel.force(false)`（刷盘）
- V1 不做批量刷盘优化；V2 可引入 Page Cache + 定时 flush

### 4.5 元数据持久化

Controller 将集群元数据写入 JSON 文件：

```
{controller-data-dir}/cluster-metadata.json
```

内容包含：Broker 列表、Topic 定义、分区 → Leader Broker 映射、Consumer Group 成员与 offset。

---

## 5. Broker 设计

### 5.1 Broker 启动参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `--broker-id` | Broker 唯一 ID | `0` |
| `--host` | 监听地址 | `localhost` |
| `--port` | 监听端口 | `9092` |
| `--data-dir` | 数据目录 | `./data/broker-0` |
| `--controller` | 是否为 Controller | `true`（仅 Broker-0） |

### 5.2 Controller

V1 固定 **Broker-0** 为 Controller，不做选举。负责：Topic 元数据、Consumer Group 协调、Offset 存储。

### 5.3 请求路由

| ApiKey | 处理节点 |
|--------|----------|
| CREATE_TOPIC, JOIN_GROUP, SYNC_GROUP, HEARTBEAT, OFFSET_COMMIT, OFFSET_FETCH | Controller (Broker-0) |
| METADATA | 任意 Broker（本地缓存 + 转发 Controller 若需要） |
| PRODUCE, FETCH | 分区 Leader Broker；非 Leader 返回 NOT_LEADER |

### 5.4 Topic 创建与分区分配

1. Client 向 Controller 发送 `CREATE_TOPIC`
2. Controller 校验 Topic 不存在
3. 按 **round-robin** 将分区分配到 Broker：`partitionId % brokerCount`
4. 持久化元数据，各 Broker 在本地创建对应分区 log（若为本节点 Leader）

示例：`orders` 3 分区，3 Broker：

| Partition / 分区 | Leader / Leader 节点 |
|-----------|--------|
| 0 | Broker-0 / Broker-0 节点 |
| 1 | Broker-1 / Broker-1 节点 |
| 2 | Broker-2 / Broker-2 节点 |

### 5.5 Producer 路由

1. `MetadataRequest` 获取 Topic 分区与 Leader 列表
2. 若指定 `partitionId`，发往对应 Leader
3. 若指定 `key`，`partitionId = murmur2(key) % numPartitions`（与 Kafka 默认分区器一致）
4. 若均未指定，使用 `round-robin` 或 `partition 0`

---

## 6. Consumer Group 设计

### 6.1 协调流程

```
Consumer / 消费者                 Controller (Broker-0) / 控制器
   │                                      │
   │──── JOIN_GROUP ─────────────────────►│
   │◄─── memberId, generationId ──────────│
   │                                      │
   │──── SYNC_GROUP (空 assignment) ─────►│  (Follower)
   │◄─── 分配的分区列表 ──────────────────│
   │                                      │
   │  或（Leader Member）:                 │
   │──── SYNC_GROUP (assignment) ────────►│
   │◄─── 确认 ────────────────────────────│
   │                                      │
   │──── FETCH (from assigned partition) ─► Leader Broker
   │◄─── Records ─────────────────────────│
   │                                      │
   │──── OFFSET_COMMIT ──────────────────►│
   │                                      │
   │──── HEARTBEAT (每 3s) ──────────────►│
```

### 6.2 分区分配策略

V1 使用 **Range Assignor**（与 Kafka 默认之一类似）：

- 按 Topic 分区 ID 排序
- 成员按 MemberId 字典序排序
- 尽量均匀分配：`ceil(partitions / members)` 个分区 per member

示例：Topic `orders` 3 分区，2 个 Consumer（C0, C1）：

| Consumer | 分配分区 |
|----------|----------|
| C0 / 消费者 C0 | 0, 1 |
| C1 / 消费者 C1 | 2 |

### 6.3 Offset 管理

- 存储位置：Controller 内存 + `cluster-metadata.json` 持久化
- Key：`groupId + topic + partitionId`
- 首次消费：`offset = 0`（或可配置 `auto.offset.reset=latest` 从 highWatermark 开始）
- 提交时机：Consumer 处理完消息后主动 `OFFSET_COMMIT`

### 6.4 再均衡触发

- 新成员 `JOIN_GROUP`
- 成员 `HEARTBEAT` 超时（默认 10s 无心跳视为下线）
- 再均衡期间返回 `REBALANCE_IN_PROGRESS`，Client 需重新 `JOIN_GROUP`

---

## 7. Client API 设计

### 7.1 Producer

```java
Producer producer = Producer.builder()
    .bootstrapServers("localhost:9092,localhost:9093,localhost:9094")
    .clientId("demo-producer")
    .build();

ProducerRecord record = ProducerRecord.builder()
    .topic("orders")
    .key("user-1".getBytes())
    .value("hello".getBytes())
    .build();

ProduceResult result = producer.send(record);
// result.partitionId(), result.offset()
```

### 7.2 Consumer / 消费者

```java
Consumer consumer = Consumer.builder()
    .bootstrapServers("localhost:9092,localhost:9093,localhost:9094")
    .groupId("order-processors")
    .clientId("consumer-1")
    .build();

consumer.subscribe("orders");

while (true) {
    List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord record : records) {
        // 处理消息
        consumer.commitSync(record.topic(), record.partition(), record.offset() + 1);
    }
}
```

### 7.3 CLI 工具（mq-demo）

```bash
# 创建 Topic
java -jar mq-demo.jar create-topic --brokers localhost:9092 --name orders --partitions 3

# 发送消息
java -jar mq-demo.jar produce --brokers localhost:9092 --topic orders --key user-1 --message "hello"

# 消费消息
java -jar mq-demo.jar consume --brokers localhost:9092 --topic orders --group order-processors
```

---

## 8. Demo 场景

### 8.1 启动 3 Broker

```bash
# Terminal / 终端 1
java -jar mq-broker.jar --broker-id 0 --port 9092 --data-dir ./data/broker-0 --controller

# Terminal / 终端 2
java -jar mq-broker.jar --broker-id 1 --port 9093 --data-dir ./data/broker-1

# Terminal / 终端 3
java -jar mq-broker.jar --broker-id 2 --port 9094 --data-dir ./data/broker-2
```

### 8.2 演示流程

1. **创建 Topic** — `orders`，3 分区
2. **Producer 发送 10 条消息** — 带 key，观察分区分布
3. **启动 2 个 Consumer**（同 group）— 观察分区分配与负载
4. **停止 1 个 Consumer** — 观察再均衡，剩余 Consumer 接管分区
5. **重启 Consumer** — 从已提交 offset 继续消费

### 8.3 作品集展示要点

| 主题 | 可讲述内容 |
|------|------------|
| 协议设计 | 帧格式、ApiKey 路由、CorrelationId 匹配 |
| 存储 | Append-Only、顺序写、Offset 语义 |
| 分布式 | 多 Broker 分区分布、Leader 路由 |
| Consumer Group | Join/Sync、Range 分配、Rebalance |
| 网络 | Netty EventLoop、编解码、长连接 |

---

## 9. V2 演进路线

| 特性 | 说明 |
|------|------|
| 分区副本 | Leader/Follower、ISR、副本同步 |
| Controller 选举 | 基于 Raft 或简化 Bully 算法 |
| Log Segment | 分段文件、稀疏索引、按 offset 快速定位 |
| 保留策略 | `retention.ms` / `retention.bytes` |
| 批量发送 | Producer batch、Fetch 多分区 |

---

## 10. 测试策略

| 层级 | 范围 |
|------|------|
| 单元测试 | 协议编解码、PartitionLog append/read、Range Assignor |
| 集成测试 | 单 Broker Producer/Fetch；Controller CreateTopic/Metadata |
| 端到端测试 | mq-demo 脚本：3 Broker + Produce + Consume + Rebalance |

---

## 11. 依赖版本

| 依赖 | 版本 |
|------|------|
| Java / Java 语言 | 17 |
| Maven / Maven 构建工具 | 3.8+ |
| Netty / Netty 网络框架 | 4.1.x |
| JUnit / JUnit 测试框架 | 5.x |
| SLF4J + Logback | 2.x / 1.4.x |
| Jackson | 2.x（元数据 JSON 序列化） |

---

## 12. 自审清单

- [x] 无 TBD/TODO 占位
- [x] 架构与 V1 scope（Producer + Consumer Group）一致
- [x] 多 Broker 与「无副本」表述一致
- [x] 协议、存储、Group 协调接口完整
- [x] Demo 场景可执行
- [x] V2 演进与 V1 边界清晰

