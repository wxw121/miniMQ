# MiniMQ

Kafka 风格的 Java 分布式消息队列。

## 文档入口

更完整的项目上下文请从 [docs/README.md](docs/README.md) 开始阅读。该索引会继续链接到架构、系统设计、测试、路线图、故障排查、测试报告和代码审查修复记录，适合新读者快速接手。

| 文档 | 用途 |
|------|------|
| [项目上下文索引](docs/README.md) | 推荐阅读顺序、模块地图、常用命令、术语表。 |
| [架构文档](docs/architecture.md) | 模块边界、运行拓扑、请求流、数据面/控制面、部署形态。 |
| [系统设计文档](docs/design.md) | 协议格式、序列化、存储、元数据、消费组、错误模型。 |
| [测试文档](docs/testing.md) | 测试策略、覆盖率、命令、报告位置、质量门槛。 |
| [故障排查文档](docs/troubleshooting.md) | Maven、端口、Windows、测试、GitHub/SSH 常见问题。 |
| [路线图](docs/roadmap.md) | 当前限制、V4 Operator Readiness 状态、后续功能规划、风险登记。 |

## 架构

- **Netty** 自研二进制协议
- **Append-Only Log** 持久化
- **3 Broker + etcd** Controller 自动选举、分区副本、Leader epoch fencing 与自动故障转移
- **Consumer Group** + Offset 提交
- **V4 Operator Readiness** 运维状态、Consumer lag、Topic admin、failover audit、retry/DLQ、delayed delivery
- **V5 Producer acks** `LEADER`（默认）或 `ALL`（等待当前 ISR）；`min.insync.replicas`；`LEAVE_GROUP`；topic 配置变更、分区重分配与 preferred Leader

## 模块

| 模块 | 说明 |
|------|------|
| `mq-common` | 公共模型、Murmur2 分区、Range 分配 |
| `mq-storage` | 分区日志存储 |
| `mq-protocol` | 协议编解码 |
| `mq-broker` | Broker 服务端 |
| `mq-client` | Producer / Consumer SDK |
| `mq-demo` | CLI 启动与演示 |

## 构建

```bash
# 若公司 Maven 镜像不可用，使用项目内 settings：
mvn clean install -gs .mvn/settings.xml -s .mvn/settings.xml
mvn test -gs .mvn/settings.xml -s .mvn/settings.xml
```

## 启动 3 Broker（Windows PowerShell）

```powershell
# 先启动 etcd
docker compose -f deploy/etcd/docker-compose.yml up -d

# Terminal 1
java -cp mq-demo/target/mq-demo-5.0.0.jar com.minimq.demo.BrokerMain --broker-id 0 --port 9092 --data-dir ./data/broker-0 --etcd-endpoints http://localhost:2379 --initialize-empty-cluster --admin-token <secret>

# Terminal 2
java -cp mq-demo/target/mq-demo-5.0.0.jar com.minimq.demo.BrokerMain --broker-id 1 --port 9093 --data-dir ./data/broker-1 --etcd-endpoints http://localhost:2379 --admin-token <secret>

# Terminal 3
java -cp mq-demo/target/mq-demo-5.0.0.jar com.minimq.demo.BrokerMain --broker-id 2 --port 9094 --data-dir ./data/broker-2 --etcd-endpoints http://localhost:2379 --admin-token <secret>
```

也可以让脚本从根 `pom.xml` 读取当前版本并启动三个 Broker（脚本不再使用旧的 `--controller` 参数）：

```powershell
.\scripts\start-brokers.ps1 -AdminToken <secret>
```

或使用 fat jar：

```bash
mvn -pl mq-demo package
java -jar mq-demo/target/mq-demo-5.0.0.jar broker --broker-id 0 --port 9092 --data-dir ./data/broker-0 --etcd-endpoints http://localhost:2379 --initialize-empty-cluster --admin-token <secret>
```

Delayed-message admission is bounded by --max-scheduled-records, which defaults to 100000. Setting it to 0 disables admission of new future-delayed records while preserving immediate records and ordinary fetch/produce behavior.

延迟消息准入由 --max-scheduled-records 控制，默认值是 100000；设置为 0 时会禁止新的 future delayed record 入队，但不影响即时消息和普通生产/消费路径。

## Demo 流程

```bash
# 1. 创建 Topic
java -jar mq-demo/target/mq-demo-5.0.0.jar create-topic --brokers localhost:9092,localhost:9093,localhost:9094 --admin-token <secret> --name orders --partitions 3 [--min-insync-replicas 1]

# 2. 发送消息
java -jar mq-demo/target/mq-demo-5.0.0.jar produce --brokers localhost:9092,localhost:9093,localhost:9094 --topic orders --key user-1 --message hello [--acks leader|all] [--ack-timeout-ms 5000]

# 3. 消费消息
java -jar mq-demo/target/mq-demo-5.0.0.jar consume --brokers localhost:9092,localhost:9093,localhost:9094 --topic orders --group order-processors [--session-timeout-ms 10000]
```

## Controller HA 与 Leader epoch（4.0.0）

Broker 启动后通过 `REGISTER_BROKER` 获取持久化 `brokerEpoch`，并使用 API 16 `BROKER_HEARTBEAT` 续租。运行状态按 `STARTING -> ACTIVE -> FENCED -> RECOVERING -> ACTIVE` 转换；非 `ACTIVE` Broker 会拒绝生产、消费、副本拉取和维护执行请求，并返回 `BROKER_FENCED`。

租约时间必须满足：

```text
broker heartbeat interval < broker self-fence timeout < controller broker lease timeout
```

对应启动参数为 `--broker-heartbeat-interval-ms`、`--broker-self-fence-timeout-ms` 和 `--controller-broker-lease-timeout-ms`。Controller 租约过期后会从在线 ISR 中按 LEO 选择替代 Leader；没有可选 ISR 时分区进入 frozen 状态并保留原 Leader，只有该原 Broker 使用更高 epoch 重新注册后才能恢复。

所有 Broker 通过 etcd lease 竞选 Controller；共享元数据写入同时校验 Controller 身份和值与 etcd revision，旧 Controller 无法提交。Producer 和 Consumer 可使用任意 Broker bootstrap 自动发现 Controller，并在 `NOT_LEADER`、`BROKER_FENCED`、`STALE_LEADER_EPOCH` 或 `LEADER_NOT_READY` 时刷新一次路由。

Leader 切换会先按旧 ISR 已确认 LEO 计算恢复点，持久化 Leader epoch 状态并崩溃安全地截断分叉尾部，完成 `LEADER_READY` 握手后才开放读写。4.0.0 仍不支持 unclean election；没有在线 ISR 时分区保持 frozen。SDK 不连接 etcd。

2.x 数据目录升级时，仅首个节点使用 `--initialize-cluster-from-local` 将本地元数据显式 CAS 导入空的 etcd 集群；全新集群使用 `--initialize-empty-cluster`。V3 客户端可继续使用 API 0-19，因为这些 wire layout 未改变；V3/V4 Broker 混合集群不受支持，所有 Broker 必须一起升级。回退前必须保留升级前备份。


## V4 Operator Readiness

当前工作树已经包含 V4 operator-readiness 协议与服务实现，不再只是 roadmap 或 design-only 规划。API 20-41 覆盖 OPS_STATUS、GROUP_STATUS、Topic list/describe/delete/increase、FAILOVER_AUDIT、AUTH_STATUS、CONFIGURE_TOPIC_RELIABILITY、admin create-topic、PRODUCE_V2、REPLICA_FETCH_V2、FETCH_V2、METADATA_V2、RUNTIME_METRICS_V2 与 replica-state 上报、`LEAVE_GROUP`、`ALTER_TOPIC_CONFIG`、`REASSIGN_PARTITIONS` 与 `ELECT_PREFERRED_LEADERS`。

Reliability links are explicit: operators configure retry and dead-letter topics through CONFIGURE_TOPIC_RELIABILITY; the client Producer resolves those links through METADATA_V2. sendToDeadLetter writes immediately with the normal produce path, while sendToRetry uses PRODUCE_V2 and an exact deliverAtEpochMs.

Delayed records are stored in the normal partition log and WAL. Future records are counted against the Broker-wide scheduled-record limit, WAL type 8 is SCHEDULED_DATA_APPEND, and recovery rebuilds scheduling state from checkpoints plus WAL replay.

Consumers use FETCH_V2 and the bounded client wait contract. When a Broker returns nextVisibleAtEpochMs, the client sleeps only up to the remaining poll timeout, then retries the same offset once it can become visible; Brokers do not hold fetch requests open. Legacy FETCH remains compatibility-only and is constrained to the contiguous visible prefix, so future delayed records are not exposed early.

Topic 名称必须为 1 到 249 个 `[A-Za-z0-9._-]` 字符，且不能是 `.` 或 `..`。删除按持久化的 `DELETING -> DELETED` 状态推进，默认最多等待 10 秒；超时时返回当前 `DELETING` 状态。已删除名称由 tombstone 永久保留，不能重新创建。

常用运维命令：

```powershell
java -jar mq-demo/target/mq-demo-5.0.0.jar ops-status --broker localhost:9092 --admin-token <secret>
java -jar mq-demo/target/mq-demo-5.0.0.jar group-status --brokers localhost:9092,localhost:9093,localhost:9094 --admin-token <secret> --group order-processors
java -jar mq-demo/target/mq-demo-5.0.0.jar list-topics --brokers localhost:9092,localhost:9093,localhost:9094 --admin-token <secret>
java -jar mq-demo/target/mq-demo-5.0.0.jar describe-topic --brokers localhost:9092,localhost:9093,localhost:9094 --admin-token <secret> --topic orders
java -jar mq-demo/target/mq-demo-5.0.0.jar runtime-metrics --broker localhost:9092 --admin-token <secret>
```

控制面命令接受完整 bootstrap 列表并自动发现当前 Controller；维护和 runtime metrics 等 Broker-local 命令只接受单个 `--broker`。`--admin-token` 拥有全部权限，也可用 `--operator-credential token=STATUS,TOPIC_ADMIN,MAINTENANCE,BROKER_OFFLINE` 配置最小权限凭据。

三节点脚本使用带 PID 与进程启动时间的 manifest，并只清理自己启动的进程。运行目录包含 ownership marker；`stop-brokers.ps1 -CleanupRunDirectory` 只会递归删除带有效 marker 的目录。

```powershell
.\scripts\start-brokers.ps1 -AdminToken <secret>
.\scripts\demo-v4-failover.ps1 -AdminToken <secret>
.\scripts\stop-brokers.ps1 -CleanupRunDirectory
```

## Web 前端主流程

MiniMQ 提供一个 Brutalism 风格的前端测试面板，用于从浏览器直接运行主流程：创建 Topic、生产消息、消费消息。前端支持两种运行路径：

- `REAL`：真实连接 MiniMQ Broker，执行 create-topic / produce / consume。
- `AUTO`：优先真实连接 Broker；只有在尚未发生真实副作用时，才会切换到 mock/fallback 演示流程，避免页面直接报错。若真实 `PRODUCE` 已经成功，或写入结果已不可确定，后续 poll/commit 失败会作为真实错误返回，不会伪装为 mock 成功。

推荐使用单进程本地开发启动器。它会在同一个 JVM 内启动 Broker 和 Web 前端，避免本地测试时分别管理两个后台进程：

```powershell
.\scripts\start-web.cmd
```

浏览器打开：`http://127.0.0.1:18082/`

该脚本默认使用：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| Web 端口 | `18082` | 浏览器访问端口。 |
| Broker 端口 | `19092` | Web API 真实消息流连接的 MiniMQ Broker 端口。 |
| 数据目录 | `.run/data-live` | 本地 Broker 数据目录。 |

如果已经完成打包，可以跳过构建：

```powershell
.\scripts\start-web.cmd -SkipBuild
```

如果希望当前命令结束后服务仍然常驻，使用独立窗口启动脚本。它会打开一个最小化终端窗口，窗口存在时 Broker 和 Web 会持续运行：

```powershell
.\scripts\start-web-window.cmd
```

停止本地 Web 服务：

```powershell
.\scripts\stop-web.cmd
```

如果希望直接运行 PowerShell 脚本，并且本机执行策略阻止 `.ps1` 文件，可以显式使用一次性绕过策略：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start-web.ps1 -SkipBuild
```

也可以直接运行开发启动器：

```powershell
java -Dminimq.web.port=18082 -Dminimq.broker.port=19092 -Dminimq.data.dir=.run/data-live -cp mq-demo/target/mq-demo-5.0.0.jar com.minimq.demo.web.MiniMqWebDevMain
```

前端自动化测试覆盖：静态页面资源、真实 MiniMQ 主流程、Broker 不可用时的 mock fallback、HTTP 错误处理、表单默认值与模式归一化。

### Web Runtime 复用与回收 / Web Runtime Reuse And Cleanup

Web 后端现在会按 `bootstrapServers + topic + groupId` 缓存真实主流程 runtime。这里的 runtime 指一次真实 Web 主流程需要复用的管理连接、Producer、Consumer 和消费组状态。这样做的原因是浏览器测试页面会反复点击“发送并消费”，如果每次都重新建连、重新加入消费组，用户会看到秒级等待，而真实消息队列的稳定连接路径本应更快。

The Web backend now caches real-flow runtimes by `bootstrapServers + topic + groupId`. A runtime means the admin connection, Producer, Consumer, and consumer-group state reused by one real Web flow. This exists because the browser panel sends repeated produce/consume requests; recreating connections and rejoining the consumer group on every click creates second-level latency, while a stable message-queue path should be much faster.

本次修复重点：

- 并发隔离 / Concurrency isolation：同一个 runtime 内把 create-topic、produce、poll、commit 包成一个同步执行单元，避免两个并发 HTTP 请求互相消费对方刚写入的消息。
- 失效 runtime 回收 / Failed-runtime cleanup：检测到已关闭的 runtime 时会先从缓存移除并重建后重试；其他真实流程异常会驱逐并关闭 runtime 后直接返回错误，避免在可能已经产生副作用时重复执行。AUTO 模式只有在尚未产生真实写入副作用时才会回退到 mock。
- 缓存上限 / Cache limit：runtime 缓存采用访问顺序 LRU，最多保留 4 个活跃 runtime；超限时先从缓存移除，再由后台清理线程关闭旧连接，避免资源回收阻塞当前 HTTP 请求。

如果不修复这些点，验收页面在并发点击时可能出现“发送 A 却消费到 B”的串消息；Broker 重启或连接异常后可能一直卡在旧 runtime；不同 topic/group 的测试越多，后台 Netty 连接和心跳线程也会无上限增长。

Without these fixes, concurrent clicks could send message A but consume message B, broker restarts or connection errors could leave the page stuck on a stale runtime, and repeated topic/group combinations could grow Netty connections and heartbeat threads without a bound.

## 协议 API

| ApiKey | 名称 / Name | 中文说明 |
|--------|-------------|----------|
| 0 | `CREATE_TOPIC` / 创建 Topic | 创建包含指定分区数的 Topic。 |
| 1 | `METADATA` / 元数据 | 拉取 Broker 列表和 Topic-Partition leader 元数据。 |
| 2 | `PRODUCE` / 生产消息 | 向指定 Topic-Partition 追加写入消息。 |
| 3 | `FETCH` / 拉取消息 | 从指定 offset 开始读取分区消息。 |
| 4-8 | Consumer Group / Offset / 消费组与位点 | 加入消费组、同步分区分配、心跳、提交 offset、拉取 offset。 |
| 9 | `REGISTER_BROKER` / 注册 Broker | Broker 向 Controller 注册自身地址与状态。 |

Additional V4 API keys:

- 20-21: OPS_STATUS and GROUP_STATUS for bounded operator status and Consumer lag snapshots.
- 22-28: Topic admin, failover audit, auth status, and retry/DLQ reliability-link configuration.
- 31: PRODUCE_V2 with deliverAtEpochMs for delayed/retry records.
- 32: REPLICA_FETCH_V2 with delivery timestamps and visibility watermarks.
- 34: FETCH_V2 with visible watermark and nextVisibleAtEpochMs; Consumer uses this path by default.
- 35-37: METADATA_V2, RUNTIME_METRICS_V2, and replica-state V2 reporting.
- 38-41: LEAVE_GROUP, ALTER_TOPIC_CONFIG, REASSIGN_PARTITIONS, and ELECT_PREFERRED_LEADERS.

## 已实现里程碑

- V2 系列：分区副本 / ISR、Controller failover、Log Segment、retention、compaction、zero-copy FETCH。
- V3 系列：etcd Controller HA、Leader epoch fencing、runtime metrics、resilience hardening、network transport selector。
- V4 当前实现：operator readiness、topic admin、failover audit、bounded metrics V2、retry/DLQ links、delayed messages、FETCH_V2 bounded wait。

