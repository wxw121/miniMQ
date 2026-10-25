# 一页项目卡：MiniMQ

> 用于项目开场、简历附件和技术一面准备。所有个人贡献均需替换为本人实际情况。

## 1. 项目定位

MiniMQ 是一个 **Java 17、多模块、Kafka 风格的教学型消息队列**。目标不是复刻生产级 Kafka，而是把 Java 后端中常见的网络通信、日志持久化、并发资源管理、主从副本、故障恢复、客户端 SDK 和可观测性串成一个可运行、可测试的系统。

它由 `mq-common`、`mq-protocol`、`mq-storage`、`mq-broker`、`mq-client` 和 `mq-demo` 六个 Maven 模块组成；使用 Netty 自定义二进制协议、WAL 支持的分段追加日志、etcd Controller 选举，以及 Producer/Consumer SDK。架构来源见 [架构文档](../../architecture.md)。

**本人贡献：** `[请按本人实际贡献填写：例如负责 mq-storage 的 WAL/恢复路径，或负责 mq-demo Web 运行时并发隔离。]`

## 2. 一句话架构

Producer 先通过 bootstrap Broker 获取分区 Leader 元数据；按 key 的 Murmur2 或显式 partition 路由到 Leader；Leader 批量追加到分段日志并按 `acks=LEADER|ALL` 返回。Consumer 通过 Controller 完成组协调和 offset 管理，再从分区 Leader 拉取可见消息。etcd 只服务 Broker/Controller 的选举与元数据一致性，SDK 不依赖 etcd。

```text
Producer / Consumer SDK
        │ Netty custom binary protocol
        ▼
Broker (Controller APIs / partition Leader)
        │
WAL + checkpoint + segmented append-only log
        │
etcd: Controller election + metadata CAS
```

## 3. 三个值得深挖的技术挑战

### 挑战 A：可靠写入与崩溃恢复

- **问题：** 只把消息追加到普通文件，宕机时无法可靠区分已落盘数据、未完成写入和恢复后的可见边界。
- **设计：** 存储层采用 WAL、checkpoint、分段日志与稀疏索引；恢复完成后才开放请求。默认 flush 策略为 `everyNAppends(100)`，可用 `always` 改为每次刷盘。
- **取舍：** 默认值偏吞吐而非每条消息同步 fsync；因此“写入成功”与最终物理持久化的时点要结合 acks、ISR 和 flush 策略解释，不能笼统承诺零数据丢失。
- **证据：** [系统设计的存储章节](../../design.md)；2026-08-14 全量报告覆盖 auto-batch 与默认 flush。

### 挑战 B：Leader 切换时防止旧节点写入

- **问题：** Controller 切换或旧 Leader 网络恢复后，旧身份若仍接受请求，会产生分叉尾部或过期元数据写入。
- **设计：** Broker 通过 etcd lease 竞选 Controller，元数据写入校验 Controller 身份与 revision；数据面携带精确 Leader epoch。切换后按 ISR 已确认 LEO 计算恢复点，完成崩溃安全截断和 `LEADER_READY` 后才开放读写。
- **取舍：** 不做 unclean election。没有在线 ISR 时分区保持 frozen，牺牲可用性换取避免不一致数据。
- **证据：** [故障转移架构约束](../../architecture.md)

### 挑战 C：Web 演示层不能为“方便演示”牺牲真实语义

- **问题：** 每次 HTTP 请求都新建 Producer/Consumer 会导致反复入组和秒级等待；并发点击还可能发生“发送 A、消费 B”的串消息。
- **设计：** Web 后端按 `bootstrapServers + topic + groupId` 复用真实 runtime；同一 runtime 内把 create-topic、produce、poll、commit 同步执行；失败 runtime 驱逐重建，并用最多 4 个活跃实例的 LRU 上限回收资源。
- **取舍：** 对已产生真实写入副作用的请求不再伪装成 mock 成功；AUTO 模式仅在尚未发生真实副作用时才 fallback。
- **证据：** [根 README 的 Web runtime 说明](../../../README.md) 与 `MiniMqWebAppTest`。

## 4. 可量化结果（必须带日期）

| 指标 | 可引用表述 | 来源 |
| --- | --- | --- |
| 模块边界 | 6 个 Maven 模块 | [根 README](../../../README.md) |
| 全量测试 | 823 tests，0 failures，6 skipped | 2026-08-14 `mvn clean verify`，SHA `cdaa377` |
| 覆盖率 | 总体 line coverage 91.78%（12273 / 13372）；每模块门槛 ≥ 90% | [2026-08-14 验证报告](../../test-report-2026-08-14-batch-flush-defaults.md) |
| 当前验证状态 | 该报告后的当前提交应在面试前复跑；real-etcd Failsafe 当时为 0 tests | [证据与性能页](05-evidence-and-performance.md) |

## 5. 主动交代的边界

- 这是教育型、单 Leader 分区模型，不是生产级 Kafka 替代品。
- 不支持事务与 exactly-once semantics（EOS）。
- 不支持 unclean election；没有在线 ISR 时分区冻结。
- Consumer group 是简化实现，不含完整 Kafka 式协作再均衡协议。
- real-etcd Failsafe 集成验证依赖 Docker，本地历史环境未跑通；应说“环境受限的验证缺口”，不能说“已经通过”。

## 6. 收尾句

“这个项目让我把 Java 后端里单点模块的知识串成了一条完整链路：从 Netty 请求进入，到分区日志持久化、Leader/ISR 可靠性控制，再到客户端重试、可观测性和可演示性；同时我会明确区分已验证能力与尚未覆盖的生产级语义。”
