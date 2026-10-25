# Java 后端深挖问答：MiniMQ

> 回答顺序建议：先给结论（20–30 秒）→ 解释项目实现 → 主动说出边界。不要背诵与本人贡献无关的实现细节。

## 1. 并发、网络与资源管理

### Q1：为什么 `send(record)` 要做 auto-batch？会不会改变调用方语义？

**短答：** 单条小消息逐条网络往返会放大 syscall、编码和网络开销，因此 Producer 以分区为单位按 `batchSize=50`、`lingerMs=5` 暂存；`offset()`、`flush()`、`close()` 会把 pending batch 刷出。这样保留按分区顺序，同时减少 Produce 请求数。

**深挖：** `send().offset()` 仍会触发等待路径，因此不能把“异步入队”误说成“调用方已确认成功”。批量只合并同一分区记录，避免把不同分区的顺序语义混在一起。

**边界：** 这不是对 Kafka 客户端所有 batching 语义的完整复刻；当前参数和当前版本性能要以复测为准。

### Q2：Web runtime 为什么要复用？如何避免请求互相干扰？

**短答：** 每个 HTTP 请求新建 Producer/Consumer 会重复连接、入组和启动心跳，延迟很高。项目按 `bootstrapServers + topic + groupId` 缓存真实 runtime。

**深挖：** 同一 runtime 把 create-topic、produce、poll、commit 同步执行，避免并发请求“发送 A 却消费 B”；关闭或失败的 runtime 会从缓存移除并重建，缓存最多保留 4 个活跃实例，超限后异步关闭旧连接。

**边界：** 这把同一 key 的 Web 流程串行化，优先保证演示与语义隔离，不是高并发 HTTP 服务的最终吞吐方案。

### Q3：为什么用 Netty 和自定义二进制协议，而不是 HTTP/JSON？

**短答：** Broker 数据面需要更紧凑、可版本化的帧和较低编解码开销。项目将 frame、request/response header、API key 与 payload serde 放在协议模块，便于 Broker 与 SDK 共用。

**深挖：** 二进制协议需要明确 frame 长度、字段顺序、兼容策略和 malformed input 测试；项目保留 key 0–19 的原字段顺序，并对部分新增 trailing fields 做兼容解码。

**边界：** 自定义协议提高了可控性，也提高了演进成本；不能只凭“二进制”就承诺一定比 HTTP 快，性能仍要实测。

### Q4：背压和请求过载怎么处理？

**短答：** 项目有 in-flight throttling、请求节流、Controller request circuit breaking 和 `STORAGE_DEGRADED` 行为，避免无限堆积把单点故障扩散。

**深挖：** 面试中要把“协议级限流、客户端有界重试、存储不可用时快速失败”与 Netty 的 Channel writability 区分开：前者是项目已记录的运行韧性机制；后者若未亲自实现或验证，不应声称已形成完整的端到端背压闭环。

## 2. 存储、I/O 与恢复

### Q5：WAL、checkpoint 和分段日志各自解决什么？

**短答：** WAL 记录崩溃恢复需要的写入意图/过程，checkpoint 记录已恢复或已落盘的边界，分段日志限制单文件大小并支持独立索引、保留和压缩。

**深挖：** Broker 启动时先做 WAL replay 和 checkpoint 校验，再开放请求。读取时稀疏 offset index 缩小定位范围；sealed segment 才可进入 retention 或 compaction，读快照用于延迟物理删除。

**边界：** WAL replay 与刷盘策略一起讨论；不能把“有 WAL”直接等同于“任意故障下零丢失”。

### Q6：为什么默认每 100 次 append 刷盘，而不是每条 fsync？

**短答：** 每条同步 fsync 延迟和吞吐成本较高；默认 `FlushPolicy.everyNAppends(100)` 是在本地教学/吞吐场景下的折中，仍可显式设为 `always`。

**深挖：** 可靠性要分层说：Leader-local append、ISR 的复制确认、操作系统页缓存与物理介质持久化不是同一个时点。`acks=ALL` 解决副本确认条件，并不自动把所有本地 flush 策略变成同步落盘。

### Q7：为什么要分段日志和稀疏索引？

**短答：** 分段限制文件规模，让过期数据清理、压缩和恢复不必扫描整个分区；稀疏索引用较小元数据缩小按 offset 查找时的扫描范围。

**深挖：** retention 只删除 sealed segment，compaction 只重写 sealed segment 最新 key，读快照让正在进行的 fetch 持有稳定视图。

## 3. 可靠性、路由与故障转移

### Q8：`acks=LEADER` 和 `acks=ALL` 分别意味着什么？

**短答：** `LEADER` 在 Leader 本地追加后响应；`ALL` 等待当前 ISR 的高水位追上 post-append Leader LEO，且 `isr.size() >= min.insync.replicas`，否则超时或返回副本不足。

**深挖：** `ALL` 的等待交给专用 executor，避免阻塞网络处理线程；Leader/ISR 状态变化时等待必须能终止，不能对一个已失效的副本集合无限等待。

**边界：** 不支持 `acks=0`、事务或 EOS；不要把 `acks=ALL` 表述为端到端 exactly-once。

### Q9：Leader epoch fencing 解决什么问题？

**短答：** 解决旧 Controller 或旧 Leader 在选主后继续处理过期写入的问题。客户端路由携带 Leader epoch，Broker 通过最新 epoch 拒绝过期请求。

**深挖：** Controller 选举使用 etcd lease；共享元数据写入校验 Controller 身份和值以及 etcd revision。切换时先按 ISR 已确认 LEO 做恢复/截断，等 `LEADER_READY` 再接流量。

**边界：** V3/V4 Broker 混合集群不支持；没有在线 ISR 时冻结分区，不采用 unclean election。

### Q10：客户端如何在 Controller 或 Leader 变化后恢复？

**短答：** SDK 轮询完整 bootstrap 列表发现 Controller，携带路由时的 Leader epoch；遇到 `NOT_LEADER`、`BROKER_FENCED`、`STALE_LEADER_EPOCH` 或 `LEADER_NOT_READY` 时刷新一次路由。

**深挖：** 有界重试避免把网络或控制面故障放大为无限请求风暴；客户端不直接依赖 etcd，降低应用接入复杂度。

## 4. 消费语义与工程质量

### Q11：Consumer group 做到了什么，没做到什么？

**短答：** 支持 join、sync、heartbeat、offset commit/fetch、Range Assignment 和 per-member session timeout；`close()` 会发送 `LEAVE_GROUP`。

**深挖：** 组 leader 根据成员和分区计算分配；超时未心跳会触发新 generation。消费成功与 offset 提交是两个步骤，业务应按自身处理语义决定 commit 时机。

**边界：** 没有完整 Kafka 风格 rebalance protocol 或 cooperative assignment；不提供事务型消费-生产原子性。

### Q12：延迟消息为什么要有 visible watermark？

**短答：** 未来投递的记录仍可写入普通 partition log，但普通客户端 fetch 不应提前看到它。系统维护连续可见前缀和 `nextVisibleAtEpochMs`，Consumer 在自己的 poll 预算内等待并重试。

**深挖：** `FETCH_V2` 提供可见水位与下一条可见时间；Broker 不长时间挂起 fetch，客户端有界等待，这避免服务端连接被无限占用。

### Q13：如何证明项目质量，而不只报一个覆盖率？

**短答：** 看分层证据：协议 serde/malformed-input 测试、存储恢复测试、嵌入式 Broker/真实 Broker 集成测试、CLI/E2E 与 Web HTTP 测试；JaCoCo 只是一条门槛。

**深挖：** 2026-08-14 的历史全量验证为 823 tests、0 failures、6 skipped、总行覆盖 91.78%，且每模块 line ≥ 90%。测试报告必须带日期和 SHA `cdaa377`。

**边界：** Failsafe 为 0 tests，Docker-gated real-etcd 仍未验证；当前提交的数字应在面试前复跑。

### Q14：如果现在继续做，你会优先补什么？

**短答：** 我会优先关闭真实 etcd 集成验证环境、补当前版本性能基线，再评估复杂度较高的事务/EOS，而不是先堆新功能。

**理由：** 这两个缺口直接影响可靠性结论的可信度；事务会改变协议、日志、协调与恢复模型，需要独立设计和端到端验证，不能作为小功能插入。

## 回答自检

- 先说本人实际改过的类或链路，再讲全局架构。
- 每次提到吞吐、延迟、覆盖率或可用性，都说明日期、环境和验证范围。
- 被问到未知细节时，说明自己会从模块边界、测试和日志定位；不要补造实现。
- 参考：[架构](../../architecture.md)、[设计](../../design.md)、[测试](../../testing.md)。
