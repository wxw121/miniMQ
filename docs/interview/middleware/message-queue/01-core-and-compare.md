# 核心原理与 Kafka / RocketMQ 对比

## 八个先说结论

1. **顺序是局部的。** Kafka 默认只保证单 partition 内顺序；RocketMQ 的顺序消费也必须围绕同一 sharding key / queue 设计。全局顺序会牺牲并行度。
2. **Consumer group 决定“广播还是分摊”。** 同组实例分摊分区/队列；不同组各自消费一份。消费者数超过可分配分区/队列时，部分实例会闲置。
3. **可靠投递靠端到端设计。** Producer ack、Broker 副本、Consumer 提交位点和业务幂等缺一不可。
4. **至少一次是常态。** 处理成功但提交 offset 前宕机，重启后仍会重投；应使用业务唯一键、去重表、状态机或幂等更新。
5. **重试不是无限重放。** 必须设定次数、退避、隔离重试 Topic/队列、DLQ、告警和人工补偿流程。
6. **积压先算吞吐，再扩容。** `积压时间 ≈ backlog / (消费吞吐 - 生产吞吐)`；当消费吞吐不大于生产吞吐时，扩容/分区和限流必须同时考虑。
7. **Key 既是顺序键也是热点风险。** 用订单号保证订单内顺序会造成大客户/热点订单集中到一个 partition；要把有序范围控制在业务必要粒度。
8. **“exactly once”需要限定范围。** Kafka 的 producer idempotence 解决单 producer session 的重试重复；事务可把输出记录与消费 offset 原子化。外部 DB/HTTP 副作用仍需要 outbox、幂等和补偿。

## Kafka：面试必须讲清的链路

### Topic、partition、replica 与 ISR

消息被追加到 Topic 的某个 partition；相同 key 可以映射到同一 partition，因此该 partition 内有写入顺序。每个 partition 有 Leader 和 follower replicas；生产/消费通常访问 Leader。`acks=all` 与 `min.insync.replicas` 的意义是限制允许确认写入时的 ISR 数量，避免只剩少量副本仍误报强可靠。

Kafka 官方文档说明：顺序保证是 topic-partition 级别；同组内一个 partition 在任一时刻只分给一个 consumer。参考 [Kafka Introduction](https://kafka.apache.org/documentation/) 与 [Kafka Design](https://kafka.apache.org/42/design/design/)。

### Producer：batch、linger、retry、幂等与事务

Producer 会按 partition 聚合 batch，在 `linger.ms` / batch size 条件下发送；这降低小消息开销，但增加可控等待。开启 idempotence 后，Broker 基于 producer identity 和 sequence 去重重试写入；它并不能去重应用自己重复发出的两次业务请求。Kafka transaction 进一步把多个 partition 的写入与消费 offset 提交做原子组合；消费端需采用 read-committed 才能获得相应可见性语义。

**追问回答：** “我不会把 Kafka EOS 延伸到数据库。跨 Kafka 和 MySQL 时，我优先采用本地事务写业务数据与 outbox，再可靠投递；消费者仍按幂等处理。”

### Consumer group、offset 与 rebalance

Consumer 自己拉取数据并提交 offset。实例加入/退出、订阅变化或分区变化会触发 assignment/rebalance；业务要关注 rebalance 窗口、长时间处理、offset 提交时机和重复消费。一个 group 内 consumer 数多于 partition 数不能提高并行度。

## RocketMQ：面试必须讲清的链路

### 消息模型与消费

RocketMQ 常用 Topic 下的 MessageQueue 作为并行单元；consumer group 以集群消费或广播消费组织。普通消息通常并发消费；顺序消息要让同一业务 sharding key 路由到同一 queue，并选择顺序消费模型。不要把“发送顺序”误说成“多个并发消费者下的全局业务顺序”。

### Retry 与 DLQ

消费失败、抛异常或消费超时会进入重试路径；达到最大次数后进入死信队列，供排查、修复和补偿。RocketMQ 官方文档明确描述 retry 状态机和 DLQ，且 4.x 文档说明广播模式不提供同样的 retry 行为。参考 [Consumption Retry](https://rocketmq.apache.org/docs/featureBehavior/10consumerretrypolicy/) 与 [Push Consumer](https://rocketmq.apache.org/docs/4.x/consumer/02push/)。

### 事务消息：解决“半消息”问题，不取代业务幂等

面试回答应围绕两阶段：先发送半消息，再执行本地事务，随后 commit/rollback；Broker 在状态不确定时回查 Producer。它解决“本地事务成功但消息未发出”或相反的双写一致性窗口，但回查必须幂等，消费者仍必须幂等，外部副作用仍需补偿策略。

## Kafka 与 RocketMQ：面试级比较

| 维度 | Kafka | RocketMQ | 答题重点 |
| --- | --- | --- | --- |
| 并行单元 | partition | MessageQueue | 不要执着术语；都受并行单元数量限制。 |
| 典型定位 | 日志型事件流、流处理生态 | 业务消息、事务/顺序/延时等场景能力 | 结合团队已有生态和运维能力选型。 |
| 消费组织 | consumer group + partition assignment | consumer group + cluster/broadcast model | 同组分摊，不同组订阅。 |
| 顺序 | partition 内顺序 | 按 sharding key/queue 的局部顺序 | 明确顺序范围与热点代价。 |
| 重试 / 死信 | 通常用重试 Topic、延迟重试和业务治理组合 | 产品化 retry/DLQ 机制明显 | 都需要幂等、告警、回放策略。 |
| 强一致路径 | idempotent producer + transaction + read-committed | transaction message + broker 回查 | 都不能自动覆盖数据库等外部副作用。 |
| 积压治理 | 扩 partition/consumer、优化批量和下游 | 扩队列/consumer、调优消费和治理重试 | 先量化生产与消费速率。 |

## 选型话术

“我会先问业务是事件流/实时计算，还是交易链路中的业务解耦；再看团队已有集群、运维经验、生态和 SLA。Kafka 的分区日志与流处理生态适合事件流；RocketMQ 对业务消息的顺序、事务、延时和重试治理通常更直接。最终不以单项功能定胜负，而是以一致性边界、吞吐、可运维性和团队成本做权衡。”
