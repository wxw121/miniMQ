# MiniMQ 与标准消息队列的映射

> 目的不是把 MiniMQ 伪装成 Kafka/RocketMQ，而是让项目经历成为理解中间件原理的可验证证据。

## 可以用项目直接佐证的能力

| 中间件概念 | MiniMQ 已实现的项目证据 | 面试说法 |
| --- | --- | --- |
| partition / key 路由 | `Murmur2Partitioner`、显式 partition、Leader 路由 | “我实现/阅读过按 key 到 partition 的路由，因此知道顺序只能承诺在该 partition 内。” |
| append-only log | WAL、checkpoint、segmented log、sparse index | “我能从恢复顺序、索引定位和 retention/compaction 解释日志存储。” |
| 副本与 ISR | replication、ISR、Leader LEO、clean failover | “副本确认、Leader 就绪和恢复截断必须一起看。” |
| ack 语义 | `acks=LEADER|ALL`、`min.insync.replicas` | “ALL 是当前 ISR 的确认条件，不等于数据库副作用的 exactly once。” |
| 元数据与 fencing | etcd Controller election、metadata revision CAS、Leader epoch | “选主后仍需要 fencing，防止旧身份继续写入。” |
| consumer group | join/sync/heartbeat/offset/LEAVE_GROUP | “组协调、assignment 和 offset 是独立于拉取数据面的控制链路。” |
| retry / DLQ / delayed | reliability links、PRODUCE_V2、deliverAt、visible watermark | “重试需要明确目标、延迟和可见性，不能只在 catch 中无限重发。” |
| 运营性 | ops status、group lag、audit、runtime metrics、CLI/Web | “只实现消息通路不够，积压、故障和资源状态要可观测。” |

参考：[MiniMQ 架构](../../../architecture.md)、[系统设计](../../../design.md)、[项目卡](../../java-backend/01-project-card.md)。

## 必须主动说出未实现的边界

| 标准产品能力 / 复杂度 | MiniMQ 状态 | 正确说法 |
| --- | --- | --- |
| Kafka transaction / EOS | 未实现 | “项目没有事务和 EOS；我能解释 Kafka 的机制，但不会把项目描述成已经具备。” |
| 完整 Kafka cooperative rebalance | 未实现 | “当前组协调是简化模型，有 session timeout 和离组，但没有完整协作式再均衡。” |
| unclean election | 明确不做 | “没有在线 ISR 时冻结分区，优先避免数据不一致。” |
| 真实 etcd Failsafe gate | 环境受限未完成 | “有确定性本地测试；Docker-gated real-etcd 验证仍需环境恢复后补齐。” |
| RocketMQ transaction message | 未实现 | “可用半消息、本地事务、回查解释产品原理，但不是项目已有功能。” |
| 大规模集群运维 | 非项目范围 | “这是教学型实现，性能和可运维结论需要限定测试环境。” |

## 推荐回答结构

**被问 Kafka ISR：** 先解释 Leader/Follower、LEO/HW 和 `acks=all`，再说 MiniMQ 的 ALL 等待当前 ISR HWM 追上 Leader LEO；最后说明项目不做 unclean election 和 EOS。

**被问重复消费：** 先说明 consumer 成功处理但未提交位点时会重投；再给出业务幂等方案（唯一业务键、去重表、状态机、条件更新）；最后说明 MiniMQ 有 offset 提交与 retry/DLQ，但没有事务型端到端保障。

**被问事务消息：** 区分 Kafka transaction 与 RocketMQ half message + check；说明二者都不消灭外部系统副作用的幂等/补偿需求；不把 MiniMQ 写成支持事务。
