# 消息队列中间件面试专项

**覆盖：** Kafka、RocketMQ 与 MiniMQ 项目映射。  
**目标：** 3 年以上 Java 后端社招中，能从原理、产品差异、业务场景和项目证据四层回答问题。

## 20 分钟突击路线

1. 读 [核心原理与产品对比](01-core-and-compare.md) 的“八个结论”和对比表。
2. 读 [MiniMQ 映射](02-minimq-mapping.md)，明确哪些能以项目代码佐证、哪些只能作为产品知识回答。
3. 练 [场景题与问答](03-scenarios-and-qa.md) 中的订单、支付、积压、顺序、重复消费和故障恢复。

## 不同技术栈的侧重点

| 面试方线索 | 优先准备 |
| --- | --- |
| Kafka / 流式计算 | partition、consumer group、offset、rebalance、ISR、幂等生产、事务/EOS 的边界。 |
| RocketMQ / 交易链路 | Topic/MessageQueue、顺序消息、事务消息、retry、DLQ、broker 集群与消费模式。 |
| 高并发业务 | 削峰、积压、重试风暴、限流、幂等、最终一致性、可观测性。 |
| Java 基础深挖 | Producer batch、网络 I/O、线程模型、序列化、磁盘写入和资源释放。 |

## 统一表达边界

- 先回答通用机制，再说明 Kafka/RocketMQ 的产品差异，最后连接到 MiniMQ 的已实现路径。
- “至少一次”不是“业务绝不重复”；重复消费必须由业务幂等或去重兜底。
- Kafka 的幂等生产、事务与端到端 EOS 有明确前提；RocketMQ 的事务消息也不等于外部系统自动原子提交。
- MiniMQ 是教学型实现：有分区、副本、ISR、acks、retry/DLQ、延迟消息和简化组协调，但没有事务/EOS、unclean election 或完整 Kafka 协作式 rebalance。
- 不要把 MiniMQ 说成 Kafka/RocketMQ 的替代品；详见 [项目映射](02-minimq-mapping.md)。
