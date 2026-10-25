# Redis 面试资料包

适用：3 年以上 Java 后端社招。目标不是背命令，而是把每个方案说清楚：**业务目标 → 一致性/可用性取舍 → 具体实现 → 监控与降级 → 边界**。

## 20 分钟复习路线

1. 先看 [缓存一致性与热点治理](01-cache-consistency-and-hotkeys.md)，准备常规缓存题。
2. 再看 [锁与稳定性](02-distributed-lock-and-operations.md)，准备并发控制与线上治理题。
3. 最后用 [场景题与速答卡](03-scenarios-and-qa.md) 练习完整回答。

## 回答原则

- Redis 通常是加速层或协调工具，不替代订单、库存等核心事实的数据库事务。
- “最终一致”要说出失效窗口、补偿方式与可接受业务后果。
- 分布式锁只解决互斥的一部分问题；涉及扣款、扣库存时仍以数据库条件更新、幂等键或状态机兜底。
- 不臆造本项目 Redis 生产数据；项目实证见 [Java 后端项目卡](../../java-backend/01-project-card.md)，本资料用于展示可迁移的设计能力。

## 官方资料

- [Redis 分布式锁模式](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)
- [Redis 键淘汰策略](https://redis.io/docs/latest/develop/reference/eviction/)
- [Redis 持久化](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis Cluster 扩缩容与一致性](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)
