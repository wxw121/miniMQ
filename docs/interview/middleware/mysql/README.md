# MySQL 面试资料包

适用：3 年以上 Java 后端社招。回答时先说业务语义与数据规模，再落到索引、事务、锁或架构方案；不要脱离条件背“标准答案”。

## 20 分钟复习路线

1. [索引与 SQL 优化](01-index-and-sql.md)：设计索引和看执行计划。
2. [事务、锁与复制](02-transaction-lock-and-replication.md)：解释并发正确性和读写一致性。
3. [场景题与速答卡](03-scenarios-and-qa.md)：把知识组织成面试回答。

## 回答原则

- 数据库是订单、库存等核心事实的最终裁决者；缓存、消息和锁都应有数据库约束或幂等机制兜底。
- 不把“建索引”当万能解。索引会占用空间，并增加插入、更新、删除成本。
- 优化遵循“指标 → 慢 SQL / 执行计划 → 最小改动 → 回归验证”，而不是猜测或线上直接强制索引。
- 本项目 MiniMQ 的实证在协议、持久化、投递语义与测试，不虚构 MySQL 生产使用经验；可引用 [项目卡](../../java-backend/01-project-card.md) 后说明 MySQL 方案迁移思路。

## 官方资料

- [优化与索引](https://dev.mysql.com/doc/refman/8.0/en/optimization-indexes.html)
- [执行计划](https://dev.mysql.com/doc/refman/8.0/en/execution-plan-information.html)
- [InnoDB 事务隔离级别](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html)
- [InnoDB 锁](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html)
- [MySQL 复制实现](https://dev.mysql.com/doc/refman/8.0/en/replication-implementation.html)
