# 分布式系统面试资料包

适用：3 年以上 Java 后端社招。核心不是罗列 CAP、Saga、限流等名词，而是能说明**事实来源是什么、故障时允许牺牲什么、怎样收敛和观测**。

## 20 分钟复习路线

1. [一致性、幂等与分布式事务](01-consistency-idempotency-and-transaction.md)：先建立正确性主线。
2. [高可用、治理与可观测性](02-high-availability-and-governance.md)：再讲容量、保护和排障。
3. [场景题与速答卡](03-scenarios-and-qa.md)：练习端到端表达。

## 与 MiniMQ 的映射方式

MiniMQ 可以作为“如何理解持久化、确认、恢复和至少一次投递”的项目证据，详见 [项目卡](../java-backend/01-project-card.md) 与 [消息队列项目映射](../middleware/message-queue/02-minimq-mapping.md)。不要声称它已经实现跨服务事务、完整生产 SRE 体系或 Redis/MySQL 集群；这些内容应表达为迁移到业务系统时的设计方案。

## 官方/权威资料

- [Transactional Outbox 模式](https://microservices.io/patterns/data/transactional-outbox)
- [Saga 模式](https://microservices.io/patterns/data/saga.html)
- [Spring Cloud CircuitBreaker](https://docs.spring.io/spring-cloud-circuitbreaker/docs/current/reference/html/)
- [OpenTelemetry 信号模型](https://opentelemetry.io/docs/concepts/signals/)
- [Google SRE：级联故障](https://sre.google/sre-book/addressing-cascading-failures/)
