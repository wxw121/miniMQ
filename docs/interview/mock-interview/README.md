# 3 年以上 Java 后端模拟面试包

目标：把已有知识材料练成**有结构、可追问、不过度承诺**的口头表达。每轮录音或找同伴提问，结束后使用 [复盘量表](04-self-review-scorecard.md)。

## 60 分钟模拟流程

| 时间 | 环节 | 目标 |
| --- | --- | --- |
| 0–5 分钟 | 自我介绍 + 项目概览 | 90 秒讲清 MiniMQ 的定位、职责与个人贡献 |
| 5–20 分钟 | 项目深挖 | 架构、投递语义、持久化、测试与取舍 |
| 20–38 分钟 | Java 与中间件 | 并发/JVM/网络 + MQ/Redis/MySQL |
| 38–55 分钟 | 分布式系统 | 一致性、幂等、可靠事件、治理与故障处置 |
| 55–60 分钟 | 反问 + 复盘 | 明确岗位匹配点与待补知识 |

## 演练规则

1. 每题先给结论，再讲“原理 → 落地 → 边界/验证”；普通题控制在 1–2 分钟。
2. 无项目实证时明确说“这是我会采用的方案”，不要说成已经在线上落地。
3. 被追问到不确定数据时，不编造数字；说清需要看哪些指标、日志或代码来验证。
4. 面试官可从题库任意下钻，不按固定顺序；每轮至少练 2 个项目追问。

## 练习入口

- [项目深挖](01-project-deep-dive.md)
- [Java 与中间件轮](02-java-and-middleware-round.md)
- [分布式系统轮](03-distributed-systems-round.md)
- [复盘量表](04-self-review-scorecard.md)

## 支撑资料

项目证据见 [Java 后端资料](../java-backend/README.md)，中间件见 [MQ](../middleware/message-queue/README.md)、[Redis](../middleware/redis/README.md)、[MySQL](../middleware/mysql/README.md)，分布式系统见 [专项资料](../distributed-systems/README.md)。
