# 简历描述与项目讲述稿

> 先替换所有 `[请按本人实际贡献填写]`，再放入简历或背诵。不要认领仓库无法证明的职责。

## 简历项目描述

### A. 保守版（适合贡献范围较聚焦）

**MiniMQ｜Java 17 消息队列实践项目**  
`[请按本人实际贡献填写]` 基于 Maven 多模块架构参与 Kafka 风格消息队列开发，涉及 Netty 二进制协议、WAL/分段日志、Broker/Client SDK 与 Web/CLI 演示；补充测试与文档，使 2026-08-14 全量验证达到 823 tests、整体行覆盖率 91.78%（历史报告）。

### B. 标准版（适合负责一个完整技术链路）

**MiniMQ｜Java 17 分布式消息队列**  
`[请按本人实际贡献填写]` 负责/参与消息写入、恢复或运行时链路：采用 Netty 自定义二进制协议与 WAL 支持的分段追加日志，结合 Leader epoch、ISR 与 `acks=LEADER|ALL` 控制可靠性；实现或维护真实 Web 主流程的资源复用、并发隔离和异常回收。项目按 6 个 Maven 模块拆分，并以全量测试和 JaCoCo 门槛作为质量证据。

### C. 强化版（仅在真实贡献覆盖相应内容时使用）

**MiniMQ｜Java 17 Kafka 风格消息队列**  
`[请按本人实际贡献填写]` 设计并实现/深度参与 Broker 可靠性链路：WAL/checkpoint 恢复、分段日志、分区副本 ISR、etcd Controller 选举与 Leader epoch fencing；通过 Web/CLI 展示真实生产消费、运维状态与受控 failover。以 2026-08-14 的 823 tests、91.78% line coverage 为历史验证基线，并明确事务/EOS 与 real-etcd 环境验证仍不在已完成范围内。

## 90 秒项目讲述

“我做的是 MiniMQ，一个 Java 17 的 Kafka 风格消息队列实践项目。它不是要替代 Kafka，而是把 Java 后端里的网络、存储、并发和可靠性完整串起来。项目按 common、protocol、storage、broker、client、demo 六个 Maven 模块拆分，协议层用 Netty 自定义二进制帧，存储层是 WAL 支持的分段追加日志。

我实际负责的是 `[请按本人实际贡献填写]`。我会重点讲一个链路：Producer 通过 bootstrap Broker 获取 Leader 元数据，按分区把消息发给 Leader；Leader 写入日志，并根据 `acks=LEADER` 或 `ALL` 决定何时响应。故障转移时通过 etcd Controller 选举和 Leader epoch 避免旧节点继续写入，没有在线 ISR 时宁可冻结分区也不做 unclean election。

工程上项目有 Web/CLI 演示和测试闭环。历史全量验证是 823 个测试、总体行覆盖率 91.78%。我也会明确说，这是一套教学型实现，事务/EOS 和真实 etcd 环境验证并没有宣称完成。”

## 3 分钟项目讲述

“MiniMQ 是一个 Java 17 的多模块消息队列项目，定位是学习和验证 Kafka 风格系统的核心机制，而不是生产级 Kafka 替代品。模块上把领域模型、协议、存储、Broker、SDK 和演示分开；运行上由 Broker 承载数据面和控制面，etcd 负责 Controller 选举和元数据一致性。

我实际负责的是 `[请按本人实际贡献填写]`。整个写入链路是：客户端先通过 bootstrap list 获取当前 Controller 和分区 Leader；Producer 按 key 的 Murmur2 或显式分区路由消息；Leader 追加到 WAL 支持的分段日志；`acks=LEADER` 表示本地追加确认，`acks=ALL` 还要等当前 ISR 高水位达到写入后的 LEO，并满足 `min.insync.replicas`。

第一个难点是可靠性。我不会把 WAL 说成万能的：它解决了恢复时已完成和未完成写入的识别，checkpoint 和分段索引帮助恢复与定位；默认 `everyNAppends(100)` 是吞吐和耐久性的取舍，不等价于每条都 fsync。

第二个难点是 failover。Controller 通过 etcd lease 选举，写元数据时校验 Controller 身份和 revision；数据面带 Leader epoch。切换后要按 ISR 确认的 LEO 进行截断，等 `LEADER_READY` 后才开放流量。没有在线 ISR 时项目选择冻结分区而非 unclean election，这是用可用性换数据一致性。

演示层也有真实工程问题：Web 后端复用 Producer/Consumer runtime，但同一个 runtime 内同步 create-topic、produce、poll、commit，避免并发请求串消息；失败后会驱逐 runtime，并且已发生真实写入时不会伪装成 mock 成功。

质量上，历史全量验证在 2026-08-14 的 `cdaa377` 是 823 tests、0 failures、91.78% line coverage。这个数字是历史基线，当前提交面试前需要复跑；Docker-gated real-etcd Failsafe 未执行，我会如实说明。”

## 5 分钟项目讲述

先按“3 分钟项目讲述”讲完，再补充下面两段。

**架构取舍：** “我把控制面和数据面拆开理解。Topic 元数据、组协调、offset、Broker 注册等属于 Controller 范围；produce/fetch 走对应分区 Leader。SDK 不直接依赖 etcd，而是通过完整 bootstrap list 发现 Controller。这样客户端依赖更轻，但要处理 metadata 刷新、过期路由和 Leader epoch。项目只做一次有界重试，不把复杂的重试风暴隐藏在客户端。”

**验证边界：** “项目有单测、真实 Broker 集成测试、CLI/E2E 测试和 Web HTTP 测试；2026-08-14 的完整验证记录为 823 tests、0 failures、6 skipped。真实性上，Docker 环境受限导致 real-etcd Failsafe 为 0 tests，因此我不会说 Controller HA 在真实 etcd 环境已完全验收。性能也只引用带机器与参数的历史基准，V5 当前数据需要复测。”

## 不要说什么

| 不要说 | 推荐说法 |
| --- | --- |
| “这是生产级 Kafka。” | “这是覆盖 Kafka 核心机制的教学型 Java 实现，生产化范围有明确边界。” |
| “真实 etcd 压测已经通过。” | “控制面有本地确定性测试；real-etcd Failsafe 受 Docker 环境限制尚未完成。” |
| “实现了精确一次 / EOS。” | “当前不支持事务和 exactly-once semantics，重点是 at-least-once 路径中的 offset 与可靠性控制。” |
| “我独立负责整个项目。” | “我实际负责的是 `[请按本人实际贡献填写]`，其余模块我能解释协作边界。” |
| “吞吐就是 X。” | “历史基准在指定主机与参数下是 X；当前版本面试前会复测。” |
