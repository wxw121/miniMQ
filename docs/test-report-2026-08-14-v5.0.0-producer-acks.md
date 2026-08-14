# MiniMQ 5.0.0 全量测试报告（Producer Acknowledgements，2026-08-14）

本文档记录在 `cursor/v5-producer-acks-15db` 上一次完整的 `mvn clean verify` 与 JaCoCo 核对。它接在 [test-report-2026-08-13-v4.0.0-timeout-and-wire.md](test-report-2026-08-13-v4.0.0-timeout-and-wire.md) 之后。

## 1. 范围与环境

| 项 | 值 |
| --- | --- |
| 仓库 HEAD（本报告验证时） | `2e742c0` `test: wait for async topic tombstone in embedded delete test` |
| 功能提交 | `bb2df6f` `feat: add V5 producer LEADER and ALL acknowledgements` |
| 基线 | `origin/main` @ `30d7ae0` |
| JDK | OpenJDK 21.0.10（`release` 仍声明 17；本环境无 JDK 17） |
| Maven | 3.8.7 + `.mvn/settings.xml` |
| 命令 | `mvn -gs .mvn/settings.xml -s .mvn/settings.xml clean verify` |
| 墙钟 | 22 分 07 秒（`BUILD SUCCESS` 2026-08-14 00:50:03 UTC） |
| Docker | **不可用**（本环境无 Docker） |
| 浏览器 | 本轮未重跑 Chromium 操作台；Web 以 `MiniMqWebAppTest`（40 tests）为准 |

未跑：`ControllerFailoverIT`、`LeaderEpochFailoverIT`、`EtcdControllerCoordinationStoreIT`、`EtcdClusterMetadataRepositoryIT`。这些仍依赖 Docker/etcd，不得记为通过。

## 2. 全量 `mvn clean verify` 结果

`BUILD SUCCESS`。各模块 Surefire 计数：

| 模块 | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| mq-common | 17 | 0 | 0 | 0 |
| mq-protocol | 59 | 0 | 0 | 0 |
| mq-storage | 170 | 0 | 0 | 0 |
| mq-broker | 348 | 0 | 0 | 0 |
| mq-client | 94 | 0 | 0 | 0 |
| mq-demo | 79 | 0 | 0 | 6 |
| **合计** | **767** | **0** | **0** | **6** |

6 个 skipped 全部是 `@DisabledOnOs(LINUX)` 的 Windows 脚本/所有权用例（`DemoCliTest` ×5、`WindowsNativeArgumentQuotingTest` ×1），不是回归。

对照 2026-08-13 timeout/wire 全量（754 tests）：本轮多 13 个测试，来自 `ProduceAcksTest`、produce 尾字段解码、`ReplicaStateManager` waiter，以及 `acks=ALL` 的 EmbeddedChannel 路径。

第一次 `mvn clean verify` 曾在 `deleteTopicWaitsForCleanupWithoutBlockingEventLoopOrReleasingPermit` 上失败：`DELETE_TOPIC` 在 mutation executor 上安装 tombstone，测试在 DISCOVER 返回后立即读取，存在竞态。测试改为等待 tombstone 后复跑通过。该竞态与 acks 逻辑无关。

## 3. JaCoCo（质量门：line ≥ 90%）

`jacoco:check` 全部通过。CSV 汇总：

| 模块 | Instruction | Branch | Line | Method | Class |
| --- | ---: | ---: | ---: | ---: | ---: |
| mq-common | 97.71% (1152/1179) | 93.29% (139/149) | **97.57%** (201/206) | 96.55% (28/29) | 100% (10/10) |
| mq-protocol | 94.79% (7805/8234) | 81.76% (399/488) | **94.72%** (1722/1818) | 93.66% (251/268) | 100% (109/109) |
| mq-storage | 90.93% (10583/11639) | 81.69% (1004/1229) | **90.10%** (2338/2595) | 97.57% (401/411) | 98% (49/50) |
| mq-broker | 91.24% (23458/25709) | 77.00% (2079/2700) | **90.34%** (5227/5786) | 94.69% (838/885) | 100% (97/97) |
| mq-client | 90.87% (3424/3768) | 76.24% (276/362) | **90.27%** (807/894) | 93.75% (165/176) | 100% (27/27) |
| mq-demo | 95.06% (4912/5167) | 77.97% (269/345) | **93.12%** (975/1047) | 94.02% (173/184) | 100% (13/13) |
| **整体** | **92.17%** (51334/55696) | **79.01%** (4166/5273) | **91.28%** (11270/12346) | **95.03%** (1856/1953) | **99.67%** (305/306) |

六个模块 line 均 ≥ 90%。质量门未下调。

## 4. 本轮行为回归证据

| 项 | 测试 / 文档 |
| --- | --- |
| `LEADER` / `ALL` 码与超时边界 | `ProduceAcksTest` |
| V4 produce body 缺尾字段时默认 `LEADER` + 5000 ms | `ProtocolSerdesTest.decodeLegacyProduceWithoutAcksDefaultsToLeader` |
| 仅有 `acks`、缺 timeout 时用默认超时 | `decodeProduceWithAcksOnlyUsesDefaultTimeout` |
| 非法 `acks=0` 拒绝解码 | `decodeProduceRejectsUnsupportedAcks` |
| 选举后未上报 follower 多次 HW 计算仍阻塞 | `ReplicaStateManagerTest.repeatedHighWatermarkStillBlocksUnreportedFollowerAfterNewWrites` |
| waiter 立即成功 / 超时 / follower 追上 | `awaitReturnsImmediatelyWhenIsrAlreadyCaughtUp` 等 |
| 单 Broker ISR 上 `ALL` 立即成功 | `produceAcksAllSucceedsOnSingleBrokerIsr` |
| RF=2 follower 落后时 `PRODUCE`/`PRODUCE_V2` 超时且不回滚 | `produceAcksAllTimesOutWhenFollowerStaysBehindAndKeepsLocalAppend` |
| 空 ISR 返回 `NOT_ENOUGH_REPLICAS` 且保留本地追加 | `produceAcksAllReturnsNotEnoughReplicasWhenIsrIsEmpty` |
| follower 追上后 `ALL` 成功 | `produceAcksAllSucceedsWhenFollowerCatchesUp` |
| CLI `--acks leader\|all` | `DemoCliTest.parsesCliHelperValues` |

## 5. 已知缺口

1. Docker/etcd Failsafe 集成测试未跑，不得记为通过。
2. 本轮未重跑 Chromium 操作台；Web 路径以 `MiniMqWebAppTest` 为准。
3. `acks=ALL` 等待仍在请求处理线程上同步阻塞。设计要求下一步改到专用 executor，避免占满 Netty event loop。
4. 质量门只卡 line ≥ 90%。broker branch 77.00%、client branch 76.24% 仍低于 80%。

## 6. 结论

- 全量 `mvn clean verify` **通过**（767 tests / 0 fail / 6 skip）。
- 六个模块 JaCoCo line **均 ≥ 90%**，整体 91.28%。
- V5.0 Producer `LEADER` / `ALL` 确认已落地：可选尾字段、ISR 等待、超时不回滚、客户端/CLI 默认 `LEADER`。
- Docker/etcd failover **未验证**。
