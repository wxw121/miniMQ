# MiniMQ V2.0 Storage Recovery Closeout Report / MiniMQ V2.0 存储恢复收口报告 - 2026-07-14

> Version / 版本：`2.0.0`
>
> Primary verification command / 主要验证命令：`mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml`
>
> Result / 结果：`BUILD SUCCESS`，耗时 6 分 54 秒。

## Executive Summary / 执行摘要

V2.0 closes the storage recovery foundation: independent WAL, checkpointed replay,
configurable flush policy, append-count checkpoint cadence, and broker shutdown
storage closure. The final full reactor verification completed successfully after
the broker and client coverage gaps were closed.

V2.0 完成了存储恢复基础能力的收口：独立 WAL、基于检查点的重放、可配置刷盘策略、按追加次数触发检查点，以及 Broker 关闭时的存储资源释放。在补齐 Broker 与客户端的覆盖率缺口后，最终完整 Reactor 验证成功完成。

| Metric / 指标 | Result / 结果 |
| --- | ---: |
| Full reactor test count / Reactor 测试总数 | 144 |
| Failures / 失败 | 0 |
| Errors / 错误 | 0 |
| Skipped / 跳过 | 0 |
| Weighted line coverage / 加权行覆盖率 | 2158 / 2348 = 91.91% |
| JaCoCo line coverage gate / JaCoCo 行覆盖率门禁 | 每个受检模块均不低于 90% |
| Full reactor verification / 完整 Reactor 验证 | `BUILD SUCCESS` |

## Full Reactor Verification / 完整 Reactor 验证

The final verification completed every module, including the downstream demo Web
end-to-end suite that was not reached when earlier coverage checks failed.

最终验证覆盖了所有模块，包括此前因覆盖率检查失败而未能运行到的下游 Demo Web 端到端测试套件。

| Module / 模块 | Tests / 测试数 | Line coverage / 行覆盖率 | Result / 结果 |
| --- | ---: | ---: | --- |
| `mq-common` | 10 | 134 / 137 = 97.81% | PASS / 通过 |
| `mq-storage` | 40 | 514 / 565 = 90.97% | PASS / 通过 |
| `mq-protocol` | 15 | 328 / 357 = 91.88% | PASS / 通过 |
| `mq-broker` | 34 | 572 / 619 = 92.41% | PASS / 通过 |
| `mq-client` | 24 | 317 / 352 = 90.06% | PASS / 通过 |
| `mq-demo` | 21 | 293 / 318 = 92.14% | PASS / 通过 |
| **Total / 总计** | **144** | **2158 / 2348 = 91.91%** | **BUILD SUCCESS** |

## Verified V2.0 Storage Scope / 已验证的 V2.0 存储范围

- `WriteAheadLogTest` (11): WAL encode/decode, checksum failure, truncated-tail
  recovery, unknown record rejection, validation, and defensive payload copies.
  覆盖 WAL 编解码、校验和失败、截断尾部恢复、未知记录拒绝、参数校验和防御性 payload 拷贝。
- `CheckpointStoreTest` (6): checkpoint save/load, missing, empty, and corrupt
  fallback, validation, partition keys, and temporary-file cleanup.
  覆盖检查点保存/加载、缺失/空文件/损坏回退、参数校验、分区键和临时文件清理。
- `LogManagerRecoveryTest` (5): replay without a checkpoint, behind-checkpoint
  replay, duplicate replay idempotence, and malformed checkpoint keys.
  覆盖无检查点重放、日志落后检查点时的重放、重复重放幂等性和畸形检查点键。
- `FlushPolicyTest` (5) and `LogManagerFlushPolicyTest` (3): all flush modes,
  invalid settings, checkpoint cadence, concurrent offset allocation, and close
  behavior.
  覆盖全部刷盘模式、非法配置、检查点节奏、并发 offset 分配和关闭行为。
- `FilePartitionLogTest` (10): partition-log corruption recovery, close behavior,
  and the idempotent append path used by replay.
  覆盖分区日志损坏恢复、关闭行为，以及重放使用的幂等追加路径。
- `BrokerConfigTest` (2): Broker flush and checkpoint CLI parsing and validation.
  覆盖 Broker 刷盘与检查点 CLI 参数的解析和校验。

## Closeout Findings / 收口结论

1. `mq-storage` passes its JaCoCo line gate at 514 / 565 = 90.97%.
   `mq-storage` 以 514 / 565 = 90.97% 通过 JaCoCo 行覆盖率门禁。
2. `LogManager.append` and `checkpoint` are serialized to prevent concurrent
   callers from allocating the same offset.
   `LogManager.append` 与 `checkpoint` 采用串行化处理，避免并发调用方分配相同 offset。
3. Broker shutdown closes `LogManager`, which checkpoints and closes the WAL and
   partition logs before rejecting further writes.
   Broker 关闭时会关闭 `LogManager`；它会先写检查点并关闭 WAL 与分区日志，再拒绝后续写入。
4. Broker metadata and handler tests plus client timeout, route-validation, and
   stale-cache recovery tests raise `mq-broker` to 92.41% and `mq-client` to
   90.06% without reducing the 90% gate.
   Broker 元数据与处理器测试，加上客户端超时、路由校验和陈旧缓存恢复测试，使 `mq-broker` 达到 92.41%、`mq-client` 达到 90.06%，且未降低 90% 门禁。
5. V2.0 intentionally keeps one WAL file and one partition file per
   topic-partition. Segment rolling, sparse indexes, retention, compaction, and
   zero-copy reads remain V2.1 and V2.2 work.
   V2.0 有意保持每个 Topic-Partition 一个 WAL 文件和一个分区文件。segment 滚动、稀疏索引、保留策略、压缩和零拷贝读取留待 V2.1 与 V2.2 完成。

## Milestone Readiness / 里程碑就绪状态

- The verified source tree is ready for the V2.0 storage recovery milestone
  commit and the `v2.0.0` tag.
  已验证的源码树可用于 V2.0 存储恢复里程碑提交和 `v2.0.0` 标签。
- The `v2.0.0` tag marks this verified release baseline.
  `v2.0.0` 标签标记了这一已验证发布基线。
- The next development baseline is V2.1 segmented logs, sparse indexes,
  retention, and compaction.
  下一开发基线是 V2.1 分段日志、稀疏索引、保留策略和压缩。
