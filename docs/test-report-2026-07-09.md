# MiniMQ V1.3 Closeout Test Report / MiniMQ V1.3 收口测试报告 - 2026-07-09

> Version / 版本：`1.3.0`  
> Verification command / 验证命令：`mvn -o -Dmaven.repo.local=.m2\repository -pl mq-storage,mq-broker -am -Dtest=FilePartitionLogTest,MetadataStoreTest -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false test`  
> Result / 结果：PASS / 通过

## 1. Executive Summary / 执行摘要

This report records the V1.3 closeout verification after client resilience, metadata corruption recovery, and partition-log corruption recovery were completed.
本文记录 V1.3 收口验证结果，覆盖客户端韧性、元数据损坏恢复和分区日志损坏恢复完成后的状态。

| Metric / 指标 | Result / 结果 |
| --- | ---: |
| Project version / 项目版本 | `1.3.0` |
| Targeted tests run / 目标测试总数 | 16 |
| Failures / 失败数 | 0 |
| Errors / 错误数 | 0 |
| Skipped / 跳过数 | 0 |
| Targeted result / 目标验证结果 | PASS / 通过 |

## 2. Verified Scope / 验证范围

- `FilePartitionLogTest`: 9 tests covering append/read/reopen recovery, truncated tail header, truncated key payload, truncated value payload, invalid negative length, impossible payload size, non-monotonic offset, recovered `nextOffset`, and `LogManager` caching. / 9 个测试，覆盖追加/读取/重开恢复、截断尾部 header、截断 key payload、截断 value payload、非法负长度、异常 payload 大小、非单调 offset、恢复后的 `nextOffset` 与 `LogManager` 缓存。
- `MetadataStoreTest`: 7 tests covering corrupt JSON failure, empty metadata file failure, schema-incompatible metadata, topic key mismatch, explicit quarantine recovery, schema quarantine recovery, and safe-write reload without temp-file leftovers. / 7 个测试，覆盖损坏 JSON 失败、空元数据文件失败、结构不兼容元数据、Topic key 不匹配、显式隔离恢复、结构损坏隔离恢复，以及安全写入后重载且不残留临时文件。

## 3. V1.3 Closeout Notes / V1.3 收口说明

- V1.3 is ready to tag as `v1.3.0` after the release-state commit. / V1.3 在 release 状态提交后可打 `v1.3.0` tag。
- The release version is `1.3.0`; future development should move back to a snapshot version when V2 work begins. / 发布版本为 `1.3.0`；进入 V2 开发时应切回 snapshot 版本。
- Full `mvn verify` was not repeated in this closeout because the current environment required offline/local-repository validation. The targeted V1.3 recovery tests passed in the offline reactor. / 本次收口未重复完整 `mvn verify`，因为当前环境使用离线/本地仓库验证；V1.3 目标恢复测试已在离线 Reactor 中通过。

## 4. Next Baseline / 下一基线

The next recommended development baseline is V2.0 storage recovery foundation: independent WAL format, checkpoint recovery contract, and explicit flush policy.
下一条推荐开发基线是 V2.0 存储恢复基础：独立 WAL 格式、checkpoint 恢复契约和显式刷盘策略。
