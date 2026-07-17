# MiniMQ V2.3 Replication Test Report / V2.3 副本机制测试报告

## Environment / 环境

| Item / 项目 | Value / 值 |
| --- | --- |
| Date / 日期 | 2026-07-17 |
| OS | Linux 6.12.94+ x86_64 |
| JDK | OpenJDK 21 |
| Maven | 3.8.7 |
| Branch / 分支 | `dev/v2.3-replication` |
| Project version / 项目版本 | `2.3.0` |

## Implemented Scope / 实现范围

- Partition `replicaBrokerIds` / `isrBrokerIds` in cluster metadata
- `CreateTopicRequest.replicationFactor`
- API `REPLICA_FETCH` (10) and `MARK_BROKER_OFFLINE` (11)
- `ReplicaFetcher` background follower loop
- `ReplicaStateManager` replication-aware high watermark with election grace period
- Client `FETCH` and sendfile capped at replication HW
- `REPLICA_FETCH` returns leader log bytes for follower catch-up
- Static-controller leader failover via `MARK_BROKER_OFFLINE`

## Focused Test Results / 聚焦测试结果

Command / 命令：

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Dtest=ClusterControllerTest,ReplicaStateManagerTest,ReplicationIntegrationTest,FetchResponseWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Result / 结果：exit code 0, zero failures.

Key classes / 关键测试类：

- `ClusterControllerTest`
- `ReplicaStateManagerTest`
- `ReplicationIntegrationTest`
- `FetchResponseWriterTest`

## Full Reactor Verification / 全量 Reactor 验证

Command / 命令：

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Result / 结果：`BUILD SUCCESS`, all modules passed tests and JaCoCo line-coverage gate.

## Code Review Closeout / 代码审查收口

All P0 findings from `docs/code-review-2026-07-17-v2.3-replication.md` are fixed in `2.3.0`.  
`docs/code-review-2026-07-17-v2.3-replication.md` 中列出的全部 P0 问题已在 `2.3.0` 修复。

P1 items are tracked for `2.3.1-SNAPSHOT`.  
P1 问题记录在 `2.3.1-SNAPSHOT` 待办中。

## Release Tag / 发布标签

```text
v2.3.0-replication
```
