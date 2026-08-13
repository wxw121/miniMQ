# MiniMQ 4.0.0 全量测试报告（成员超时与协议兼容，2026-08-13）

本文档记录在 `cursor/group-timeout-and-verify-15db` 上一次完整的 `mvn clean verify` 与 JaCoCo 核对。它接在 [test-report-2026-08-13-v4.0.0-full-verification.md](test-report-2026-08-13-v4.0.0-full-verification.md) 之后，不替代该报告中的 Web 浏览器验收记录。

## 1. 范围与环境

| 项 | 值 |
| --- | --- |
| 仓库 HEAD（本报告验证时） | `b34c990` `feat: evict timed-out group members and decode legacy heartbeat/commit` |
| 基线对照 | `4f40fed` `test: cover concurrent broker client cache races`（`main`） |
| 更早全量报告 | `133cb3d` 见 [full-verification](test-report-2026-08-13-v4.0.0-full-verification.md)（718 tests） |
| JDK | OpenJDK 21.0.10（`release` 仍声明 17；本环境无 JDK 17） |
| Maven | 3.8.7 + `.mvn/settings.xml` |
| 命令 | `MAVEN_OPTS=-Xmx2g mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml` |
| 墙钟 | 22 分 04 秒（`BUILD SUCCESS` 2026-08-13 23:36:42） |
| Docker | **不可用**（`docker: command not found`） |
| 浏览器 | 本轮未重跑 Chromium 操作台；Web 以 `MiniMqWebAppTest`（40 tests）为准 |

未跑：`ControllerFailoverIT`、`LeaderEpochFailoverIT`、`EtcdControllerCoordinationStoreIT`、`EtcdClusterMetadataRepositoryIT`。这些仍依赖 Docker/etcd，不得记为通过。

## 2. 全量 `mvn clean verify` 结果

`BUILD SUCCESS`。各模块 Surefire 计数：

| 模块 | Suites | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: |
| mq-common | 4 | 15 | 0 | 0 | 0 |
| mq-protocol | 2 | 56 | 0 | 0 | 0 |
| mq-storage | 27 | 170 | 0 | 0 | 0 |
| mq-broker | 45 | 340 | 0 | 0 | 0 |
| mq-client | 13 | 94 | 0 | 0 | 0 |
| mq-demo | 8 | 79 | 0 | 0 | 6 |
| **合计** | **99** | **754** | **0** | **0** | **6** |

6 个 skipped 全部是 `@DisabledOnOs(LINUX)` 的 Windows 脚本/所有权用例（`DemoCliTest` ×5、`WindowsNativeArgumentQuotingTest` ×1），不是回归。

对照 2026-08-13 较早全量（718 tests / 98 suites）：本轮多 36 个测试，来自后续 `main` 上的 P2 修复、客户端缓存竞态覆盖，以及本 PR 新增的成员超时与旧协议解码用例。本 PR 直接新增：`GroupCoordinatorTest` 8 个、`ProtocolSerdesTest` 4 个。

## 3. JaCoCo（质量门：line ≥ 90%）

`jacoco:check` 全部通过。CSV 汇总：

| 模块 | Instruction | Branch | Line | Method | Class |
| --- | ---: | ---: | ---: | ---: | ---: |
| mq-common | 97.92% (1084/1107) | 92.96% (132/142) | **98.40%** (185/188) | 100% (23/23) | 100% (9/9) |
| mq-protocol | 94.73% (7707/8136) | 81.67% (392/480) | **94.65%** (1698/1794) | 93.61% (249/266) | 100% (109/109) |
| mq-storage | 90.93% (10583/11639) | 81.69% (1004/1229) | **90.10%** (2338/2595) | 97.57% (401/411) | 98% (49/50) |
| mq-broker | 91.26% (23327/25561) | 76.90% (2064/2684) | **90.35%** (5188/5742) | 94.68% (836/883) | 100% (97/97) |
| mq-client | 90.97% (3404/3742) | 76.24% (276/362) | **90.33%** (803/889) | 94.22% (163/173) | 100% (27/27) |
| mq-demo | 95.03% (4879/5134) | 77.78% (266/342) | **93.08%** (968/1040) | 93.99% (172/183) | 100% (13/13) |
| **整体** | **92.16%** (50984/55319) | **78.91%** (4134/5239) | **91.28%** (11180/12248) | **95.10%** (1844/1939) | **99.67%** (304/305) |

六个模块 line 均 ≥ 90%。质量门未下调。

## 4. 本轮行为回归证据

| 项 | 测试 / 文档 |
| --- | --- |
| 成员超过 10s 未心跳被踢出 | `GroupCoordinatorTest`：自超时 → `GROUP_NOT_FOUND`；同伴超时 → `REBALANCE_IN_PROGRESS`；join 不双加 generation；commit 刷新心跳；sync 返回空分配 |
| 成功 commit 算一次心跳 | `successfulOffsetCommitRefreshesHeartbeat` |
| 再均衡期间仍刷新存活成员 | `heartbeatRefreshKeepsMemberAliveDuringRebalance` |
| 旧 HEARTBEAT / OFFSET_COMMIT body 可解码 | `ProtocolSerdesTest`：缺 `generationId` / `memberId` 时默认 `0` / `""`；当前编码仍写出新字段 |
| 线格式说明 | `docs/design.md` §2.5 / §7；`docs/architecture.md`；`docs/README.md` |

旧客户端解码不再因缺字段报畸形请求，但 join 之后 generation ≥ 1，省略 generation 的心跳/提交仍会被协调器拒绝。这是预期语义，不是解码失败。

## 5. 已知缺口

1. Docker/etcd Failsafe 集成测试未跑，不得记为通过。
2. 本轮未重跑 Chromium 操作台；Web 路径以 `MiniMqWebAppTest` 为准。
3. 质量门只卡 line ≥ 90%。broker branch 76.90%、client branch 76.24% 仍低于 80%。

## 6. 结论

- 全量 `mvn clean verify` **通过**（754 tests / 0 fail / 6 skip）。
- 六个模块 JaCoCo line **均 ≥ 90%**，整体 91.28%。
- 消费组成员 10 秒超时踢人与 HEARTBEAT / OFFSET_COMMIT 旧 body 解码已落地并有单测。
- Docker/etcd failover **未验证**。
