# MiniMQ 4.0.0 全量测试报告（2026-08-13）

> 更新的 Surefire / JaCoCo 数字见 [test-report-2026-08-13-v4.0.0-timeout-and-wire.md](test-report-2026-08-13-v4.0.0-timeout-and-wire.md)。本文件保留当时的 Web 浏览器验收记录。

本文档记录一次完整的 `mvn clean verify`、JaCoCo 覆盖率核对、Web 操作台浏览器验收，以及本轮 review 修复的回归证据。它补在 [testing.md](testing.md) 既有 2026-07-29 基线之后，不替代该基线。

## 1. 范围与环境

| 项 | 值 |
| --- | --- |
| 仓库 HEAD（本报告） | `133cb3d` `fix: restore surefire 3.2.5 after resources-plugin edit` |
| 基线对照 | `b6b83ab` `fix: merge v4 readiness review findings`（`main`） |
| JDK | OpenJDK 21.0.9（`release` 仍声明 17；本环境无 JDK 17） |
| Maven | 3.8.7 + `.mvn/settings.xml` |
| 命令 | `MAVEN_OPTS=-Xmx2g mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml` |
| 墙钟 | 约 21 分 33 秒（`BUILD SUCCESS` 2026-08-13 15:38:27） |
| Docker | **不可用**（`docker: command not found`） |
| 浏览器 | Chromium（Cloud Agent computer-use）+ `curl` 对照 |

未跑：`ControllerFailoverIT`、`LeaderEpochFailoverIT`、`EtcdControllerCoordinationStoreIT`、`EtcdClusterMetadataRepositoryIT`。这些仍依赖 Docker/etcd，不得记为通过。

## 2. 全量 `mvn clean verify` 结果

`BUILD SUCCESS`。各模块 Surefire 计数：

| 模块 | Suites | Tests | Failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: |
| mq-common | 4 | 15 | 0 | 0 | 0 |
| mq-protocol | 2 | 52 | 0 | 0 | 0 |
| mq-storage | 27 | 163 | 0 | 0 | 0 |
| mq-broker | 45 | 323 | 0 | 0 | 0 |
| mq-client | 12 | 90 | 0 | 0 | 0 |
| mq-demo | 8 | 75 | 0 | 0 | 6 |
| **合计** | **98** | **718** | **0** | **0** | **6** |

6 个 skipped 全部是 `@DisabledOnOs(LINUX)` 的 Windows 脚本/所有权用例（`DemoCliTest` ×5、`WindowsNativeArgumentQuotingTest` ×1），不是回归。

对照 2026-07-29 基线（712 tests / 0 fail / 6 skip）：本轮多 6 个测试，来自 review 修复与 `ReflectionNativeSupport` 覆盖。

## 3. JaCoCo（质量门：line ≥ 90%）

`jacoco:check` 全部通过。CSV 汇总：

| 模块 | Instruction | Branch | Line | Method | Class |
| --- | --- | ---: | ---: | ---: | ---: |
| mq-common | 97.92% | 92.96% | **98.40%** (185/188) | 100% | 100% (9/9) |
| mq-protocol | 94.97% | 81.70% | **94.92%** (1680/1770) | 94.27% | 100% (109/109) |
| mq-storage | 90.98% | 81.54% | **90.20%** (2291/2540) | 97.29% | 98% (49/50) |
| mq-broker | 91.21% | 76.51% | **90.30%** (5136/5688) | 94.65% | 100% (97/97) |
| mq-client | 90.90% | 75.56% | **90.56%** (787/869) | 94.15% | 100% (27/27) |
| mq-demo | 94.98% | 77.88% | **92.98%** (953/1025) | 93.89% | 100% (13/13) |
| **整体** | **92.18%** (50400/54676) | **78.64%** (4062/5165) | **91.32%** (11032/12080) | **95.10%** (1826/1920) | **99.67%** (304/305) |

相对 2026-07-29（整体 line 91.17%、broker class 96/96）：整体 line 升到 91.32%；broker class 变成 97/97，因为 `ReflectionNativeSupport` 被 `NettyTransportFactoryTest` 覆盖。

## 4. 本轮 review 修复的回归证据

| 缺陷 | 测试 / 验收 |
| --- | --- |
| AUTO subscribe 假 PASS | `AutoFlowRunnerTest.autoFlowDoesNotFallbackWhenSubscribeFailsForExistingTopic` |
| MetadataWatch 竞态 | `MetadataWatchServiceTest` 等待 `source.restarted` 与 `installer.installedRevision()` |
| ReplicaFetcher WARN 冲爆 Surefire | `mq-broker/src/test/resources/logback-test.xml`；broker 323 tests 全部产出 XML |
| `poll()` FETCH 超时抛异常 | `ConsumerIntegrationTest.pollTimesOutWhenNoMessagesAreAvailable`；`Consumer.poll` 剩余预算 < 1ms 时返回 empty |
| `inFlight` 在 flush 之后记账 | `BrokerRuntimeMetricsIntegrationTest`；`MqRequestHandler` 先 `recordCompletion` 再 `writeResponse` |
| Web `GET /` 在 Java 21 空 path 下 404 | `MiniMqWebApp.isIndexPath()`；`MiniMqWebAppTest` 覆盖 `""` / `null` |
| 1970 mtime 资源被 maven-resources-plugin 2.6 跳过 | 根 POM `maven-resources-plugin` 3.3.1 + `overwrite=true`；jar 内含 `web/index.html` |
| 编辑 resources 插件时误删 Surefire 3.2.5 | 根 POM `pluginManagement` 同时保留 Surefire 3.2.5；否则 reactor 回退 2.12.4，跑 0 个 JUnit 5 测试 |

## 5. 浏览器 / 现场控制台验收

启动：

```text
java -Dminimq.web.port=18082 -Dminimq.broker.port=19092 \
  -Dminimq.data.dir=.run/data-live -Dminimq.admin.token=web-dev-admin \
  -cp mq-demo/target/mq-demo-4.0.0.jar com.minimq.demo.web.MiniMqWebDevMain
```

| 检查 | 结果 |
| --- | --- |
| `GET /` HTML | HTTP 200，含 `MiniMQ Operator Console`、`app.js`、`styles.css` |
| `GET /styles.css` / `GET /app.js` | HTTP 200 |
| 配置 | `127.0.0.1:19092`，`embedded-web` |
| `POST /api/run-flow` `mode=real` | `ok=true`，`consumedValue=hello-minimq`，`真实 MiniMQ 主流程通过` |
| `POST /api/ops` `status` | broker 0 `ONLINE`，`web-orders/0` `READY` |
| `POST /api/ops` `list-topics` | `web-orders` |
| Chromium 操作台 | 标题、Real broker 模式、Run create/produce/consume **PASS**、Ops status **PASS**、List topics 显示 `web-orders` |

录像：`/opt/cursor/artifacts/minimq-operator-console-browser-test.mp4`。

未测：外部 broker 模式、etcd 集群操作、Windows `.cmd` 启动。这些仍以 `MiniMqWebAppTest` / `DemoCliTest` 为准。

## 6. 已知缺口（测试未覆盖或环境挡掉）

1. Docker/etcd 集成测试未跑。
2. [code-review-2026-08-13-v4.0.0-full-review.md](code-review-2026-08-13-v4.0.0-full-review.md) 中未修的 P1（R4-P1-04～08）与全部 P2。本轮测试证明当前代码在默认 standalone 路径上可发布，不证明那些缺陷不存在。
3. 质量门只卡 line ≥ 90%。broker branch 76.51%、client branch 75.56% 仍低于 80%。

## 7. 结论

- 全量 `mvn clean verify` **通过**（718 tests / 0 fail / 6 skip）。
- 六个模块 JaCoCo line **均 ≥ 90%**，整体 91.32%。
- Web 操作台在真实内嵌 broker 上，浏览器主流程与 Ops 查询 **通过**。
- Docker/etcd failover **未验证**；未修的存储/controller P1 见同期 code review。
