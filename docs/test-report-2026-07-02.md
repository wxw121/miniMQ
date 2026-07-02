# MiniMQ Test Report / MiniMQ 测试报告 - 2026-07-02

## Summary / 摘要

- Command / 命令：`mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml`
- Result / 结果：PASS / 通过
- Total tests / 测试总数：61
- Failures / 失败数：0
- Errors / 错误数：0
- Skipped / 跳过数：0
- Total wall time observed / 观察到的总耗时：about 157 seconds / 约 157 秒
- Overall JaCoCo line coverage / JaCoCo 总体行覆盖率：90.84%
- Overall JaCoCo branch coverage / JaCoCo 总体分支覆盖率：75.63%

## New Unit Tests Added / 新增单元测试

| Module / 模块 | Test class / 测试类 | Added coverage / 新增覆盖点 |
| --- | --- | --- |
| `mq-common` | `Murmur2PartitionerTest` | Rejects zero and negative partition counts. / 拒绝 0 和负数分区数量。 |
| `mq-common` | `CommonModelTest` | Covers common records and `ErrorCode` mapping. / 覆盖公共 Record 模型和 `ErrorCode` 映射。 |
| `mq-common` | `RangeAssignorTest` | Covers assignment round trips, empty values, and protocol limit errors. / 覆盖 assignment 往返、空值和协议限制错误。 |
| `mq-protocol` | `ProtocolSerdesTest` | Covers all request/response API body round trips, frame limits, encoder behavior, malformed collection counts, and oversized produce record list encoding. / 覆盖所有请求/响应 API body 往返、帧限制、编码器行为、畸形集合 count，以及超大 produce record 列表编码。 |
| `mq-broker` | `EmbeddedBrokerChannelTest` | Returns `INVALID_REQUEST` with the original correlation id for a malformed request body. / 对畸形请求 body 返回带原始 correlation id 的 `INVALID_REQUEST`。 |
| `mq-broker` | `EmbeddedBrokerChannelTest` | Covers handler errors for non-controller, unknown topic, and unknown partition paths. / 覆盖非 Controller、未知 Topic、未知 Partition 的 handler 错误路径。 |
| `mq-broker` | `BrokerConfigTest` | Covers broker CLI config parsing and validation. / 覆盖 Broker CLI 配置解析与校验。 |
| `mq-broker` | `BrokerShutdownTest` | Verifies broker stop/start cleanup by restarting on the same port. / 通过同一端口停止后重启验证 Broker 资源释放。 |
| `mq-client` | `ClientValidationTest` | Covers invalid bootstrap validation and closing an unconnected network client. / 覆盖非法 bootstrap 校验和未连接网络客户端关闭。 |
| `mq-client` | `NetworkClientRetryTest` | Covers fixed-backoff retry attempt budget and retry policy validation. / 覆盖固定退避重试次数预算和重试策略校验。 |
| `mq-demo` | `DemoCliTest` | Covers CLI usage, validation helpers, and create/produce/consume through the CLI. / 覆盖 CLI 用法、校验 helper，以及 CLI 创建/生产/消费流程。 |

## Test Results By Class / 按测试类统计结果

| Module / 模块 | Test class / 测试类 | Tests / 测试数 | Failures / 失败 | Errors / 错误 | Skipped / 跳过 | Time / 耗时 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `mq-common` | `RangeAssignorTest` | 6 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-common` | `Murmur2PartitionerTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-common` | `CommonModelTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-protocol` | `ProtocolSerdesTest` | 15 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-storage` | `FilePartitionLogTest` | 3 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `BrokerConfigTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `BrokerShutdownTest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `ClusterControllerTest` | 3 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `GroupCoordinatorTest` | 5 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `EmbeddedBrokerChannelTest` | 5 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `ProduceFetchIntegrationTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `TcpSmokeTest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-client` | `ClientValidationTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-client` | `ConsumerIntegrationTest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-client` | `NetworkClientRetryTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-client` | `ProducerIntegrationTest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-demo` | `DemoCliTest` | 7 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-demo` | `MiniMqE2ETest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |


## Key Flow Screenshots / 关键流程测试截图

These screenshots summarize the key test flows from the latest successful verification run. They are stored in `docs/assets/test-results` and should be regenerated when test counts, coverage, or major test flows change.

这些截图汇总最近一次成功验证中的关键测试流程。图片存放在 `docs/assets/test-results`，当测试数量、覆盖率或主要测试流程发生变化时需要重新生成。

### Full Maven Verify / 完整 Maven 验证

![Full Maven Verify / 完整 Maven 验证](assets/test-results/full-maven-verify.png)

### Protocol And Storage Unit Tests / 协议与存储单元测试

![Protocol And Storage Unit Tests / 协议与存储单元测试](assets/test-results/protocol-storage-unit.png)

### Broker And Client Integration Tests / Broker 与客户端集成测试

![Broker And Client Integration Tests / Broker 与客户端集成测试](assets/test-results/broker-client-integration.png)

### CLI And End-To-End Tests / CLI 与端到端测试

![CLI And End-To-End Tests / CLI 与端到端测试](assets/test-results/demo-e2e.png)

### Coverage Summary / 覆盖率汇总

![Coverage Summary / 覆盖率汇总](assets/test-results/coverage-summary.png)

## Generated Artifacts / 生成产物

Surefire reports are available under each module's `target/surefire-reports` directory.
Surefire 报告位于每个模块的 `target/surefire-reports` 目录，例如：

- `mq-common/target/surefire-reports`
- `mq-protocol/target/surefire-reports`
- `mq-storage/target/surefire-reports`
- `mq-broker/target/surefire-reports`
- `mq-client/target/surefire-reports`
- `mq-demo/target/surefire-reports`

JaCoCo coverage reports are available under each module's `target/site/jacoco` directory.
JaCoCo 覆盖率报告位于每个模块的 `target/site/jacoco` 目录，例如：

- `mq-common/target/site/jacoco/index.html`
- `mq-protocol/target/site/jacoco/index.html`
- `mq-storage/target/site/jacoco/index.html`
- `mq-broker/target/site/jacoco/index.html`
- `mq-client/target/site/jacoco/index.html`
- `mq-demo/target/site/jacoco/index.html`

## JaCoCo Coverage / JaCoCo 覆盖率

| Module / 模块 | Instruction / 指令 | Branch / 分支 | Line / 行 | Method / 方法 |
| --- | ---: | ---: | ---: | ---: |
| `mq-broker` | 92.56% | 72.08% | 90.29% | 97.75% |
| `mq-client` | 89.09% | 68.06% | 90.46% | 84.00% |
| `mq-common` | 96.96% | 84.38% | 97.81% | 100.00% |
| `mq-demo` | 88.43% | 80.65% | 79.27% | 64.29% |
| `mq-protocol` | 92.55% | 79.53% | 91.88% | 100.00% |
| `mq-storage` | 90.08% | 71.43% | 90.43% | 100.00% |
| **Overall / 总体** | **92.03%** | **75.63%** | **90.84%** | **93.98%** |

## Coverage Gate / 覆盖率门禁

JaCoCo `check` is bound to Maven `verify` with a 90% line coverage threshold. `BrokerMain` is excluded because it is a blocking process entrypoint whose behavior is indirectly covered through CLI and broker integration tests.
JaCoCo `check` 已绑定到 Maven `verify`，行覆盖率门槛为 90%。`BrokerMain` 是阻塞式进程入口，行为通过 CLI 和 Broker 集成测试间接覆盖，因此从门禁统计中排除。

## Notes / 备注

- The slowest tests are integration/E2E tests that start broker/client stacks and include network waits. / 最慢的测试是会启动 broker/client 栈并包含网络等待的集成或端到端测试。
- `TcpSmokeTest` no longer prints diagnostic `System.out` lines; it now asserts the response error code directly. / `TcpSmokeTest` 不再打印诊断性 `System.out` 行，而是直接断言响应错误码。