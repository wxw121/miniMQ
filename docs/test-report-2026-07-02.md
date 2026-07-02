# MiniMQ Test Report / MiniMQ 测试报告 - 2026-07-02

## Summary / 摘要

- Command / 命令：`mvn -q test -gs .mvn/settings.xml -s .mvn/settings.xml`
- Result / 结果：PASS / 通过
- Total tests / 测试总数：51
- Failures / 失败数：0
- Errors / 错误数：0
- Skipped / 跳过数：0
- Total wall time observed / 观察到的总耗时：about 162 seconds / 约 162 秒
- Overall JaCoCo line coverage / JaCoCo 总体行覆盖率：90.42%
- Overall JaCoCo branch coverage / JaCoCo 总体分支覆盖率：74.35%

## New Unit Tests Added / 新增单元测试

| Module / 模块 | Test class / 测试类 | Added coverage / 新增覆盖点 |
| --- | --- | --- |
| `mq-common` | `Murmur2PartitionerTest` | Rejects zero and negative partition counts. / 拒绝 0 和负数分区数量。 |
| `mq-common` | `CommonModelTest` | Covers common records and `ErrorCode` mapping. / 覆盖公共 Record 模型和 `ErrorCode` 映射。 |
| `mq-common` | `RangeAssignorTest` | Covers assignment round trips, empty values, and protocol limit errors. / 覆盖 assignment 往返、空值和协议限制错误。 |
| `mq-protocol` | `ProtocolSerdesTest` | Rejects oversized frame length through the Netty decoder path. / 通过 Netty decoder 路径拒绝超大 frame 长度。 |
| `mq-protocol` | `ProtocolSerdesTest` | Covers all request/response API body round trips and encoder behavior. / 覆盖所有请求/响应 API body 往返和编码器行为。 |
| `mq-broker` | `EmbeddedBrokerChannelTest` | Returns `INVALID_REQUEST` with the original correlation id for a malformed request body. / 对畸形请求 body 返回带原始 correlation id 的 `INVALID_REQUEST`。 |
| `mq-broker` | `EmbeddedBrokerChannelTest` | Covers handler errors for non-controller, unknown topic, and unknown partition paths. / 覆盖非 Controller、未知 Topic、未知 Partition 的 handler 错误路径。 |
| `mq-broker` | `BrokerConfigTest` | Covers broker CLI config parsing and validation. / 覆盖 Broker CLI 配置解析与校验。 |
| `mq-client` | `ClientValidationTest` | Covers invalid bootstrap validation and closing an unconnected network client. / 覆盖非法 bootstrap 校验和未连接网络客户端关闭。 |
| `mq-demo` | `DemoCliTest` | Covers CLI usage, validation helpers, and create/produce/consume through the CLI. / 覆盖 CLI 用法、校验 helper，以及 CLI 创建/生产/消费流程。 |

## Test Results By Class / 按测试类统计结果

| Module / 模块 | Test class / 测试类 | Tests / 测试数 | Failures / 失败 | Errors / 错误 | Skipped / 跳过 | Time / 耗时 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `mq-common` | `RangeAssignorTest` | 6 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-common` | `Murmur2PartitionerTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-common` | `CommonModelTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-protocol` | `ProtocolSerdesTest` | 8 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-storage` | `FilePartitionLogTest` | 3 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `BrokerConfigTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `ClusterControllerTest` | 3 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `GroupCoordinatorTest` | 5 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `EmbeddedBrokerChannelTest` | 5 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `ProduceFetchIntegrationTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-broker` | `TcpSmokeTest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-client` | `ClientValidationTest` | 2 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-client` | `ConsumerIntegrationTest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-client` | `ProducerIntegrationTest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-demo` | `DemoCliTest` | 7 | 0 | 0 | 0 | see Surefire / 见 Surefire |
| `mq-demo` | `MiniMqE2ETest` | 1 | 0 | 0 | 0 | see Surefire / 见 Surefire |

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

| Module / 模块 | Instruction / 指令 | Branch / 分支 | Line / 行 | Method / 方法 | Class / 类 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mq-broker` | 92.05% | 71.43% | 90.63% | 97.75% | 100.00% |
| `mq-client` | 88.01% | 63.33% | 89.81% | 82.22% | 100.00% |
| `mq-common` | 96.96% | 84.38% | 97.81% | 100.00% | 100.00% |
| `mq-demo` | 88.43% | 80.65% | 79.27% | 64.29% | 100.00% |
| `mq-protocol` | 91.68% | 77.17% | 90.25% | 100.00% | 100.00% |
| `mq-storage` | 90.08% | 71.43% | 90.43% | 100.00% | 100.00% |
| **Overall / 总体** | **91.49%** | **74.35%** | **90.42%** | **93.85%** | **100.00%** |

## Notes / 备注

- The slowest tests are integration/E2E tests that start broker/client stacks and include network waits. / 最慢的测试是会启动 broker/client 栈并包含网络等待的集成或端到端测试。
- `TcpSmokeTest` still prints two `System.out` lines during the run; functional result is passing. / `TcpSmokeTest` 运行时仍会打印两行 `System.out`，但功能结果是通过的。
