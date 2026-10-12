# MiniMQ 测试指南

MiniMQ 使用 JUnit 5、Maven Surefire 和 JaCoCo。源码位于各模块的 `src/main/java`，测试位于对应的 `src/test/java`；`target/` 中的内容均为构建产物，不是项目事实来源。

## 运行验证

在仓库根目录运行完整验证：

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

该命令会编译所有 Maven 模块、运行测试、生成 JaCoCo 报告并执行覆盖率检查。每个模块的行覆盖率必须达到 90%。

只运行测试（不执行 `verify` 阶段的覆盖率门禁）：

```powershell
mvn test -gs .mvn/settings.xml -s .mvn/settings.xml
```

运行单个模块：

```powershell
mvn -pl mq-client test -gs .mvn/settings.xml -s .mvn/settings.xml
```

运行指定测试类：

```powershell
mvn -pl mq-demo -Dtest=DemoCliTest test -gs .mvn/settings.xml -s .mvn/settings.xml
```

## 覆盖率报告

`mvn verify` 后，每个模块的 HTML 报告位于：

```text
mq-common/target/site/jacoco/index.html
mq-storage/target/site/jacoco/index.html
mq-protocol/target/site/jacoco/index.html
mq-broker/target/site/jacoco/index.html
mq-client/target/site/jacoco/index.html
mq-demo/target/site/jacoco/index.html
```

## 测试层次

| 层次 | 关注点 |
| --- | --- |
| 单元测试 | 协议编解码、日志格式、分区、配置校验和错误处理。 |
| 集成测试 | Broker、客户端、复制、消费组、Topic 管理及故障恢复路径。 |
| CLI / Web 测试 | 命令解析、端到端演示与 Web 主流程。 |
| 环境依赖测试 | 需要 Docker 或 etcd 的场景；运行前先确认本机 Docker 可用。 |

## 变更要求

- 修改协议字段时，更新 serdes 测试和 [总体设计](design.md)。
- 修改 Topic、分区或路由行为时，更新集成测试和 [架构](architecture.md)。
- 修改 CLI、脚本或 Web 启动方式时，更新根 [README](../README.md) 和本文档。
- 提交前运行 `mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml`。
