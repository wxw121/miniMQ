# MiniMQ

Kafka 风格的 Java 分布式消息队列（作品集 / 面试向）。

## 文档入口

更完整的项目上下文请从 [docs/README.md](docs/README.md) 开始阅读。该索引会继续链接到架构、系统设计、测试、路线图、故障排查、测试报告和代码审查修复记录，适合新读者和新 agent 快速接手。

| 文档 | 用途 |
|------|------|
| [项目上下文索引](docs/README.md) | 推荐阅读顺序、模块地图、常用命令、术语表。 |
| [架构文档](docs/architecture.md) | 模块边界、运行拓扑、请求流、数据面/控制面、部署形态。 |
| [系统设计文档](docs/design.md) | 协议格式、序列化、存储、元数据、消费组、错误模型。 |
| [测试文档](docs/testing.md) | 测试策略、覆盖率、命令、报告位置、质量门槛。 |
| [故障排查文档](docs/troubleshooting.md) | Maven、端口、Windows、测试、GitHub/SSH 常见问题。 |
| [路线图](docs/roadmap.md) | 当前限制、近期代办、V2/V3 功能规划、风险登记。 |

## 架构

- **Netty** 自研二进制协议
- **Append-Only Log** 持久化
- **3 Broker** 分区分布（Broker-0 为 Controller）
- **Consumer Group** + Offset 提交

## 模块

| 模块 | 说明 |
|------|------|
| `mq-common` | 公共模型、Murmur2 分区、Range 分配 |
| `mq-storage` | 分区日志存储 |
| `mq-protocol` | 协议编解码 |
| `mq-broker` | Broker 服务端 |
| `mq-client` | Producer / Consumer SDK |
| `mq-demo` | CLI 启动与演示 |

## 构建

```bash
# 若公司 Maven 镜像不可用，使用项目内 settings：
mvn clean install -gs .mvn/settings.xml -s .mvn/settings.xml
mvn test -gs .mvn/settings.xml -s .mvn/settings.xml
```

## 启动 3 Broker（Windows PowerShell）

```powershell
# Terminal 1
java -cp mq-demo/target/mq-demo-1.0.0-SNAPSHOT.jar com.minimq.demo.BrokerMain --broker-id 0 --port 9092 --data-dir ./data/broker-0 --controller

# Terminal 2
java -cp mq-demo/target/mq-demo-1.0.0-SNAPSHOT.jar com.minimq.demo.BrokerMain --broker-id 1 --port 9093 --data-dir ./data/broker-1 --controller-addr localhost:9092

# Terminal 3
java -cp mq-demo/target/mq-demo-1.0.0-SNAPSHOT.jar com.minimq.demo.BrokerMain --broker-id 2 --port 9094 --data-dir ./data/broker-2 --controller-addr localhost:9092
```

或使用 fat jar：

```bash
mvn -pl mq-demo package
java -jar mq-demo/target/mq-demo-1.0.0-SNAPSHOT.jar broker --broker-id 0 --port 9092 --data-dir ./data/broker-0 --controller
```

## Demo 流程

```bash
# 1. 创建 Topic
java -jar mq-demo/target/mq-demo-1.0.0-SNAPSHOT.jar create-topic --brokers localhost:9092 --name orders --partitions 3

# 2. 发送消息
java -jar mq-demo/target/mq-demo-1.0.0-SNAPSHOT.jar produce --brokers localhost:9092 --topic orders --key user-1 --message hello

# 3. 消费消息
java -jar mq-demo/target/mq-demo-1.0.0-SNAPSHOT.jar consume --brokers localhost:9092 --topic orders --group order-processors
```

## 协议 API

| ApiKey | 名称 / Name | 中文说明 |
|--------|-------------|----------|
| 0 | `CREATE_TOPIC` / 创建 Topic | 创建包含指定分区数的 Topic。 |
| 1 | `METADATA` / 元数据 | 拉取 Broker 列表和 Topic-Partition leader 元数据。 |
| 2 | `PRODUCE` / 生产消息 | 向指定 Topic-Partition 追加写入消息。 |
| 3 | `FETCH` / 拉取消息 | 从指定 offset 开始读取分区消息。 |
| 4-8 | Consumer Group / Offset / 消费组与位点 | 加入消费组、同步分区分配、心跳、提交 offset、拉取 offset。 |
| 9 | `REGISTER_BROKER` / 注册 Broker | Broker 向 Controller 注册自身地址与状态。 |

## V2 规划

- 分区副本 / ISR
- Controller 选举
- Log Segment 分段
