# MiniMQ

MiniMQ 是一个基于 Java 17 的 Kafka 风格分布式消息队列学习项目。它实现了自定义二进制协议、追加写日志、分区副本、etcd Controller 选举、Producer/Consumer SDK、消费组和运维 API，用于理解消息系统的核心机制；**不作为生产级 Kafka 替代品**。

## 特性

- Netty 上的自定义二进制协议与 Producer / Consumer SDK
- WAL 支撑的分段追加日志、保留策略、压缩、恢复与零拷贝拉取
- 3 Broker + etcd Controller 选举、ISR、Leader epoch fencing 和自动故障转移
- Consumer Group、Offset 提交、延迟消息、重试与死信队列关联
- `LEADER` / `ALL` 生产确认、`min.insync.replicas`、分区重分配和 preferred Leader
- Topic 管理、运行指标、故障转移审计，以及本地 Web 测试面板

## 前置条件

- JDK 17
- Apache Maven（项目未包含 Maven Wrapper）
- Docker Desktop 与 Docker Compose（仅运行 3 Broker + etcd 演示或 etcd 集成测试时需要）
- Windows PowerShell（使用仓库内的启动脚本时需要）

默认 Broker 端口为 `9092`、`9093`、`9094`，etcd 端口为 `2379`；启动前请确认这些端口未被占用。

## 快速开始

### 1. 构建并验证

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

该命令运行全部 Maven 测试，并执行每个模块至少 90% 行覆盖率的 JaCoCo 门禁。

### 2. 启动 3 Broker 演示集群

```powershell
docker compose -f deploy/etcd/docker-compose.yml up -d
.\scripts\start-brokers.ps1 -AdminToken <secret>
```

脚本会从根 `pom.xml` 读取当前版本并启动三个 Broker。`<secret>` 是本地演示使用的管理员令牌；请替换为自己的非空值。

### 3. 创建 Topic、生产并消费消息

```powershell
java -jar mq-demo/target/mq-demo-5.0.0.jar create-topic --brokers localhost:9092,localhost:9093,localhost:9094 --admin-token <secret> --name orders --partitions 3
java -jar mq-demo/target/mq-demo-5.0.0.jar produce --brokers localhost:9092,localhost:9093,localhost:9094 --topic orders --key user-1 --message hello --acks leader
java -jar mq-demo/target/mq-demo-5.0.0.jar consume --brokers localhost:9092,localhost:9093,localhost:9094 --topic orders --group order-processors
```

停止演示环境：

```powershell
.\scripts\stop-brokers.ps1 -CleanupRunDirectory
docker compose -f deploy/etcd/docker-compose.yml down
```

## Web 测试面板

本地开发启动器会在同一 JVM 中启动 MiniMQ Broker 与 Web 面板：

```powershell
.\scripts\start-web.cmd -AdminToken <secret>
```

浏览器访问 <http://127.0.0.1:18082/>。若已构建完成，可使用 `-SkipBuild`。后台窗口模式需先设置令牌：

```powershell
$env:MINIMQ_ADMIN_TOKEN = "<secret>"
.\scripts\start-web-window.cmd
```

停止 Web 环境：

```powershell
.\scripts\stop-web.cmd
```

## 项目结构

| 模块 | 职责 |
| --- | --- |
| `mq-common` | 公共模型、错误码、Murmur2 分区和 Range 分配 |
| `mq-storage` | 分区日志、WAL、索引和维护能力 |
| `mq-protocol` | 请求/响应模型与协议编解码 |
| `mq-broker` | Broker、Controller、复制和请求处理 |
| `mq-client` | Producer、Consumer 与网络客户端 |
| `mq-demo` | CLI、端到端示例与 Web 面板 |

## 文档

- [文档索引](docs/README.md)：推荐阅读顺序和项目快照
- [架构](docs/architecture.md)：模块边界、运行拓扑及数据/控制流
- [总体设计](docs/design.md)：协议、存储、元数据、消费组和兼容性约束
- [测试指南](docs/testing.md)：测试策略、覆盖率和验证命令
- [故障排查](docs/troubleshooting.md)：构建、端口、Windows 与 GitHub/SSH 问题

## 贡献

欢迎通过 Issue 或 Pull Request 参与。提交前请：

1. 使用 JDK 17。
2. 运行 `mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml`。
3. 修改协议、存储、CLI 或运行方式时，同步更新相应文档和测试。

## 许可证

本项目采用 [MIT License](LICENSE)。
