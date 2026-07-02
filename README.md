# MiniMQ

Kafka 风格的 Java 分布式消息队列（作品集 / 面试向）。

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

| ApiKey | 名称 |
|--------|------|
| 0 | CREATE_TOPIC |
| 1 | METADATA |
| 2 | PRODUCE |
| 3 | FETCH |
| 4-8 | Consumer Group / Offset |
| 9 | REGISTER_BROKER |

## V2 规划

- 分区副本 / ISR
- Controller 选举
- Log Segment 分段
