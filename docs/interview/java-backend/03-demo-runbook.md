# 现场演示与录屏运行手册

> 原则：演示“真实链路”时始终选择 `REAL`；`AUTO` 只用于体验页面，不可把 fallback mock 当作真实 Broker 成功。

## A. 面试前 10 分钟检查

| 检查项 | 命令 / 操作 | 通过标准 |
| --- | --- | --- |
| Java / Maven | `java -version`、`mvn -version` | 使用项目要求的 Java 17。 |
| 打包（首次） | `mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-demo -am package -DskipTests` | 生成 `mq-demo/target/mq-demo-5.0.0.jar`。 |
| Web 端口 | 浏览器确认 `http://127.0.0.1:18082/` 未被占用 | Web 默认端口可访问。 |
| failover 环境（可选） | `docker version` | 仅三 Broker + etcd 演示需要；失败时直接采用降级方案。 |

准备一个只用于本地演示的随机 token，例如在当前 PowerShell 会话中执行：

```powershell
$env:MINIMQ_ADMIN_TOKEN = 'interview-demo-local-only'
```

不要把真实凭据放到录屏、截图或 Git 历史中。

## B. 首选：90 秒单 Broker Web 真实主流程

### 启动

```powershell
.\scripts\start-web.cmd -AdminToken $env:MINIMQ_ADMIN_TOKEN
```

脚本会在同一 JVM 中启动本地 Broker（默认 `19092`）和 Web 前端（默认 `18082`）。浏览器打开：`http://127.0.0.1:18082/`。

### 页面操作与讲解词

| 步骤 | 页面操作 | 讲解词 | 预期观察点 |
| --- | --- | --- | --- |
| 1 | 将运行模式设为 **REAL**，填写唯一 Topic，如 `interview-orders-001`。 | “这里不是前端 mock：Web 后端会调用 MiniMQ 的真实 Admin、Producer 和 Consumer。” | 页面显示真实模式，不显示 fallback。 |
| 2 | 点击创建 / 运行主流程。 | “请求先进入 Java Web 层，再通过客户端 SDK 获取分区路由；消息写入对应 Leader。” | 返回 `mode=real`、topic、offset 与消费到的 value。 |
| 3 | 用不同 key 再发一条消息并消费。 | “Producer 可以按 key 的 Murmur2 或显式分区路由；客户端会刷新 metadata 处理过期路由。” | 新消息可被消费；不是页面内存回显。 |
| 4 | 打开 Ops / Runtime metrics 查询。 | “业务路径之外，Broker 还暴露受 token 保护的状态、分区、ISR 和运行时指标。” | 显示 broker/partition 状态或 metrics。 |
| 5 | 解释资源管理。 | “为了避免每次 HTTP 请求都新建连接和重新入组，Web runtime 按 bootstrap、topic、group 复用；同一 runtime 的 create/produce/poll/commit 串行化，避免并发串消息。” | 讲解即可；不需要临时制造并发。 |

### 停止

```powershell
.\scripts\stop-web.cmd
```

该脚本会结束本地 Web/Broker 端口对应进程。仅在本机演示结束后执行。

## C. 加分：三 Broker + etcd 受控 failover

这段建议作为二面或录屏素材，预计 2–3 分钟。它依赖 Docker，并会由既有脚本管理 etcd Compose、Broker 启停、目录 ownership marker 和清理流程。

```powershell
.\scripts\demo-v4-failover.ps1 -AdminToken $env:MINIMQ_ADMIN_TOKEN
```

脚本内部会：

1. 启动三个 Broker 与 etcd；等待三副本均进入 ISR。
2. 创建单分区、3 副本 Topic，生产并消费 `before-failover`。
3. 识别并停止当前 Leader。
4. 等待新的 Leader `READY`，检查 `frozen=false`。
5. 生产并消费 `after-failover`，查询 failover audit 中的 `LEADER_CHANGED` 事件。
6. 用 PID/start-time manifest 只清理自己启动的进程与 Compose 项目。

**讲解重点：** “这里的目标不是展示重启命令，而是验证旧 Leader 离开后，Controller 只从在线 ISR 选择替代 Leader；新的 Leader 完成恢复和 epoch 切换后，生产和拉取仍能恢复。若没有可用 ISR，系统宁可冻结分区也不做 unclean election。”

**成功信号：** 控制台出现 `Failover complete: broker-X -> broker-Y.` 和 `V4 failover demo proved produce and fetch recovery.`；Ops 状态中可看到新 `leaderId`、`readiness=READY`、`frozen=false`，审计中有 `eventType=LEADER_CHANGED`。

## D. 失败与降级方案

| 情况 | 不要做 | 可用降级 |
| --- | --- | --- |
| Docker 不可用 | 不要临场反复尝试启动 etcd，也不要声称 failover 已验证。 | 演示 B 的单 Broker **REAL** 主流程；播放事先录制的 C；出示 [验证证据](05-evidence-and-performance.md)。 |
| Broker 连接不上 | 不要切成 AUTO 后把 mock 成功当作 Broker 成功。 | 检查端口和 token；仍不可用则展示预录真实流程或测试报告。 |
| Web 端口被占用 | 不要随机结束不属于本次演示的进程。 | 先确认占用进程，或按 [故障排查](../../troubleshooting.md) 修改/释放端口。 |
| 时间不足 | 不要堆砌所有 API。 | 只做 B 的步骤 1–4，剩余内容用项目卡讲解。 |

## E. 录屏分镜（总计 3–4 分钟）

| 时间 | 画面 | 旁白 |
| ---: | --- | --- |
| 0:00–0:20 | 项目卡中的模块图 | “MiniMQ 用六个 Maven 模块把协议、存储、Broker、SDK 和演示分开。” |
| 0:20–1:25 | Web REAL 主流程 | “这一段证明浏览器不是 mock，而是经 SDK 到真实 Broker 的 create/produce/consume。” |
| 1:25–2:30 | 三节点 failover 控制台 | “停止 Leader 后，只从 ISR 提升新 Leader；epoch 和 READY 握手避免旧路由继续写。” |
| 2:30–3:10 | 测试报告 / 证据页 | “历史全量验证有 823 测试和 91.78% 行覆盖；real-etcd 环境验证仍是诚实保留项。” |

录制前用一次全新 Topic 名称；结束后确认运行脚本已清理其 own resources。更多启动参数见根 [README](../../../README.md) 与 [故障排查](../../troubleshooting.md)。

## F. 手动排障命令索引

若只排查三节点环境，可使用以下既有命令；正式录屏仍优先使用 `demo-v4-failover.ps1`。

```powershell
docker compose -f deploy/etcd/docker-compose.yml up -d
.\scripts\start-brokers.ps1 -AdminToken $env:MINIMQ_ADMIN_TOKEN
```
