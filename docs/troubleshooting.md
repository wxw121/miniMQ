# MiniMQ Troubleshooting / MiniMQ 故障排查文档

> This guide records common setup, build, test, runtime, and GitHub/SSH problems.
> 本文记录项目常见的环境、构建、测试、运行、GitHub/SSH 问题。

## 1. Maven Build / Maven 构建

### 1.1 Corporate Maven Mirror Fails / 公司 Maven 镜像不可用

Symptom / 现象：

```text
Could not transfer artifact ...
Connection timed out
```

Use the project-local Maven settings file.
使用项目内置 Maven settings 文件：

```powershell
mvn clean install -gs .mvn/settings.xml -s .mvn/settings.xml
mvn verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

Explanation / 解释：

`-s` selects the user settings file, while `-gs` selects the global settings file. In this project both point to `.mvn/settings.xml` so Maven uses Maven Central directly.
`-s` 指定用户 settings，`-gs` 指定全局 settings。本项目两个参数都指向 `.mvn/settings.xml`，让 Maven 直接使用 Maven Central。

### 1.2 Generated Build Output Appears / 出现构建产物

Generated files such as `target/` and `mq-demo/dependency-reduced-pom.xml` are ignored by `.gitignore`. Do not use them as source of truth.
`target/` 和 `mq-demo/dependency-reduced-pom.xml` 等生成文件已被 `.gitignore` 排除，不要把它们当作事实来源。

## 2. Tests And Coverage / 测试与覆盖率

### 2.1 Full Test Command / 完整测试命令

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml verify
```

Expected baseline / 预期基线：

| Metric / 指标 | Current baseline / 当前基线 |
| --- | ---: |
| Tests / 测试数 | 144 |
| Failures / 失败 | 0 |
| Errors / 错误 | 0 |
| Skipped / 跳过 | 0 |
| Overall line coverage / 总体行覆盖率 | 91.91% |

The latest V2.0 storage recovery closeout report is in [test-report-2026-07-10-v2-storage.md](test-report-2026-07-10-v2-storage.md).
最新 V2.0 存储恢复收口报告见 [test-report-2026-07-10-v2-storage.md](test-report-2026-07-10-v2-storage.md)。

### 2.2 Slow Integration Tests / 集成测试较慢

Some tests start real brokers, open Netty channels, and wait for consumer polling windows. This is expected.
部分测试会启动真实 Broker、打开 Netty 通道并等待 Consumer poll 窗口，这属于预期行为。

| Test / 测试 | Reason / 原因 |
| --- | --- |
| `MiniMqE2ETest` | Starts a three-broker cluster and two consumers. / 启动三节点 Broker 集群和两个 Consumer。 |
| `ProduceFetchIntegrationTest` | Verifies real broker produce/fetch routing. / 验证真实 Broker 生产与拉取路由。 |
| `ConsumerIntegrationTest` | Exercises subscribe, heartbeat, poll, and commit. / 覆盖订阅、心跳、poll 和提交。 |

## 3. Runtime Ports / 运行端口

The default demo ports are `9092`, `9093`, and `9094`. If a broker fails to start, check whether the port is already occupied.
默认演示端口为 `9092`、`9093`、`9094`。如果 Broker 启动失败，先检查端口是否被占用。

```powershell
netstat -ano | findstr ":9092"
netstat -ano | findstr ":9093"
netstat -ano | findstr ":9094"
```

If needed, start brokers with different `--port` values and update `--controller-addr`.
如有需要，可使用不同 `--port` 启动 Broker，并同步调整 `--controller-addr`。

## 4. Windows PowerShell Notes / Windows PowerShell 注意事项

Use PowerShell command examples on Windows. When a command contains many quotes or empty arguments, prefer the exact command from the documentation rather than converting from Bash.
Windows 下优先使用文档中的 PowerShell 示例。命令包含大量引号或空参数时，不建议直接从 Bash 写法硬转。

For Maven, this project consistently uses / Maven 命令统一使用：

```powershell
-gs .mvn/settings.xml -s .mvn/settings.xml
```

## 5. GitHub And SSH / GitHub 与 SSH

### 5.1 Avoid Using Company Git Identity / 避免使用公司 Git 身份

Use repository-local Git config instead of global config.
使用仓库级 Git 配置，不使用全局配置：

```powershell
git config --local user.name wxw121
git config --local user.email wxw121@users.noreply.github.com
```

Verify / 验证：

```powershell
git config --show-origin --get user.name
git config --show-origin --get user.email
```

The output should come from `.git/config`, not the global company config.
输出应来自 `.git/config`，而不是公司全局配置。

### 5.2 Use A Dedicated SSH Key / 使用专用 SSH Key

This repository can use a dedicated SSH key through repository-local `core.sshCommand`.
本仓库可通过仓库级 `core.sshCommand` 使用专用 SSH key：

```powershell
git config --local core.sshCommand "ssh -i C:/Users/bangsun/.ssh/id_ed25519_github_wxw121 -o IdentitiesOnly=yes"
```

Verify GitHub authentication / 验证 GitHub 认证：

```powershell
ssh -i C:\Users\bangsun\.ssh\id_ed25519_github_wxw121 -o IdentitiesOnly=yes -T git@github.com
```

Expected message / 预期信息：

```text
Hi wxw121! You've successfully authenticated, but GitHub does not provide shell access.
```

## 6. Documentation Maintenance / 文档维护

When changing protocol, storage, metadata, CLI, or test behavior, update the corresponding docs.
修改协议、存储、元数据、CLI 或测试行为时，应同步更新对应文档。

| Change area / 变更区域 | Update docs / 需更新文档 |
| --- | --- |
| Protocol fields / 协议字段 | [design.md](design.md), [testing.md](testing.md) |
| Topic or routing / Topic 或路由 | [architecture.md](architecture.md), [design.md](design.md) |
| Test count or coverage / 测试数或覆盖率 | [testing.md](testing.md), [test-report-2026-07-09.md](test-report-2026-07-09.md) |
| CLI behavior / CLI 行为 | Root [README.md](../README.md), [docs/README.md](README.md) |
| Roadmap items / 路线图事项 | [roadmap.md](roadmap.md) |
