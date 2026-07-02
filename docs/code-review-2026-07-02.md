# Code Review Fix Log / 代码审查修复记录 - 2026-07-02

## Scope / 范围

Reviewed the MiniMQ broker/client/protocol/storage/common modules for runtime bugs and readability issues.  
本次审查覆盖 MiniMQ 的 broker、client、protocol、storage、common 模块，重点关注运行时 bug 和可读性问题。

## Findings And Fixes / 发现与修复

| Area / 区域 | Finding / 问题 | Fix / 修复 |
| --- | --- | --- |
| Storage / 存储 | `FileChannel.write` was called once per buffer, which could persist partial log records. / 每个 buffer 只调用一次 `FileChannel.write`，可能落盘不完整日志记录。 | Added looped full-buffer writes in `FilePartitionLog` and synchronized `highWatermark()`. / 在 `FilePartitionLog` 中加入循环写满逻辑，并同步 `highWatermark()`。 |
| Protocol / 协议 | Frame, string, byte array, and collection lengths were not bounded or consistently validated. / Frame、字符串、字节数组、集合长度没有统一边界校验。 | Added max frame size, protocol version checks, readable-byte checks, and count/length limits in codec/serde code. / 在 codec/serde 代码中加入最大 frame、协议版本、可读字节数和数量/长度限制。 |
| Broker request handling / Broker 请求处理 | Request decoding happened before the handler's error path, so malformed requests closed the channel instead of returning a controlled error. / 请求解码发生在 handler 错误处理路径之前，畸形请求会关闭连接而不是返回可控错误。 | Moved decode into the guarded path and return `INVALID_REQUEST` where a correlation id can be recovered. / 把解码移入受保护路径，并在能恢复 correlation id 时返回 `INVALID_REQUEST`。 |
| Netty buffers / Netty buffer | Client handlers retained responses without handling queue overflow and manually released auto-released messages on unknown correlation ids. / 客户端 handler retain 响应但未处理队列溢出，并且对未知 correlation id 的自动释放消息手动 release。 | Removed double-release paths and release retained buffers when queues reject them. / 移除重复释放路径，并在队列拒绝时释放 retained buffer。 |
| Metadata concurrency / 元数据并发 | `ClusterController.snapshot()` returned the internal mutable metadata object. / `ClusterController.snapshot()` 返回了内部可变元数据对象。 | Snapshot now returns a deep copy; metadata mutations go through a synchronized update method. / 快照现在返回深拷贝，元数据修改通过同步更新方法完成。 |
| Consumer groups / 消费组 | Stale member eviction could remove the leader while keeping the old `leaderMemberId`. / 清理过期成员时可能移除 leader，但仍保留旧 `leaderMemberId`。 | Recompute leader when the stored leader is missing after eviction. / 过期清理后如果原 leader 缺失，则重新计算 leader。 |
| Topic validation / Topic 校验 | Topics with empty names or non-positive partition counts could be created. / 空名称或非正分区数的 Topic 可能被创建。 | Reject invalid create-topic requests and guard partition calculation against non-positive counts. / 拒绝非法 create-topic 请求，并保护非正分区数的分区计算。 |
| Assignment serde / Assignment 序列化 | Full assignments used a fixed 4096-byte buffer and decoders trusted counts blindly. / 完整 assignment 使用固定 4096 字节 buffer，decoder 盲目信任 count。 | Compute exact encoding size, keep output within the short-prefixed protocol limit, and validate decode bounds. / 精确计算编码大小，确保输出在 short 前缀协议限制内，并校验解码边界。 |
| Client error handling / 客户端错误处理 | Several client paths cast `body()` before checking response error codes. / 多个客户端路径在检查响应错误码前就强转 `body()`。 | Check `ensureSuccess` before body access and improve exception messages. / 在访问 body 前检查 `ensureSuccess`，并改进异常信息。 |
| Broker binding/config / Broker 绑定与配置 | Broker ignored configured host and always bound to `127.0.0.1`; CLI parsing silently ignored bad args. / Broker 忽略配置的 host，总是绑定 `127.0.0.1`；CLI 解析会静默忽略坏参数。 | Bind to `config.host()` and add basic argument validation. / 绑定到 `config.host()`，并加入基础参数校验。 |
| Tests/logging / 测试与日志 | Netty DEBUG logs dominated test output. / Netty DEBUG 日志淹没了测试输出。 | Added test logback config to keep test logs at WARN by default. / 添加测试用 logback 配置，默认将测试日志保持在 WARN。 |

## Verification / 验证

- Added targeted regression coverage for invalid topic partitions, malformed protocol payloads, and assignment encoding beyond 4096 bytes. / 为非法 Topic 分区、畸形协议 payload、超过 4096 字节的 assignment 编码补充定向回归测试。
- Ran `mvn -q test -gs .mvn/settings.xml -s .mvn/settings.xml` successfully after code fixes. / 代码修复后成功运行 `mvn -q test -gs .mvn/settings.xml -s .mvn/settings.xml`。
