# V5 Multi-Node Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use $superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking via update_plan.

**Goal:** Build operator-facing multi-node status and validation capabilities after V3.0.0.

**Architecture:** Add read-only authenticated admin surfaces first, then deterministic local validation, then Docker-gated real-etcd validation only when the environment supports it.

**Tech Stack:** Java 17, Maven, JUnit 5, Netty embedded channels, MiniMQ protocol records/serdes, existing demo CLI.

---

### Task 1: Broker Operations Snapshot

**Files:**
- Modify: `mq-protocol/src/main/java/com/minimq/protocol/ApiKeys.java`
- Modify: `mq-protocol/src/main/java/com/minimq/protocol/message/ProtocolMessages.java`
- Modify: `mq-protocol/src/main/java/com/minimq/protocol/ProtocolSerdes.java`
- Modify: `mq-broker/src/main/java/com/minimq/broker/handler/MqRequestHandler.java`
- Test: `mq-protocol/src/test/java/com/minimq/protocol/ProtocolSerdesTest.java`
- Test: `mq-broker/src/test/java/com/minimq/broker/EmbeddedBrokerChannelTest.java`

- [ ] Add a failing protocol round-trip test for a read-only operations snapshot API.
- [ ] Implement the minimal request/response records and serdes branch.
- [ ] Route the API through the existing admin token model.
- [ ] Verify with protocol and embedded-channel focused tests.

### Task 2: CLI Operations Status

**Files:**
- Modify: `mq-demo/src/main/java/com/minimq/demo/DemoCli.java`
- Test: `mq-demo/src/test/java/com/minimq/demo/DemoCliTest.java`

- [ ] Add a failing CLI test for `ops-status --broker host:port --admin-token <secret>`.
- [ ] Render availability state, controller role, broker epoch, metadata version, transport mode, runtime metrics, and maintenance status.
- [ ] Verify token redaction and unauthorized error handling.

### Task 3: Local Multi-Node Validation Harness

**Files:**
- Create or modify broker/demo test utilities under `src/test/java`.
- Test: focused broker or demo integration tests.

- [ ] Build a local non-Docker three-broker harness with bounded startup waits.
- [ ] Verify controller discovery, leader readiness, produce/fetch, and broker shutdown cleanup.
- [ ] Keep timeouts explicit and low enough for developer machines.

### Task 4: Docker-Gated Real-etcd Report

**Files:**
- Modify: `docs/testing.md`
- Create: a V5 test report under `docs/`

- [ ] Check Docker API access before running any `-Petcd-it` command.
- [ ] If Docker is unavailable, record the gate as environment-blocked after bounded attempts.
- [ ] If Docker is available, run the real-etcd Failsafe suite once and record output.

### Task 5: Documentation Sync

**Files:**
- Modify: `docs/README.md`
- Modify: `docs/design.md`
- Modify: `docs/testing.md`
- Modify: `docs/roadmap.md`

- [ ] Update API tables, CLI examples, and testing evidence from the actual diff.
- [ ] Keep V4 LSM documentation clearly marked as optional experimental work.
- [ ] Record any unresolved environment gates in known issues.