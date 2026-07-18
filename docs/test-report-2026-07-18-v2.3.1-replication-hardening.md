# Test Report - V2.3.1 Replication Hardening / 2026-07-18

## Scope

This report covers the V2.3.1 replication hardening release candidate on branch `release/v2.3.1`.

Included fixes:

- Preferred-order and LEO-aware leader election.
- Controller-visible persisted replica LEO.
- ISR lag shrink and safe re-expansion with bounded lag tolerance.
- Segment append/read-plan synchronization for sendfile safety.
- Sendfile prefix-failure channel close.
- Focused `ReplicaFetcher`, `LeaderReplicaClient`, and `REPLICA_FETCH` handler coverage.

Deferred after V2.3.1:

- R3-P1-05: pooled replica-fetch connections per leader broker.

## Verification Command

```powershell
mvn clean verify -gs .mvn/settings.xml -s .mvn/settings.xml
```

## Latest Evidence

Release-branch verification after changing Maven versions from `2.3.1-SNAPSHOT` to `2.3.1`:

- Result: `BUILD SUCCESS`
- Total time: `10:22 min`
- Modules: `mq-common`, `mq-storage`, `mq-protocol`, `mq-broker`, `mq-client`, `mq-demo`
- Coverage: JaCoCo checks passed for all checked modules.
- Demo artifact: `mq-demo-2.3.1.jar`

## Release Decision

V2.3.1 is ready as a patch release. R3-P1-05 replica-fetch connection pooling remains deferred to V2.3.2.
