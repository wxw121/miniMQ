# Known Issues - V3.0.1-V3.2 Productionization

Date: 2026-07-20
Branch: `dev/v3.0.0-control-plane-ha`

## Resolution Policy

Issues that cannot be resolved after five bounded attempts are recorded here and carried to the next version. Local validation must avoid reboot-risk actions and must not rely on Docker unless the Docker API is already accessible.

## KI-2026-07-20-01: Real-etcd Failsafe Environment Not Reached

Status: Open, environment-gated.

The `-Petcd-it` profile did not reach Failsafe real-etcd tests in the earlier V3.0.0 verification pass because Surefire stopped first. Docker Desktop processes were present, but the local sandbox could not access `npipe:////./pipe/docker_engine`; no standalone etcd or Podman runtime was available.

Next command when Docker API access is available:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am -Petcd-it verify
```

Do not mark the real-etcd gate complete until `ControllerFailoverIT`, `EtcdControllerCoordinationStoreIT`, `LeaderEpochFailoverIT`, and `EtcdClusterMetadataRepositoryIT` run and their fresh results are recorded.

Latest recheck on 2026-07-21:

```powershell
docker version
```

Result: exit code 1. Docker client metadata was printed, but local access still failed with `WARNING: Error loading config file: open C:\Users\Wxw\.docker\config.json: Access is denied.` and `permission denied while trying to connect to the docker API at npipe:////./pipe/docker_engine`.

## KI-2026-07-20-02: Failover FETCH Test Race

Status: Fixed in the working tree for V3.0.1.

Baseline command:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am '-Dtest=ReplicationIntegrationTest#clientFetchAfterFailoverWithoutWaitingForFollowerFetch' '-Dsurefire.failIfNoSpecifiedTests=false' '-Djacoco.skip=true' test
```

Baseline result before the V3.0.1 test synchronization fix: exit code 1, `expected: <NONE> but was: <INVALID_REQUEST>` at `ReplicationIntegrationTest.java:132`.

Root cause: the test could route a client FETCH to the elected replacement broker before the replacement broker had refreshed local metadata and before the Controller metadata exposed `leaderReady=true`. The test intent is to avoid waiting for an extra follower fetch, not to skip the Leader activation handshake.

V3.0.1 fix: `clientFetchAfterFailoverWithoutWaitingForFollowerFetch` now waits for the elected broker's local leader view and the Controller's `leaderReady=true` metadata before issuing FETCH with the ready leader epoch.

Verification after the fix:

```powershell
mvn -q -gs .mvn/settings.xml -s .mvn/settings.xml -pl mq-broker -am '-Dtest=ReplicationIntegrationTest#clientFetchAfterFailoverWithoutWaitingForFollowerFetch' '-Dsurefire.failIfNoSpecifiedTests=false' '-Djacoco.skip=true' test
```

Result: exit code 0. Expected connection-refused warnings from stopped/offline brokers may still appear during the fault-injection window.
