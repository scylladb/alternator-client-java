# Node-health implementation

This document connects the [node-health specification](../node-health.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

[`NodeHealthConfig`](../../src/main/java/com/scylladb/alternator/NodeHealthConfig.java) exposes
thresholds, probe period, concurrency, timeout, and disabled configuration.
[`NodeHealthState`](../../src/main/java/com/scylladb/alternator/NodeHealthState.java),
[`NodeHealthObservation`](../../src/main/java/com/scylladb/alternator/NodeHealthObservation.java),
and [`NodeHealthStatus`](../../src/main/java/com/scylladb/alternator/NodeHealthStatus.java) expose
state, observations, counters, timestamps, and generation snapshots.

Client wrappers expose discovered and health-partitioned endpoint views and blocking and
asynchronous quarantine-probe operations. `getLiveNodes()` intentionally remains an alias for the
raw discovered topology view.

## Internal architecture

[`NodeHealthStore`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthStore.java) owns
canonical endpoint keys, counters, generations, and transitions.
[`NodeHealthManager`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthManager.java)
coordinates probe admission, priority, deduplication, timeout, suppression, and shutdown.

[`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java)
owns topology discovery and delegates health work to the manager.
[`SystemTableTopologyDiscovery`](../../src/main/java/com/scylladb/alternator/internal/SystemTableTopologyDiscovery.java)
issues endpoint-pinned DynamoDB scans of `system.local` and `system.peers`, paginates them, parses
`rpc_address`, `data_center`, and `rack`, and applies routing scopes locally.
[`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java)
applies final active and quarantine passes. Traffic integration belongs to
[`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java).

## Lifecycle and concurrency

Initial seeds are published in quarantine. The live-node polling thread independently schedules
topology refresh and background health cycles. Probe work uses a bounded priority executor and a
separate timeout scheduler. Capacity, including running jobs, is seventeen times configured
concurrency. Background admission splits available capacity across down and quarantine tiers,
rotates starting endpoints, and alternates a sole slot.

The live-node owner builds a dedicated synchronous DynamoDB client over the polling HTTP transport.
It uses the wrapper's effective credentials and region, disables SDK retries so candidate failover
and health attribution remain explicit, and applies the probe timeout to both each API call and the
complete paired topology read. Pagination rejects repeated cursors and observes interruption
between requests.
Every topology scan and probe supplies a request-level endpoint provider for the selected candidate,
so signing and transport target the same node. The client is closed with live-node discovery; a
caller-supplied polling HTTP transport remains caller-owned.

Topology refresh consistently scans `.scylla.alternator.system.local` followed by
`.scylla.alternator.system.peers`, follows every progressing `LastEvaluatedKey`, and retains only
complete candidate snapshots with a usable local row. It tries candidates until one snapshot has
an available endpoint in the preferred scope, then applies configured scope fallbacks to cached
complete snapshots only after every candidate misses the preferred scope. Invalid peer rows are
skipped. Direct health probes instead issue one consistent
`system.local` scan with `Select.COUNT` and a limit of one; any returned `ScanResponse` proves direct
contact without requiring a topology row.

Built-in cluster, datacenter, and rack scopes filter the parsed metadata directly. Compatibility
with a custom `RoutingScope` is limited to URL-decoded `dc` and `rack` parameters returned by its
legacy `getLocalNodesQuery()` method. Empty queries retain cluster scope, recognized filters apply
locally, and malformed, rack-only, or unsupported representations match no nodes instead of failing
open to cluster scope.

Built-in polling transports reserve one connection beyond probe concurrency. Externally supplied
polling transports must support concurrent calls. SDK scans do not expose the prepared request to
the health manager for direct cancellation, so the configured API-call timeout bounds running work
during probe timeout and shutdown. `shutdownAndWait` shares one timeout across the polling thread
and both probe executors.

Traffic routing captures `NodeHealthStatus.getGeneration()` at final eligibility and reports through
the generation-aware overload. A generation-unaware overload remains for compatibility.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `HEALTH-REQ-001` | [`NodeHealthConfig`](../../src/main/java/com/scylladb/alternator/NodeHealthConfig.java) | [`FeatureSpecDefaultsTest#nodeHealthDefaultsMatchSpecification`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java) | `conformant` |
| `HEALTH-REQ-002` | [`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java) | [`RetryDistributionTest#testRetryableServerErrorsDoNotReportHealthResults`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) | `conformant` |
| `HEALTH-REQ-003` | [`NodeHealthStore`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthStore.java) | [`FeatureSpecNodeHealthTransitionsTest#stateTransitionsMatchPortableTable`](../../src/test/java/com/scylladb/alternator/internal/FeatureSpecNodeHealthTransitionsTest.java) | `conformant` |
| `HEALTH-REQ-004` | [`NodeHealthStore`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthStore.java) | [`NodeHealthStoreTest#trafficFromGenerationBeforeDownIsIgnoredAfterRecovery`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthStoreTest.java) | `gap` |
| `HEALTH-REQ-005` | [`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java), [`SystemTableTopologyDiscovery`](../../src/main/java/com/scylladb/alternator/internal/SystemTableTopologyDiscovery.java) | [`AlternatorLiveNodesNodeHealthTest#discoveryActivatesContactedSeedButQuarantinesNewNodesUntilDirectProbe`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesNodeHealthTest.java) | `conformant` |
| `HEALTH-REQ-006` | [`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) | [`NodeHealthQueryPlanTest#regularPlanReturnsActiveThenQuarantineInSourceRelativeOrder`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java) | `conformant` |
| `HEALTH-REQ-007` | [`NodeHealthManager`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthManager.java) | [`AlternatorLiveNodesConcurrentProbeTest#explicitProbesRespectConfiguredConcurrencyAndReturnSnapshotOrder`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesConcurrentProbeTest.java) | `conformant` |
| `HEALTH-REQ-008` | [`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java), [`SystemTableTopologyDiscovery`](../../src/main/java/com/scylladb/alternator/internal/SystemTableTopologyDiscovery.java) | [`AlternatorLiveNodesNodeHealthTest#topologyRefreshUsesHealthBucketsAndDownFallbackIsHealthNeutral`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesNodeHealthTest.java) | `conformant` |
| `HEALTH-REQ-009` | [`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java) | [`AlternatorLiveNodesShutdownTest#testShutdownAndWaitClosesOwnedPollingClientWhenThreadNeverStarted`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesShutdownTest.java) | `conformant` |

## Test coverage

- [`FeatureSpecDefaultsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java)
  executes every machine-readable health default.
- [`FeatureSpecNodeHealthTransitionsTest`](../../src/test/java/com/scylladb/alternator/internal/FeatureSpecNodeHealthTransitionsTest.java)
  executes the portable transition table.
- [`NodeHealthConfigTest`](../../src/test/java/com/scylladb/alternator/NodeHealthConfigTest.java)
  covers defaults, normalization, and invalid probe settings.
- [`NodeHealthStoreTest`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthStoreTest.java)
  covers transitions, counter independence, canonical identity, generations, and disabled behavior.
- [`AlternatorLiveNodesNodeHealthTest`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesNodeHealthTest.java)
  covers admission, explicit probes, discovery, recovery, rediscovery, and scope behavior.
- [`AlternatorLiveNodesConcurrentProbeTest`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesConcurrentProbeTest.java)
  covers concurrency, queue admission, timeout, suppression, topology races, and rejection.
- [`SystemTableTopologyDiscoveryTest`](../../src/test/java/com/scylladb/alternator/internal/SystemTableTopologyDiscoveryTest.java)
  covers endpoint-pinned scans, pagination, parsing, deduplication, scope filtering, atomic failure,
  and the minimal direct-probe request.
- [`RetryDistributionTest`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java)
  covers per-attempt observations and health-neutral server statuses.
- [`BasicQueryPlanInterceptorTest`](../../src/test/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptorTest.java)
  covers final-gate revalidation.

## Known conformance gaps

- `HEALTH-REQ-004`: The generation-unaware compatibility overload cannot reject a late traffic
  result after recovery into a newer cycle. A traffic report made while an endpoint is down leaves
  state and counters unchanged but still advances observable update time. Concurrent or late-result
  integrations must use the generation-aware overload.
