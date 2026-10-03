# Key-route affinity implementation

This document connects the [key-route-affinity specification](../key-route-affinity.md) to the
implementation in this repository. It is informative: deviations recorded here do not weaken the
generic contract.

## Public API

[`KeyRouteAffinity`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinity.java)
defines `NONE`, `RMW`, and `ANY_WRITE`.
[`KeyRouteAffinityConfig`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfig.java)
stores the mode and optional table-to-partition-key mappings. Both client builders accept either a
mode or full configuration.
[`AlternatorLiveNodes#enableAffinityRouting`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java)
is the idempotent integration signal used by an enabled affinity interceptor. It enables the
derived affinity topology view under the manager's existing refresh schedule.
[`AlternatorLiveNodes#getAffinityQueryPlanNodesSnapshot`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java)
captures the canonical ring for one request, while
[`LazyQueryPlan#fromAffinitySnapshot`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java)
preserves that snapshot through batch voting and retry traversal.

## Internal architecture

[`KeyAffinityRequestClassifier`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyAffinityRequestClassifier.java)
classifies operations and extracts keys and batch targets.
[`PartitionKeyResolver`](../../src/main/java/com/scylladb/alternator/keyrouting/PartitionKeyResolver.java)
caches metadata and runs synchronous `DescribeTable` calls on a single background executor.

[`AttributeValueHasher`](../../src/main/java/com/scylladb/alternator/keyrouting/AttributeValueHasher.java)
encodes typed values, and
[`MurmurHash3`](../../src/main/java/com/scylladb/alternator/keyrouting/MurmurHash3.java) produces the
seed. [`AffinityQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java)
selects seeded single-item plans or aggregates batch votes against one captured topology snapshot.
[`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java)
keeps the configured routing-scope ring for ordinary plans. When its chain contains a rack,
`initializeAffinityScopes` resolves and deduplicates the fixed built-in prefix once, widening built-in
racks to their named datacenters. Opaque custom scopes stop eager traversal. Each needed custom
fallback link retains its successful resolution once per occurrence across concurrent refreshes;
failed resolution is retried later. Built-ins behind those boundaries are deferred too. Repeated
object identity does not truncate stateful finite chains, and custom runtime queries remain intact.
This selected compatibility exception differs from complete eager resolution in `AFF-REQ-008`.
`refreshAffinityNodes` shares actual ordinary query and fallback observations per occurrence and
uses the existing discovery helper. Matching query-plus-aggregation outcomes, including empty and
failed results, are reused only within that refresh. Explicit cluster routing retains seed
aggregation and partial success. A usable view is retained before a hidden rack is reached, so
later failed DC discovery keeps the previous snapshot. New affinity candidates use existing
node-health registration and immediate immutable publication. Quarantine, explicit probes and
background probes retain their existing handling; topology refresh adds no synchronous verification
or readiness requirement.

## Lifecycle and concurrency

The blocking wrapper supplies its built DynamoDB client to the resolver for background discovery.
The non-blocking wrapper has no discovery client and relies on preconfiguration. Resolver misses are
deduplicated per table. Client close shuts down the resolver before stopping live-node management
and closing transports.

Affinity activation does not interrupt the live-node manager or change its polling schedule.
Deferred structural links are coordinated privately; client discovery I/O and health publication
occur outside that coordination. Per-refresh custom observations preserve each ordinary visit's
query and selected fallback. Successful structural links persist, while query values and discovery
outcomes remain local to one refresh. Arbitrary custom fallback code may still not terminate when
needed.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `AFF-REQ-001` | [`KeyAffinityRequestClassifier`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyAffinityRequestClassifier.java) | [`FeatureSpecDefaultsTest#affinityDefaultMatchesSpecification`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java) | `conformant` |
| `AFF-REQ-002` | [`KeyRouteAffinityConfig`](../../src/main/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfig.java) and [`PartitionKeyResolver`](../../src/main/java/com/scylladb/alternator/keyrouting/PartitionKeyResolver.java) | [`PartitionKeyResolverTest#testRetryOnModeledStructured400WithoutAwsErrorDetails`](../../src/test/java/com/scylladb/alternator/keyrouting/PartitionKeyResolverTest.java) | `gap` |
| `AFF-REQ-003` | [`AttributeValueHasher`](../../src/main/java/com/scylladb/alternator/keyrouting/AttributeValueHasher.java) | [`FeatureSpecVectorsTest#affinityHashesMatchPortableVectors`](../../src/test/java/com/scylladb/alternator/FeatureSpecVectorsTest.java) | `gap` |
| `AFF-REQ-004` | [`AffinityQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java) | [`AffinityQueryPlanInterceptorTest#testPutItemSimple_AnyWrite_Affinity`](../../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) | `conformant` |
| `AFF-REQ-005` | [`AffinityQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/AffinityQueryPlanInterceptor.java) | [`BatchWriteItemKeyRouteAffinityCrossLanguageTest#testVotePreferenceIgnoresNonKeyAttributes`](../../src/test/java/com/scylladb/alternator/keyrouting/BatchWriteItemKeyRouteAffinityCrossLanguageTest.java) and [`AffinityQueryPlanInterceptorTest#testBatchWriteItemKeepsOneAffinityRingSnapshotAcrossPublication`](../../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) | `gap` |
| `AFF-REQ-006` | [`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) | [`AffinityQueryPlanInterceptorTest#testBatchWriteItemSkipsDownPreferredNodeUsingStableKnownOrder`](../../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java) | `conformant` |
| `AFF-REQ-007` | [`AlternatorDynamoDbClientWrapper`](../../src/main/java/com/scylladb/alternator/AlternatorDynamoDbClientWrapper.java) | [`AlternatorDynamoDbClientWrapperShutdownTest#testAsyncWrapperShutsDownAffinityResolverBeforeClosingClients`](../../src/test/java/com/scylladb/alternator/AlternatorDynamoDbClientWrapperShutdownTest.java) | `gap` |
| `AFF-REQ-008` | [`AlternatorLiveNodes`](../../src/main/java/com/scylladb/alternator/internal/AlternatorLiveNodes.java) | [`AlternatorLiveNodesAffinityScopeTest#testRmwAffinityOverridesRackScopeWhileReadsRemainLocal`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesAffinityScopeTest.java) and [`AlternatorLiveNodesAffinityScopeTest#testAuxiliaryFailureDoesNotEvaluateUnusedCustomDescription`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesAffinityScopeTest.java) | `gap` |

## Test coverage

- [`FeatureSpecDefaultsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java)
  executes the machine-readable disabled default.
- [`KeyRouteAffinityConfigTest`](../../src/test/java/com/scylladb/alternator/keyrouting/KeyRouteAffinityConfigTest.java)
  covers modes and preconfigured metadata.
- [`PartitionKeyResolverTest`](../../src/test/java/com/scylladb/alternator/keyrouting/PartitionKeyResolverTest.java)
  covers caching, retries, cooldown, clearing, and concurrent misses.
- [`AttributeValueHasherTest`](../../src/test/java/com/scylladb/alternator/keyrouting/AttributeValueHasherTest.java)
  and
  [`AttributeValueHasherCrossLanguageTest`](../../src/test/java/com/scylladb/alternator/keyrouting/AttributeValueHasherCrossLanguageTest.java)
  cover typed inputs and fixed hash vectors.
- [`AffinityQueryPlanInterceptorTest`](../../src/test/java/com/scylladb/alternator/AffinityQueryPlanInterceptorTest.java)
  covers plan selection, batch voting, ties, fallback, and health interactions.
- [`LazyQueryPlanCrossLanguageTest`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanCrossLanguageTest.java)
  covers seeded endpoint orders.
- [`AlternatorLiveNodesAffinityScopeTest`](../../src/test/java/com/scylladb/alternator/internal/AlternatorLiveNodesAffinityScopeTest.java)
  covers named-datacenter affinity precedence, explicit fallback order, custom queries and shared
  discovery outcomes. Single-item and batch writes preserve request snapshots across refreshes;
  reads and writes without a usable affinity key remain rack-scoped. Affinity publication uses
  existing quarantine and explicit/background probe handling without a readiness wait.

## Known conformance gaps

- `AFF-REQ-003`: The hasher checks `S`, `N`, and `B` fields in order and hashes the first present.
  An invalid value containing multiple supported fields does not use random fallback and lacks a
  malformed-value test.
- `AFF-REQ-005`: The batch classifier adds two targets when an invalid write contains both put and
  delete shapes and dereferences a null write-list entry instead of safely skipping it.
- `AFF-REQ-002`: The bulk preconfiguration helper retains null keys and values, which later fail
  when copied into the concurrent resolver cache. The single-entry helper ignores null arguments.
- `AFF-REQ-007`: Interrupted retry sleep restores interruption but can continue into another
  discovery attempt. After its graceful wait, shutdown calls immediate cancellation without waiting
  again, so an uncooperative discovery task may outlive client close.

- `AFF-REQ-008`: The selected custom-scope compatibility exception resolves opaque fallback links
  only when first needed, including built-ins behind them. Built-in-only chains resolve completely
  at initialization, but opaque tails do not satisfy the generic complete-eager-resolution wording.
  Each successful link is retained safely per occurrence, with no new input restriction or
  guarantee that arbitrary custom fallback code terminates.
