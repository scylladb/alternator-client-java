# Query-plan implementation

This document connects the [query-plan specification](../query-plan.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

Query planning is installed automatically by the client builders. Key-route affinity selects an
affinity-aware interceptor; other requests use ordinary random routing. There is no separate public
plan-lifecycle API.

## Internal architecture

[`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) captures
and orders discovered candidates.
[`GoRand`](../../src/main/java/com/scylladb/alternator/internal/GoRand.java) implements the reference
seeded generator, while `AlternatorLiveNodes.drainSeeded(...)` implements pick-and-remove selection.

[`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java)
applies active and quarantine passes and cycle behavior. Request-scoped routing state belongs to
[`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java).
It is connected to physical transmissions by
[`AttemptRoutingSdkHttpClient`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRoutingSdkHttpClient.java)
and
[`AttemptRoutingSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRoutingSdkAsyncHttpClient.java).
[`AttemptRequestSigner`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRequestSigner.java)
re-signs a prepared attempt when final routing changes its authority.

## Lifecycle and concurrency

`beforeExecution` creates one plan and routing state. `modifyHttpRequest` selects the initial route.
`beforeTransmission` registers execution attributes by `amz-sdk-invocation-id`, and the outer
transport wrapper performs final route selection for each physical transmission before header
filtering. When selection changes an already signed request's scheme, host, or port, the wrapper
replaces `Host` and invokes the SDK-selected authentication signer again with the original payload.
`afterTransmission` clears in-flight state for every response; execution completion or failure
unregisters routing state.

The routing registry is concurrent. Individual plans are request-scoped and synchronized only where
health-aware selection mutates tried-state.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `QUERY-REQ-001` | [`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) | [`LazyQueryPlanTest#testLazyBehaviorReadsCurrentNodes`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) | `gap` |
| `QUERY-REQ-002` | [`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) | [`LazyQueryPlanTest#testNodesAreNotDuplicated`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) | `conformant` |
| `QUERY-REQ-003` | [`GoRand`](../../src/main/java/com/scylladb/alternator/internal/GoRand.java) | [`FeatureSpecVectorsTest#seededPlansMatchPortableVectors`](../../src/test/java/com/scylladb/alternator/FeatureSpecVectorsTest.java) | `conformant` |
| `QUERY-REQ-004` | [`LazyQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/LazyQueryPlan.java) | [`LazyQueryPlanTest#testPreferredNodesAreReturnedBeforeSortedRemaining`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) | `gap` |
| `QUERY-REQ-005` | [`BasicQueryPlanInterceptor`](../../src/main/java/com/scylladb/alternator/queryplan/BasicQueryPlanInterceptor.java) | [`RetryDistributionTest#testSdkRetryPipelineRoutesEachAttemptToDifferentNode`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) | `conformant` |
| `QUERY-REQ-006` | [`NodeHealthQueryPlan`](../../src/main/java/com/scylladb/alternator/internal/NodeHealthQueryPlan.java) | [`NodeHealthQueryPlanTest#regularPlanReturnsActiveThenQuarantineInSourceRelativeOrder`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java) | `conformant` |
| `QUERY-REQ-007` | [`AttemptRoutingSdkAsyncHttpClient`](../../src/main/java/com/scylladb/alternator/queryplan/AttemptRoutingSdkAsyncHttpClient.java) | [`RetryDistributionTest#testAsyncSdkRetryPipelineRoutesEachAttemptToDifferentNode`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java) | `conformant` |

## Test coverage

- [`FeatureSpecVectorsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecVectorsTest.java)
  executes the portable seeded-order table.
- [`FeatureSpecCanonicalEndpointVectorsTest`](../../src/test/java/com/scylladb/alternator/internal/FeatureSpecCanonicalEndpointVectorsTest.java)
  executes the portable canonical-endpoint table against the implementation's sort and deduplication
  path.
- [`LazyQueryPlanTest`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanTest.java) covers
  iteration, random and seeded uniqueness, preferred prefixes, sorting, and health-agnostic order.
- [`LazyQueryPlanCrossLanguageTest`](../../src/test/java/com/scylladb/alternator/LazyQueryPlanCrossLanguageTest.java)
  covers canonical seeded sequences for fixed, negative, zero, and maximum seeds.
- [`RetryDistributionTest`](../../src/test/java/com/scylladb/alternator/RetryDistributionTest.java)
  covers retry traversal, cycles, physical blocking and non-blocking routing, in-flight attribution,
  authority replacement, signature validation, and server-response health classification.
- [`AttemptRequestSignerTest`](../../src/test/java/com/scylladb/alternator/queryplan/AttemptRequestSignerTest.java)
  covers signer payload replacement and asynchronous cancellation propagation.
- [`NodeHealthQueryPlanTest`](../../src/test/java/com/scylladb/alternator/internal/NodeHealthQueryPlanTest.java)
  covers active and quarantine passes, dynamic eligibility, canonical duplication, and down-node
  exclusion.

## Known conformance gaps

- `QUERY-REQ-001`: The lazy-snapshot boundary lacks a topology-mutation test. The test named
  `testLazyBehaviorReadsCurrentNodes` neither changes topology nor verifies behavior before and after
  initialization; its comment incorrectly says every iterator call reads current state.
- `QUERY-REQ-004`: Preferred lists containing duplicates or unavailable endpoints lack direct tests.
