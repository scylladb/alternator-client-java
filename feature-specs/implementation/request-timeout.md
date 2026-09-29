# Request timeout implementation

This document connects the [request-timeout specification](../request-timeout.md) to the
implementation in this repository. It is informative: endpoint-side behavior lives in ScyllaDB
and is outside this client's implementation boundary.

## Public API

[`AlternatorRequestOptions`](../../src/main/java/com/scylladb/alternator/AlternatorRequestOptions.java)
exposes `SERVER_TIMEOUT_HEADER` with value `X-Scylla-Timeout` and
`serverTimeout(Duration)`. The method returns a consumer for an AWS per-request override builder,
so every low-level synchronous or asynchronous DynamoDB request can use the same form:

```java
GetItemRequest request = GetItemRequest.builder()
    .tableName("users")
    .key(key)
    .overrideConfiguration(
        AlternatorRequestOptions.serverTimeout(Duration.ofSeconds(5)))
    .build();
```

The helper validates eagerly and emits the canonical value `5000ms`. It accepts zero, rejects
null, negative, non-millisecond-aligned, and out-of-range durations, replaces earlier values for
the header, and leaves other override settings intact.

Callers needing another valid CQL spelling can set the public header constant directly:

```java
.overrideConfiguration(options -> options.putHeader(
    AlternatorRequestOptions.SERVER_TIMEOUT_HEADER, "5s"))
```

No client-level timeout default is added, and this repository does not emit
`X-Scylla-Default-Timeout`. The AWS DynamoDB Enhanced Client does not expose these per-operation
override configurations. Applications needing a different timeout for one Enhanced operation must
issue that operation through the low-level client.

## Internal architecture

The helper converts the immutable `Duration` when it is called, captures the canonical string, and
writes that value into the operation's `AwsRequestOverrideConfiguration` when the request builder
applies the returned consumer. No execution interceptor or request-body change is required. The AWS
request pipeline carries the operation override into each attempt.

[`AlternatorConfig`](../../src/main/java/com/scylladb/alternator/AlternatorConfig.java) includes the
timeout header in the required header set and adds it automatically to an explicitly configured
whitelist. Consequently both blocking and non-blocking header-filtering transports preserve it
when optimization is active without rejecting older custom whitelists. The header begins in the
operation override before signing and remains independent of request and response compression.
Query-plan routing may change the attempt URI but does not rebuild the operation override.

Direct header configuration uses the same request override path but intentionally bypasses helper
validation and canonicalization. A compatible ScyllaDB endpoint owns CQL parsing, validation, and
deadline enforcement. Server support is tracked in
[scylladb/scylladb#31652](https://github.com/scylladb/scylladb/issues/31652).

## Lifecycle and concurrency

`serverTimeout` returns a stateless consumer that captures only an immutable header string. The
consumer may be reused across independently owned request builders; it adds no executor, transport,
or closeable resource. Request builders retain their normal thread-safety rules.

The immutable operation request retains one override value for its initial attempt and retries.
Each retry sends that same value through the selected blocking or non-blocking transport. Any fresh
deadline created upon receipt is server state, not client state. Client call timeouts, attempt
timeouts, retry decisions, completion, and cancellation continue to be owned by the AWS request
pipeline.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `TIMEOUT-REQ-001` | [`AlternatorRequestOptions`](../../src/main/java/com/scylladb/alternator/AlternatorRequestOptions.java) | [`ServerTimeoutRequestTest#timeoutIsIsolatedPerRequest`](../../src/test/java/com/scylladb/alternator/ServerTimeoutRequestTest.java) | `conformant` |
| `TIMEOUT-REQ-002` | [`AlternatorRequestOptions`](../../src/main/java/com/scylladb/alternator/AlternatorRequestOptions.java) | [`AlternatorRequestOptionsTest#serverTimeoutRejectsInvalidDurations`](../../src/test/java/com/scylladb/alternator/AlternatorRequestOptionsTest.java) | `conformant` |
| `TIMEOUT-REQ-003` | [`AlternatorRequestOptions`](../../src/main/java/com/scylladb/alternator/AlternatorRequestOptions.java) | [`AlternatorRequestOptionsTest#allDynamoDbOperationRequestModelsAcceptServerTimeout`](../../src/test/java/com/scylladb/alternator/AlternatorRequestOptionsTest.java) | `conformant` |
| `TIMEOUT-REQ-004` | [`AlternatorRequestOptions`](../../src/main/java/com/scylladb/alternator/AlternatorRequestOptions.java) | [`ServerTimeoutRequestTest#retryAttemptsPreserveServerTimeout`](../../src/test/java/com/scylladb/alternator/ServerTimeoutRequestTest.java) | `conformant` |
| `TIMEOUT-REQ-005` | [`AlternatorConfig`](../../src/main/java/com/scylladb/alternator/AlternatorConfig.java) | [`ServerTimeoutRequestTest#syncTimeoutSurvivesHeaderOptimizationAndSigning`](../../src/test/java/com/scylladb/alternator/ServerTimeoutRequestTest.java) | `conformant` |
| `TIMEOUT-REQ-006` | [`AlternatorRequestOptions`](../../src/main/java/com/scylladb/alternator/AlternatorRequestOptions.java) | [`ServerTimeoutRequestTest#asyncTimeoutMatchesSync`](../../src/test/java/com/scylladb/alternator/ServerTimeoutRequestTest.java) | `conformant` |
| `TIMEOUT-REQ-007` | Server implementation outside this repository | Server tests outside this repository | `not-applicable` |

## Test coverage

- [`AlternatorRequestOptionsTest`](../../src/test/java/com/scylladb/alternator/AlternatorRequestOptionsTest.java)
  covers canonical encoding, zero, range boundaries, eager rejection, replacement of existing
  values, preservation of unrelated overrides, and all generated low-level operation request
  models.
- [`ServerTimeoutRequestTest`](../../src/test/java/com/scylladb/alternator/ServerTimeoutRequestTest.java)
  covers synchronous and asynchronous wire headers, direct and typed values, per-request
  isolation, retries, omission when unset, authentication, signing, and header optimization
  without requiring server support.
- Existing header-filter tests verify that `X-Scylla-Timeout` is present in the required set and
  survives blocking and non-blocking optimized transports.

The request tests inspect the outgoing HTTP attempts with a recording transport. They do not claim
that an endpoint has parsed or enforced the header.

## Known conformance gaps

None within the repository-owned low-level client scope. Endpoint parsing, validation, and deadline
enforcement are outside this repository and remain pending in
[scylladb/scylladb#31652](https://github.com/scylladb/scylladb/issues/31652). The Enhanced Client's
lack of per-operation override configuration is a documented API scope limitation; callers can use
the low-level client for operations requiring this extension.
