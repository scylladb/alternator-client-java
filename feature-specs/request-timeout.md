# Request timeout

This specification defines a per-request extension for overriding the server-side execution
timeout of an Alternator operation through the `X-Scylla-Timeout` HTTP header.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

The timeout override lets one operation ask a supporting Alternator endpoint to use a different
server-side timeout from its configured default. It applies to server work created by that request;
it is not a client-side attempt deadline, whole-operation deadline, retry budget, or cancellation
mechanism.

The contract covers typed timeout construction, direct header use, wire behavior, retries, and
server interpretation. It does not require an endpoint that does not implement this extension to
honor the header.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Timeout header | The case-insensitive HTTP header named `X-Scylla-Timeout`. |
| Typed timeout | A non-negative, exactly millisecond-aligned duration supplied through a typed per-request option. |
| Direct header value | A caller-supplied CQL duration string placed in the timeout header without typed conversion. |
| Logical operation | One client operation, including its initial transport attempt and any retries. |
| Attempt | One HTTP request sent as part of a logical operation. |
| Server deadline | Deadline created by a supporting endpoint for one attempt and shared by all backend work for that request. |
| Client deadline | Independent client limit governing an attempt or whole logical operation. |

## Configuration and defaults

The override is request-scoped and has no client-wide default. A request that does not select a
typed timeout or directly set the timeout header must not acquire the header through this feature.
In that case a supporting endpoint retains its configured timeout behavior.

The typed option defines only `X-Scylla-Timeout`. It must not emit
`X-Scylla-Default-Timeout` or turn a request override into a connection-wide default.

A typed timeout must be non-null, non-negative, exactly representable as a whole number of
milliseconds, and within the fixed-duration range supported by CQL. Zero is valid and distinct
from an unset timeout. The canonical wire representation is the unsigned total millisecond count
in decimal followed immediately by `ms`, such as `0ms` or `5000ms`.

A direct header value may use CQL duration syntax rather than the canonical typed form. Direct use
does not enable a client default and does not request client-side validation or normalization.

## Required behavior

### Typed request option

Every supported low-level operation request must allow a typed timeout to be attached through its
ordinary per-request options. The client must validate the value before sending the operation and
must encode an accepted value in exactly one timeout header. Applying the typed option must
replace earlier values for that header while preserving unrelated request options.

The client must reject null, negative, sub-millisecond, fractional-millisecond, and out-of-range
typed durations. It must not truncate, round, saturate, or silently omit them.

### Direct header option

Every supported low-level operation request must also allow a caller to place a raw value in the
timeout header through ordinary per-request header configuration. The client must transmit the
selected value without converting it to the canonical typed form. A supporting endpoint, rather
than the client, is responsible for validating the CQL duration string.

### Attempts and retries

The initial attempt and every retry belonging to one logical operation must carry the same timeout
header value. Routing a retry to another endpoint must not change, decrement, or remove the value.
Each received attempt is a new server request, so a supporting endpoint must create a fresh server
deadline from the same duration for that attempt. All reads, writes, conditional operations, and
other backend work created within that attempt must share that one server deadline.

The timeout header must not enable, disable, delay, or otherwise change client retry policy. It
must not alter client attempt deadlines, whole-operation deadlines, cancellation, or transport
timeouts.

### Server interpretation

A supporting endpoint must treat the timeout header as an override for only the current request.
Header-name matching must be case-insensitive. A valid value must be a non-negative CQL duration
whose fixed-time component resolves exactly to millisecond granularity and contains no month or
day component. Zero must be accepted as a real timeout value rather than treated as absent.

A malformed, negative, month-bearing, day-bearing, sub-millisecond, or out-of-range value must
produce a `ValidationException`. An accepted value must be able to increase or reduce the
endpoint's configured timeout. Expiration must use the endpoint's normal request-timeout response
and all deadline-aware backend work must use the same effective deadline.

### Operation and transport parity

The behavior must apply to all low-level Alternator operations, including reads, writes, queries,
scans, batches, transactions, and conditional operations. Blocking and non-blocking clients must
produce the same timeout header and per-attempt behavior.

## Interactions with other features

### Header optimization

The timeout header must remain eligible when [header optimization](header-optimization.md) is
enabled, even though most requests omit it. Filtering must preserve its name and value on every
attempt. The client must add the timeout header to the effective custom whitelist automatically so
previously valid whitelist configurations remain compatible. Requests without a timeout override
must still omit it.

### Authentication and signing

The timeout value must enter request processing early enough for authentication to observe the
final header. If a signature covers the header, later transforms must preserve the signed name and
value. Enabling or disabling authentication must not otherwise change timeout semantics.

### Compression

[Request compression](compression.md) may transform the body and its framing headers, but it must
not transform or remove the timeout header. Response compression is independent of the timeout
override.

### Routing and node health

[Query-plan](query-plan.md) routing may choose a different destination for each attempt without
changing the timeout value. A server-side timeout result participates in normal attempt outcome
and [node-health](node-health.md) handling; selecting the override must not itself mark an endpoint
healthy or unhealthy.

### Client-side deadlines and cancellation

A caller may combine this feature with independent client deadlines. The first deadline to take
effect may determine what the caller observes. Client cancellation or deadline expiry does not
change the timeout header already sent and does not guarantee that server work has stopped.

### Non-supporting endpoints

Behavior after transmission is guaranteed only for endpoints that implement this extension. Other
endpoints may ignore or reject the proprietary header. The client must not infer support, retry
without the header, or claim that an unsupported endpoint enforced the requested timeout.

## Edge cases

- A zero typed timeout must produce `X-Scylla-Timeout: 0ms`; it must not remove the header.
- A positive typed duration smaller than one millisecond or containing a fractional millisecond
  must fail before transmission.
- A typed duration beyond the fixed CQL duration range must fail before transmission.
- Direct values such as `5s` may reach a supporting endpoint without canonicalization.
- Direct values containing negative durations, month or day components, a non-zero
  sub-millisecond remainder, or malformed text must receive `ValidationException` from a
  supporting endpoint.
- Header-name casing must not affect server interpretation or filtering.
- A retry must receive the original duration rather than the time remaining from a previous
  attempt.
- Combining a shorter client deadline with a longer server timeout may cause the client to stop
  waiting before the server deadline expires.
- Applying the typed option over existing timeout-header values must leave one canonical value and
  preserve unrelated headers and request settings.

## Conformance requirements

### TIMEOUT-REQ-001: Typed encoding and default

The client must provide a request-scoped typed timeout, encode accepted values as canonical total
milliseconds followed by `ms`, preserve zero, replace earlier timeout-header values, and omit the
header when the option is unset.

### TIMEOUT-REQ-002: Typed validation

The client must reject null, negative, sub-millisecond, fractional-millisecond, and out-of-range
typed durations before request transmission without rounding or truncation.

### TIMEOUT-REQ-003: Low-level operation coverage

Every low-level operation request must accept the timeout through ordinary per-request options.

### TIMEOUT-REQ-004: Retry and deadline isolation

Every client attempt of one logical operation must carry the unchanged timeout value, and the
typed option must preserve independently configured client attempt and whole-operation deadlines.

### TIMEOUT-REQ-005: Pipeline compatibility

Authentication, signing, header filtering, compression, routing, and request transformation must
preserve the selected timeout header through their normal request processing.

### TIMEOUT-REQ-006: Transport-form parity

Blocking and non-blocking clients must emit equivalent timeout header values for the same operation
request.

### TIMEOUT-REQ-007: Server validation and enforcement

A supporting endpoint must validate CQL duration syntax and millisecond granularity, reject
invalid fixed-duration values with `ValidationException`, and apply one effective per-attempt
deadline to all backend work for every Alternator operation. Each client retry must establish a
fresh server deadline from the original duration.
