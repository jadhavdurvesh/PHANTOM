# PHANTOM — Independent Product Strategy

## Product identity
PHANTOM is a general-purpose fault-tolerant messaging and event-delivery system built around Erlang/OTP supervision, topic routing, acknowledgements, and recoverable delivery.

## Product boundary
- **Core:** broker lifecycle, topics, subscriptions, delivery modes, acknowledgements, retry policy, queue limits, persistence, and supervision.
- **Adapters:** network protocols, client libraries, authentication, metrics, and external storage.
- **Applications:** standalone event-driven service and telemetry examples.

The core must not depend on FireBot, SETU, robot event names, or FireBot envelope types. FireBot integration belongs in an optional adapter.

## Independent use cases
1. Application event delivery between backend services.
2. IoT and industrial telemetry distribution.
3. Background notification and asynchronous processing pipelines.

## Product requirements
- Explicit delivery guarantees and duplicate-delivery semantics.
- Separate broadcast and competing-consumer modes with fair dispatch.
- Bounded queues, in-flight limits, retry policy, TTL, and dead-letter handling.
- Crash-consistent journal recovery, checksums, and compaction.
- Authentication/authorization and secure transport before remote deployment.
- Metrics, tracing, load tests, chaos tests, and operational runbooks.

## Non-goals
PHANTOM must not claim exactly-once delivery or distributed availability until those properties are implemented and tested. A local journal is not replicated durability.

## FireBot adapter
An optional SETU adapter may map generic PHANTOM topics and acknowledgements to FireBot event contracts. Local emergency behavior must never depend on broker availability.

## Readiness gates
- [ ] Documented delivery semantics for each subscription mode.
- [ ] Durable recovery and corruption tests.
- [ ] Fair dispatch, retry, TTL, and dead-letter behavior.
- [ ] Two non-FireBot integration examples.
- [ ] Secure remote client boundary and operational tests.
