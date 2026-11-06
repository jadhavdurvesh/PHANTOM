# PHANTOM design notes

## Process model

The application supervisor starts a topic registry and a broker supervisor. The registry starts one broker process per topic. Brokers isolate queue state and message delivery for their topic.

## Consumer lifecycle

A consumer subscribes by PID. The broker monitors each consumer and records monitor references for in-flight deliveries. Acknowledgement removes the in-flight entry and demonitor-flushes its delivery monitor. If the consumer exits first, the broker requeues its unacknowledged messages.

The current implementation is intentionally at-least-once. Duplicate deliveries are possible and must be handled by idempotent consumers.

## Backpressure

Each broker has a configurable maximum queued-message count. Publish returns queue_full rather than allowing the in-memory queue to grow without bound. The current limit does not include in-flight messages.

## Persistence

Persistent publish appends a serialized term to a local journal. This is not yet a write-ahead log with recovery guarantees. A production implementation must define record framing, checksums, fsync semantics, replay, truncation handling, compaction and ownership during failover.

## Distribution

The current broker is single-node. Erlang distribution, node membership, partition placement, replicated metadata and leader election are future milestones. They must not be inferred from the actor-based local architecture.

## Failure domains

The supervision tree restarts failed brokers. The topic registry monitors broker PIDs and removes stale entries. Consumer death triggers redelivery for that consumer's outstanding messages. A broker crash currently loses its in-memory queue; durable replay is not yet available.
