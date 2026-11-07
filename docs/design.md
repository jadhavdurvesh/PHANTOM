# PHANTOM design notes

## Process model

The application supervisor starts a topic registry and a broker supervisor. The registry starts one broker process per topic. Brokers isolate queue state and message delivery for their topic.

## Consumer lifecycle

A consumer subscribes by PID. The broker monitors each consumer and records monitor references for in-flight deliveries. Acknowledgement removes the in-flight entry and demonitor-flushes its delivery monitor. If the consumer exits first, the broker requeues its unacknowledged messages.

The current implementation is intentionally at-least-once. Duplicate deliveries are possible and must be handled by idempotent consumers.

## Backpressure

Each broker has a configurable maximum queued-message count. Publish returns queue_full rather than allowing the in-memory queue to grow without bound. The current limit does not include in-flight messages.

## Persistence

Persistent publish appends a length-prefixed external term to a local journal and syncs the file. Acknowledgements append topic-scoped ack records. On startup, each topic broker replays the journal and restores only unacknowledged persistent messages. Invalid or truncated records stop broker startup instead of being silently ignored. The journal currently has no checksum or compaction, and it is not replicated; disk exhaustion and multi-node ownership still require explicit operational policy.

## Distribution

The current broker is single-node. Erlang distribution, node membership, partition placement, replicated metadata and leader election are future milestones. They must not be inferred from the actor-based local architecture.

## Failure domains

The supervision tree restarts failed brokers. The topic registry monitors broker PIDs and removes stale entries. Consumer death triggers redelivery for that consumer's outstanding messages. A broker crash currently loses its in-memory queue; durable replay is not yet available.

## Consumer prefetch

The broker limits each subscriber's in-flight deliveries using `prefetch_per_consumer` (default 10, minimum 1). Dispatch skips consumers at their limit and leaves messages queued. Acknowledgement or consumer recovery triggers another dispatch pass. This bounds outstanding deliveries per consumer, but does not cap total in-flight messages across all consumers or provide global byte-based memory quotas.
