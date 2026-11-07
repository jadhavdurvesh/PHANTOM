# PHANTOM roadmap

## M0 — OTP broker
- [x] OTP application and supervision tree
- [x] Dynamic topic broker processes
- [x] Topic registry and broker monitoring
- [x] Publish and subscribe API
- [x] Consumer monitoring
- [x] Message acknowledgements
- [x] In-flight tracking and redelivery after consumer exit
- [x] Bounded in-memory queue
- [x] Optional append-only journal
- [x] Broker statistics and EUnit tests

## M1 — Messaging semantics
- [ ] Broadcast and competing-consumer subscription modes
- [ ] Consumer prefetch and fair dispatch
- [ ] Retry limits and dead-letter queues
- [ ] Message TTL and expiration
- [x] Framed journal records with topic-scoped acknowledgement events
- [x] Journal replay for unacknowledged persistent messages
- [x] Per-record file sync
- [ ] Journal checksums and compaction
- [ ] Per-topic quotas and rate limits

## M2 — Distribution
- [ ] Node membership and health
- [ ] Remote publish/subscribe
- [ ] Partition-aware topic placement
- [ ] Replicated metadata
- [ ] Leader election and broker failover
- [ ] Network partition and split-brain tests

## M3 — Operations
- [ ] Metrics and tracing
- [ ] Chaos testing and fault injection
- [ ] Load and latency benchmark suite
- [ ] Authentication and authorization
- [ ] TLS transport
- [ ] Rolling upgrades and operational runbooks

- [x] Deterministic round-robin dispatch across competing consumers
