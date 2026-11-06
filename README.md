# PHANTOM

**A fault-tolerant actor-based messaging broker built with Erlang/OTP.**

PHANTOM explores OTP supervision, process isolation, topic routing, monitored consumers, acknowledgement-based delivery, bounded queues and an append-only persistence journal.

## Implemented

- OTP application and supervision tree
- Dynamic per-topic broker processes
- Topic registry with broker lifecycle monitoring
- Publish and subscribe API
- Consumer process monitoring
- Message IDs and explicit acknowledgements
- In-flight message tracking
- Requeue of unacknowledged deliveries after consumer termination
- Bounded in-memory queues and queue-full backpressure
- Optional append-only message journal
- Broker statistics
- EUnit tests and GitHub Actions workflow

## Quick start

Requirements: Erlang/OTP 26+ and Rebar3.

    rebar3 compile
    rebar3 eunit
    rebar3 shell

In the Erlang shell:

    application:ensure_all_started(phantom).
    Topic = <<"orders.created">>.
    phantom:subscribe(Topic, self()).
    {ok, MessageId} = phantom:publish(Topic, #{order_id => 42}, #{}).
    receive
      {phantom_message, Topic, MessageId, Payload} ->
        io:format("received: ~p~n", [Payload]),
        phantom:ack(Topic, MessageId)
    end.

## Delivery model

A broker delivers queued messages to a subscribed consumer and tracks them as in-flight until acknowledged. If a consumer process exits before acknowledgement, its monitored in-flight messages are returned to the queue for redelivery.

This is **at-least-once delivery**, not exactly-once delivery. Consumers should be idempotent because a message can be delivered again after a crash or acknowledgement race.

## Persistence boundary

Publishing with the option map #{persistent => true} appends a serialized Erlang term to the configured journal file. The current milestone does not replay the journal after restart, compact it, fsync every record, or coordinate journal state across nodes. It is an append-only persistence experiment, not yet a durable queue guarantee.

## Architecture

    PHANTOM application
      |
      +-- phantom_sup (one_for_one)
            |
            +-- phantom_registry
            |
            +-- phantom_broker_sup
                    |
                    +-- broker process per topic

Each broker is a gen_server. Consumers are ordinary Erlang processes monitored by the broker. Topic registry entries are monitored so crashed broker processes do not remain as valid lookups.

## Current limitations

- Single-node runtime; distributed cluster membership and replicated state are not implemented.
- The current dispatcher selects one subscriber per delivery and does not yet implement competing-consumer fairness or broadcast fan-out.
- Persistent journal replay and crash-consistent recovery are not implemented.
- No network protocol or remote client API yet.
- Queue limits are per broker and currently count queued, not in-flight, messages.

See ROADMAP.md and docs/design.md.

## License

MIT.
