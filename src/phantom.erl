-module(phantom).
-export([start/0, ensure_topic/1, publish/3, subscribe/2, ack/2, stats/1, dead_letters/1, topics/0]).
start() -> application:ensure_all_started(phantom).
ensure_topic(Topic) ->
    case phantom_registry:ensure_topic(Topic) of
      {ok, Pid} -> {ok, Pid};
      Error -> Error
    end.
publish(Topic, Payload, Options) ->
    case ensure_topic(Topic) of
      {ok, Pid} -> phantom_broker:publish(Pid, Payload, Options);
      Error -> Error
    end.
subscribe(Topic, Consumer) ->
    case ensure_topic(Topic) of
      {ok, Pid} -> phantom_broker:subscribe(Pid, Consumer);
      Error -> Error
    end.
ack(Topic, Id) ->
    case phantom_registry:lookup(Topic) of
      {ok, Pid} -> phantom_broker:ack(Pid, Id);
      not_found -> {error, unknown_topic}
    end.
stats(Topic) ->
    case phantom_registry:lookup(Topic) of
      {ok, Pid} -> phantom_broker:stats(Pid);
      not_found -> {error, unknown_topic}
    end.
dead_letters(Topic) ->
    case phantom_registry:lookup(Topic) of
      {ok, Pid} -> {ok, phantom_broker:dead_letters(Pid)};
      not_found -> {error, unknown_topic}
    end.
topics() -> phantom_registry:topics().
