-module(phantom_broker).
-behaviour(gen_server).
-export([start_link/1, publish/3, subscribe/2, ack/2, stats/1, dead_letters/1, redrive/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {topic, queue = undefined, subscribers = #{}, inflight = #{}, next_id = 1, max_queue = 10000, prefetch = 10, max_delivery_attempts = 5, dead_letters = [], rr_cursor = 0}).

start_link(Topic) -> gen_server:start_link(?MODULE, Topic, []).
publish(Pid, Payload, Options) -> gen_server:call(Pid, {publish, Payload, Options}).
subscribe(Pid, Consumer) -> gen_server:call(Pid, {subscribe, Consumer}).
ack(Pid, MessageId) -> gen_server:call(Pid, {ack, MessageId}).
stats(Pid) -> gen_server:call(Pid, stats).
dead_letters(Pid) -> gen_server:call(Pid, dead_letters).
redrive(Pid, Id) -> gen_server:call(Pid, {redrive, Id}).

init(Topic) ->
    process_flag(trap_exit, true),
    Max = application:get_env(phantom, max_queue, 10000),
    Prefetch = max(1, application:get_env(phantom, prefetch_per_consumer, 10)),
    MaxAttempts = max(1, application:get_env(phantom, max_delivery_attempts, 5)),
    case recover_journal(Topic) of
      {ok, Messages, DeadLetters, NextId} ->
        {ok, #state{topic=Topic, queue=queue:from_list(Messages), max_queue=Max, prefetch=Prefetch, max_delivery_attempts=MaxAttempts, dead_letters=DeadLetters, next_id=NextId}};
      {error, Reason} -> {stop, {journal_recovery_failed, Reason}}
    end.

handle_call({subscribe, Consumer}, _From, State) when is_pid(Consumer) ->
    case maps:find(Consumer, State#state.subscribers) of
      {ok, _} -> {reply, ok, dispatch(State)};
      error ->
        Ref = erlang:monitor(process, Consumer),
        Subs = maps:put(Consumer, Ref, State#state.subscribers),
        {reply, ok, dispatch(State#state{subscribers=Subs})}
    end;
handle_call({publish, Payload, Options}, _From, State) when is_map(Options) ->
    case queue:len(State#state.queue) >= State#state.max_queue of
      true -> {reply, {error, queue_full}, State};
      false ->
        Id = State#state.next_id,
        Persistent = maps:get(persistent, Options, false),
        Message = #{id => Id, topic => State#state.topic, payload => Payload,
                    timestamp => erlang:system_time(millisecond), attempts => 0,
                    persistent => Persistent},
        case persist_event({publish, Message}, Persistent) of
          ok ->
            Next = dispatch(State#state{queue=queue:in(Message, State#state.queue), next_id=Id+1}),
            {reply, {ok, Id}, Next};
          Error -> {reply, Error, State}
        end
    end;
handle_call({ack, Id}, _From, State) ->
    case maps:find(Id, State#state.inflight) of
      error -> {reply, {error, unknown_message}, State};
      {ok, Entry} ->
        Message = maps:get(message, Entry),
        case persist_event({ack, State#state.topic, Id}, maps:get(persistent, Message, false)) of
          ok ->
            erlang:demonitor(maps:get(monitor, Entry), [flush]),
            maps:get(consumer, Entry) ! {phantom_acked, Id},
            {reply, ok, dispatch(State#state{inflight=maps:remove(Id, State#state.inflight)})};
          Error -> {reply, Error, State}
        end
    end;
handle_call(dead_letters, _From, State) ->
    {reply, lists:reverse(State#state.dead_letters), State};
handle_call({redrive, Id}, _From, State) when is_integer(Id) ->
    case lists:partition(fun(Message) -> maps:get(id, Message) =:= Id end, State#state.dead_letters) of
      {[], _} -> {reply, {error, unknown_dead_letter}, State};
      {[Message], Remaining} ->
        case persist_event({redrive, State#state.topic, Id}, maps:get(persistent, Message, false)) of
          ok ->
            Requeued = maps:remove(dead_letter_reason, maps:remove(dead_lettered_at, Message)),
            Next = dispatch(State#state{dead_letters=Remaining, queue=queue:in(Requeued, State#state.queue)}),
            {reply, ok, Next};
          Error -> {reply, Error, State}
        end
    end;
handle_call(stats, _From, State) ->
    {reply, #{topic => State#state.topic, queued => queue:len(State#state.queue),
              inflight => map_size(State#state.inflight), subscribers => map_size(State#state.subscribers), prefetch_per_consumer => State#state.prefetch, dead_letters => length(State#state.dead_letters)}, State};
handle_call(_Request, _From, State) -> {reply, {error, bad_request}, State}.

handle_cast(_Message, State) -> {noreply, State}.
handle_info({'DOWN', _Ref, process, Consumer, _Reason}, State) ->
    Subs = maps:remove(Consumer, State#state.subscribers),
    {Retry, Keep, Dead} = maps:fold(fun(Id, Entry, {R,K,D}) ->
      case maps:get(consumer, Entry) =:= Consumer of
        true ->
          Message = maps:get(message,Entry),
          Attempts = maps:get(attempts, Message, 1),
          case Attempts >= State#state.max_delivery_attempts of
            true ->
              Dead = Message#{dead_letter_reason => consumer_crash, dead_lettered_at => erlang:system_time(millisecond)},
              case persist_event({dead_letter, State#state.topic, Dead}, maps:get(persistent, Message, false)) of
                ok -> {R,K,[Dead|D]};
                Error ->
                  logger:error("PHANTOM could not persist dead-letter transition: ~p", [Error]),
                  {[{Id,Message}|R],K,D}
              end;
            false -> {[{Id,Message}|R], K, D}
          end;
        false -> {R,maps:put(Id,Entry,K),D}
      end
    end, {[],#{},State#state.dead_letters}, State#state.inflight),
    State1 = lists:foldl(fun({_Id,Message},Acc) -> Acc#state{queue=queue:in_r(Message,Acc#state.queue)} end,
                         State#state{inflight=Keep,subscribers=Subs,dead_letters=Dead}, Retry),
    {noreply, dispatch(State1)};
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_Old, State, _Extra) -> {ok, State}.

dispatch(State=#state{subscribers=Subs}) when map_size(Subs) =:= 0 -> State;
dispatch(State=#state{queue=Queue, subscribers=Subs, inflight=Inflight, rr_cursor=Cursor, prefetch=Prefetch}) ->
    case queue:out(Queue) of
      {empty, _} -> State;
      {{value, Message}, Rest} ->
        Eligible = lists:sort([Pid || Pid <- maps:keys(Subs), consumer_inflight(Pid, Inflight) < Prefetch]),
        case Eligible of
          [] -> State;
          _ ->
            Count = length(Eligible),
            Position = (Cursor rem Count) + 1,
            Consumer = lists:nth(Position, Eligible),
            Ref = erlang:monitor(process, Consumer),
            Id = maps:get(id, Message),
            Delivered = Message#{attempts => maps:get(attempts, Message, 0) + 1},
            Consumer ! {phantom_message, maps:get(topic,Message), Id, maps:get(payload,Message)},
            Entry = #{consumer=>Consumer, monitor=>Ref, message=>Delivered},
            dispatch(State#state{queue=Rest,inflight=maps:put(Id,Entry,Inflight),rr_cursor=Cursor+1})
        end
    end.

consumer_inflight(Consumer, Inflight) ->
    maps:fold(fun(_Id, Entry, Count) ->
      case maps:get(consumer, Entry) =:= Consumer of true -> Count+1; false -> Count end
    end, 0, Inflight).

journal_path() ->
    Dir = application:get_env(phantom, data_dir, "data"),
    filename:join(Dir, "messages.log").

persist_event(_Event, false) -> ok;
persist_event(Event, true) ->
    Path = journal_path(),
    ok = filelib:ensure_dir(Path),
    Encoded = term_to_binary(Event),
    Record = <<(byte_size(Encoded)):32/unsigned-big, Encoded/binary>>,
    case file:open(Path, [append, raw, binary]) of
      {ok, Io} ->
        Result = case file:write(Io, Record) of
          ok -> file:sync(Io);
          Error -> Error
        end,
        _ = file:close(Io),
        case Result of ok -> ok; {error, Reason} -> {error, {persistence_failed, Reason}} end;
      {error, Reason} -> {error, {persistence_failed, Reason}}
    end.

recover_journal(Topic) ->
    case file:read_file(journal_path()) of
      {error, enoent} -> {ok, [], [], 1};
      {error, Reason} -> {error, Reason};
      {ok, Data} -> replay_records(Data, Topic, queue:new(), [], 1)
    end.

replay_records(<<>>, _Topic, Queue, Dead, NextId) -> {ok, queue:to_list(Queue), Dead, NextId};
replay_records(<<Length:32/unsigned-big, Rest/binary>>, Topic, Queue, Dead, NextId)
  when Length > 0, byte_size(Rest) >= Length ->
    <<Encoded:Length/binary, Tail/binary>> = Rest,
    try binary_to_term(Encoded, [safe]) of
      {publish, #{id := Id, topic := Topic}=Message} when is_integer(Id), Id > 0 ->
        replay_records(Tail, Topic, queue:in(Message, Queue), Dead, max(NextId, Id+1));
      {ack, Topic, Id} when is_integer(Id) ->
        replay_records(Tail, Topic, queue:from_list([M || M <- queue:to_list(Queue), maps:get(id,M) =/= Id]), Dead, NextId);
      {ack, _OtherTopic, _Id} ->
        replay_records(Tail, Topic, Queue, Dead, NextId);
      {publish, _Other} -> replay_records(Tail, Topic, Queue, Dead, NextId);
      {dead_letter, Topic, Message=#{id := Id}} when is_integer(Id) ->
        Remaining = queue:from_list([M || M <- queue:to_list(Queue), maps:get(id,M) =/= Id]),
        replay_records(Tail, Topic, Remaining, [Message|Dead], NextId);
      {dead_letter, _OtherTopic, _Message} ->
        replay_records(Tail, Topic, Queue, Dead, NextId);
      {redrive, Topic, Id} when is_integer(Id) ->
        case lists:keytake(Id, 1, [{maps:get(id,M),M} || M <- Dead]) of
          {value, {_Id, Message}, _Rest} ->
            DeadRemaining = [M || M <- Dead, maps:get(id,M) =/= Id],
            replay_records(Tail, Topic, queue:in(Message, Queue), DeadRemaining, NextId);
          false -> {error, redrive_missing_dead_letter}
        end;
      {redrive, _OtherTopic, _Id} ->
        replay_records(Tail, Topic, Queue, Dead, NextId);
      _ -> {error, invalid_journal_record}
    catch _:_ -> {error, invalid_journal_term}
    end;
replay_records(_Truncated, _Topic, _Queue, _Dead, _NextId) -> {error, truncated_journal_record}.
