-module(phantom_broker).
-behaviour(gen_server).
-export([start_link/1, publish/3, subscribe/2, ack/2, stats/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {topic, queue = undefined, subscribers = #{}, inflight = #{}, next_id = 1, max_queue = 10000, rr_cursor = 0}).

start_link(Topic) -> gen_server:start_link(?MODULE, Topic, []).
publish(Pid, Payload, Options) -> gen_server:call(Pid, {publish, Payload, Options}).
subscribe(Pid, Consumer) -> gen_server:call(Pid, {subscribe, Consumer}).
ack(Pid, MessageId) -> gen_server:call(Pid, {ack, MessageId}).
stats(Pid) -> gen_server:call(Pid, stats).

init(Topic) ->
    process_flag(trap_exit, true),
    Max = application:get_env(phantom, max_queue, 10000),
    case recover_journal(Topic) of
      {ok, Messages, NextId} ->
        {ok, #state{topic=Topic, queue=queue:from_list(Messages), max_queue=Max, next_id=NextId}};
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
            {reply, ok, State#state{inflight=maps:remove(Id, State#state.inflight)}};
          Error -> {reply, Error, State}
        end
    end;
handle_call(stats, _From, State) ->
    {reply, #{topic => State#state.topic, queued => queue:len(State#state.queue),
              inflight => map_size(State#state.inflight), subscribers => map_size(State#state.subscribers)}, State};
handle_call(_Request, _From, State) -> {reply, {error, bad_request}, State}.

handle_cast(_Message, State) -> {noreply, State}.
handle_info({'DOWN', _Ref, process, Consumer, _Reason}, State) ->
    Subs = maps:remove(Consumer, State#state.subscribers),
    {Retry, Keep} = maps:fold(fun(Id, Entry, {R,K}) ->
      case maps:get(consumer, Entry) =:= Consumer of
        true -> {[{Id,maps:get(message,Entry)}|R], K};
        false -> {R,maps:put(Id,Entry,K)}
      end
    end, {[],#{}}, State#state.inflight),
    State1 = lists:foldl(fun({_Id,Message},Acc) -> Acc#state{queue=queue:in_r(Message,Acc#state.queue)} end,
                         State#state{inflight=Keep,subscribers=Subs}, Retry),
    {noreply, dispatch(State1)};
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_Old, State, _Extra) -> {ok, State}.

dispatch(State=#state{subscribers=Subs}) when map_size(Subs) =:= 0 -> State;
dispatch(State=#state{queue=Queue, subscribers=Subs, inflight=Inflight, rr_cursor=Cursor}) ->
    case queue:out(Queue) of
      {empty, _} -> State;
      {{value, Message}, Rest} ->
        Consumers = lists:sort(maps:keys(Subs)),
        Count = length(Consumers),
        Position = (Cursor rem Count) + 1,
        Consumer = lists:nth(Position, Consumers),
        Ref = erlang:monitor(process, Consumer),
        Id = maps:get(id, Message),
        Consumer ! {phantom_message, maps:get(topic,Message), Id, maps:get(payload,Message)},
        Entry = #{consumer=>Consumer, monitor=>Ref, message=>Message},
        dispatch(State#state{queue=Rest,inflight=maps:put(Id,Entry,Inflight),rr_cursor=Cursor+1})
    end.

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
      {error, enoent} -> {ok, [], 1};
      {error, Reason} -> {error, Reason};
      {ok, Data} -> replay_records(Data, Topic, queue:new(), 1)
    end.

replay_records(<<>>, _Topic, Queue, NextId) -> {ok, queue:to_list(Queue), NextId};
replay_records(<<Length:32/unsigned-big, Rest/binary>>, Topic, Queue, NextId)
  when Length > 0, byte_size(Rest) >= Length ->
    <<Encoded:Length/binary, Tail/binary>> = Rest,
    try binary_to_term(Encoded, [safe]) of
      {publish, #{id := Id, topic := Topic}=Message} when is_integer(Id), Id > 0 ->
        replay_records(Tail, Topic, queue:in(Message, Queue), max(NextId, Id+1));
      {ack, Topic, Id} when is_integer(Id) ->
        replay_records(Tail, Topic, queue:from_list([M || M <- queue:to_list(Queue), maps:get(id,M) =/= Id]), NextId);
      {publish, _Other} -> replay_records(Tail, Topic, Queue, NextId);
      _ -> {error, invalid_journal_record}
    catch _:_ -> {error, invalid_journal_term}
    end;
replay_records(_Truncated, _Topic, _Queue, _NextId) -> {error, truncated_journal_record}.
