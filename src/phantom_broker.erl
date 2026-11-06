-module(phantom_broker).
-behaviour(gen_server).
-export([start_link/1, publish/3, subscribe/2, ack/2, stats/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {topic, queue = undefined, subscribers = #{}, inflight = #{}, next_id = 1, max_queue = 10000}).
start_link(Topic) -> gen_server:start_link(?MODULE, Topic, []).
publish(Pid, Payload, Options) -> gen_server:call(Pid, {publish, Payload, Options}).
subscribe(Pid, Consumer) -> gen_server:call(Pid, {subscribe, Consumer}).
ack(Pid, MessageId) -> gen_server:call(Pid, {ack, MessageId}).
stats(Pid) -> gen_server:call(Pid, stats).
init(Topic) ->
    process_flag(trap_exit, true),
    Max = application:get_env(phantom, max_queue, 10000),
    {ok, #state{topic=Topic, queue=queue:new(), max_queue=Max}}.
handle_call({subscribe, Consumer}, _From, State) when is_pid(Consumer) ->
    Ref = erlang:monitor(process, Consumer),
    Subs = maps:put(Consumer, Ref, State#state.subscribers),
    {reply, ok, State#state{subscribers=Subs}};
handle_call({publish, Payload, Options}, _From, State) when is_map(Options) ->
    case queue:len(State#state.queue) >= State#state.max_queue of
      true -> {reply, {error, queue_full}, State};
      false ->
        Id = State#state.next_id,
        Message = #{id => Id, topic => State#state.topic, payload => Payload,
                    timestamp => erlang:system_time(millisecond), attempts => 0},
        Persistent = maps:get(persistent, Options, false),
        case persist(Message, Persistent) of
          ok ->
            {reply, {ok, Id}, dispatch(State#state{queue=queue:in(Message, State#state.queue), next_id=Id+1})};
          Error -> {reply, Error, State}
        end
    end;
handle_call({ack, Id}, _From, State) ->
    case maps:take(Id, State#state.inflight) of
      error -> {reply, {error, unknown_message}, State};
      {#{monitor := Ref, consumer := Consumer}, Rest} ->
        erlang:demonitor(Ref, [flush]),
        Consumer ! {phantom_acked, Id},
        {reply, ok, State#state{inflight=Rest}}
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
dispatch(State=#state{queue=Queue, subscribers=Subs, inflight=Inflight}) ->
    case queue:out(Queue) of
      {empty, _} -> State;
      {{value, Message}, Rest} ->
        [{Consumer,_}|_] = maps:to_list(Subs),
        Ref = erlang:monitor(process, Consumer),
        Id = maps:get(id, Message),
        Consumer ! {phantom_message, maps:get(topic,Message), Id, maps:get(payload,Message)},
        Entry = #{consumer=>Consumer, monitor=>Ref, message=>Message},
        dispatch(State#state{queue=Rest,inflight=maps:put(Id,Entry,Inflight)})
    end.

persist(_Message, false) -> ok;
persist(Message, true) ->
    Dir = application:get_env(phantom, data_dir, "data"),
    ok = filelib:ensure_dir(filename:join(Dir, "placeholder")),
    Path = filename:join(Dir, "messages.log"),
    case file:open(Path, [append, raw, binary]) of
      {ok, Io} ->
        Result = file:write(Io, [term_to_binary(Message), <<10>>]),
        _ = file:close(Io),
        Result;
      {error, Reason} -> {error, {persistence_failed, Reason}}
    end.
