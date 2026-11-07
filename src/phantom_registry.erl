-module(phantom_registry).
-behaviour(gen_server).
-export([start_link/0, ensure_topic/1, lookup/1, topics/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
ensure_topic(Topic) -> gen_server:call(?MODULE, {ensure, Topic}).
lookup(Topic) -> gen_server:call(?MODULE, {lookup, Topic}).
topics() -> gen_server:call(?MODULE, topics).
init([]) -> {ok, #{}}.

handle_call({ensure, Topic}, _From, State) ->
    case maps:find(Topic, State) of
      {ok, {Pid, _Ref}} ->
        case erlang:is_process_alive(Pid) of
          true -> {reply, {ok, Pid}, State};
          false -> start_topic(Topic, maps:remove(Topic, State))
        end;
      error -> start_topic(Topic, State)
    end;
handle_call({lookup, Topic}, _From, State) ->
    case maps:find(Topic, State) of
      {ok, {Pid, _Ref}} ->
        case erlang:is_process_alive(Pid) of
          true -> {reply, {ok, Pid}, State};
          false -> {reply, not_found, maps:remove(Topic, State)}
        end;
      error -> {reply, not_found, State}
    end;
handle_call(topics, _From, State) -> {reply, maps:keys(State), State};
handle_call(_Request, _From, State) -> {reply, {error, bad_request}, State}.

start_topic(Topic, State) ->
    case phantom_broker_sup:start_broker(Topic) of
      {ok, Pid} ->
        Ref = erlang:monitor(process, Pid),
        {reply, {ok, Pid}, maps:put(Topic, {Pid, Ref}, State)};
      {error, {already_started, Pid}} ->
        Ref = erlang:monitor(process, Pid),
        {reply, {ok, Pid}, maps:put(Topic, {Pid, Ref}, State)};
      Error -> {reply, Error, State}
    end.

handle_cast(_Message, State) -> {noreply, State}.
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    Updated = maps:filter(fun(_Topic, {Value, Monitor}) ->
      not (Value =:= Pid andalso Monitor =:= Ref)
    end, State),
    {noreply, Updated};
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_Old, State, _Extra) -> {ok, State}.
