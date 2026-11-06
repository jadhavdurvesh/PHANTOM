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
      {ok, {Pid, _Ref}} when is_process_alive(Pid) -> {reply, {ok, Pid}, State};
      _ ->
        case phantom_broker_sup:start_broker(Topic) of
          {ok, Pid} ->
            Ref = erlang:monitor(process, Pid),
            {reply, {ok, Pid}, maps:put(Topic, {Pid, Ref}, State)};
          {error, {already_started, Pid}} ->
            Ref = erlang:monitor(process, Pid),
            {reply, {ok, Pid}, maps:put(Topic, {Pid, Ref}, State)};
          Error -> {reply, Error, State}
        end
    end;
handle_call({lookup, Topic}, _From, State) ->
    case maps:find(Topic, State) of
      {ok, {Pid, _Ref}} when is_process_alive(Pid) -> {reply, {ok, Pid}, State};
      _ -> {reply, not_found, maps:remove(Topic, State)}
    end;
handle_call(topics, _From, State) -> {reply, maps:keys(State), State};
handle_call(_Request, _From, State) -> {reply, {error, bad_request}, State}.

handle_cast(_Message, State) -> {noreply, State}.
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    Updated = maps:filter(fun(_Topic, {Value, Monitor}) ->
      not (Value =:= Pid andalso Monitor =:= Ref)
    end, State),
    {noreply, Updated};
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_Old, State, _Extra) -> {ok, State}.
