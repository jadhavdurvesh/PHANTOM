-module(phantom_broker_sup).
-behaviour(supervisor).
-export([start_link/0, start_broker/1, init/1]).
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
start_broker(Topic) ->
    supervisor:start_child(?MODULE, [Topic]).
init([]) ->
    Child = #{id => phantom_broker, start => {phantom_broker, start_link, []},
      restart => permanent, shutdown => 5000, type => worker, modules => [phantom_broker]},
    {ok, {{simple_one_for_one, 10, 10}, [Child]}}.
