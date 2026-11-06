-module(phantom_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
init([]) ->
    Children = [
      #{id => phantom_registry, start => {phantom_registry, start_link, []},
        restart => permanent, shutdown => 5000, type => worker, modules => [phantom_registry]},
      #{id => phantom_broker_sup, start => {phantom_broker_sup, start_link, []},
        restart => permanent, shutdown => infinity, type => supervisor, modules => [phantom_broker_sup]}
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.
