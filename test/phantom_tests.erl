-module(phantom_tests).
-include_lib("eunit/include/eunit.hrl").

broker_publish_ack_test_() ->
    {setup,
      fun() ->
        application:load(phantom),
        application:set_env(phantom, max_queue, 32),
        {ok, _} = application:ensure_all_started(phantom),
        ok
      end,
      fun(_) -> application:stop(phantom) end,
      fun(_) ->
        Topic = <<"eunit.events">>,
        ok = phantom:subscribe(Topic, self()),
        {ok, Id} = phantom:publish(Topic, #{event => created}, #{}),
        receive
          {phantom_message, Topic, Id, #{event := created}} ->
            ?assertEqual(ok, phantom:ack(Topic, Id))
        after 1000 ->
          ?assert(false)
        end,
        Stats = phantom:stats(Topic),
        ?assertEqual(0, maps:get(inflight, Stats))
      end}.

unknown_topic_ack_test() ->
    application:load(phantom),
    case application:ensure_all_started(phantom) of
      {ok, _} -> ok;
      {error, {already_started, _}} -> ok
    end,
    ?assertEqual({error, unknown_topic}, phantom:ack(<<"missing">>, 1)),
    application:stop(phantom).
