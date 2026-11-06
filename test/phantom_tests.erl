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

consumer_crash_redelivery_test_() ->
    {setup,
      fun() ->
        application:load(phantom),
        {ok, _} = application:ensure_all_started(phantom),
        ok
      end,
      fun(_) -> application:stop(phantom) end,
      fun(_) ->
        Topic = <<"eunit.redelivery">>,
        Parent = self(),
        Consumer = spawn(fun() ->
          receive
            {phantom_message, Topic, _Id, _Payload} -> Parent ! first_delivery
          end
        end),
        ok = phantom:subscribe(Topic, Consumer),
        {ok, Id} = phantom:publish(Topic, retry_me, #{}),
        receive first_delivery -> ok after 1000 -> ?assert(false) end,
        Ref = erlang:monitor(process, Consumer),
        exit(Consumer, kill),
        receive {'DOWN', Ref, process, Consumer, _} -> ok after 1000 -> ?assert(false) end,
        ok = phantom:subscribe(Topic, self()),
        receive
          {phantom_message, Topic, Id, retry_me} ->
            ?assertEqual(ok, phantom:ack(Topic, Id))
        after 1000 ->
          ?assert(false)
        end
      end}.

persistent_journal_recovery_test_() ->
    {setup,
      fun() ->
        Dir = filename:join("/tmp", "phantom-journal-" ++ integer_to_list(erlang:unique_integer([positive]))),
        application:load(phantom),
        application:set_env(phantom, data_dir, Dir),
        {ok, _} = application:ensure_all_started(phantom),
        Dir
      end,
      fun(Dir) ->
        application:stop(phantom),
        _ = file:del_dir_r(Dir),
        ok
      end,
      fun(_Dir) ->
        Topic = <<"persistent.audit">>,
        {ok, Id} = phantom:publish(Topic, #{value => 99}, #{persistent => true}),
        application:stop(phantom),
        {ok, _} = application:ensure_all_started(phantom),
        {ok, _Pid} = phantom:ensure_topic(Topic),
        Stats = phantom:stats(Topic),
        ?assertEqual(1, maps:get(queued, Stats)),
        ok = phantom:subscribe(Topic, self()),
        receive
          {phantom_message, Topic, Id, #{value := 99}} ->
            ?assertEqual(ok, phantom:ack(Topic, Id))
        after 1000 -> ?assert(false)
        end,
        application:stop(phantom),
        {ok, _} = application:ensure_all_started(phantom),
        {ok, _} = phantom:ensure_topic(Topic),
        ?assertEqual(0, maps:get(queued, phantom:stats(Topic)))
      end}.
