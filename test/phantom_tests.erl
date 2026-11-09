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
      fun(_) -> fun() ->
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
      end end}.

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
      fun(_) -> fun() ->
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
      end end}.

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
      fun(_) -> fun() ->
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
      end end}.


round_robin_consumer_fairness_test_() ->
    {setup,
      fun() ->
        application:load(phantom),
        {ok, _} = application:ensure_all_started(phantom),
        ok
      end,
      fun(_) -> application:stop(phantom) end,
      fun(_) -> fun() ->
        Topic = <<"fairness.round_robin">>,
        Parent = self(),
        Consumer = fun(Name) ->
          spawn(fun Loop() ->
            receive
              {phantom_message, Topic, Id, Payload} ->
                Parent ! {delivered, Name, Id, Payload},
                Loop()
            end
          end)
        end,
        A = Consumer(a),
        B = Consumer(b),
        ok = phantom:subscribe(Topic, A),
        ok = phantom:subscribe(Topic, B),
        {ok, _} = phantom:publish(Topic, first, #{}),
        {ok, _} = phantom:publish(Topic, second, #{}),
        receive {delivered, Name1, _, _} -> receive
          {delivered, Name2, _, _} ->
            ?assertNotEqual(Name1, Name2)
          after 1000 -> ?assert(false)
        end after 1000 -> ?assert(false) end,
        exit(A, kill),
        exit(B, kill)
      end end}.


consumer_prefetch_limit_test_() ->
    {setup,
      fun() ->
        application:load(phantom),
        application:set_env(phantom, prefetch_per_consumer, 1),
        {ok, _} = application:ensure_all_started(phantom),
        ok
      end,
      fun(_) -> application:stop(phantom) end,
      fun(_) -> fun() ->
        Topic = <<"prefetch.single">>,
        ok = phantom:subscribe(Topic, self()),
        {ok, First} = phantom:publish(Topic, first, #{}),
        {ok, Second} = phantom:publish(Topic, second, #{}),
        receive
          {phantom_message, Topic, First, first} ->
            receive
              {phantom_message, Topic, Second, second} ->
                ?assert(false)
            after 100 -> ok
            end,
            ?assertEqual(ok, phantom:ack(Topic, First)),
            receive
              {phantom_message, Topic, Second, second} ->
                ?assertEqual(ok, phantom:ack(Topic, Second))
            after 1000 -> ?assert(false)
            end
        after 1000 -> ?assert(false)
        end
      end end}.

consumer_crash_dead_letter_test_() ->
    {setup,
      fun() ->
        Dir = filename:join("/tmp", "phantom-dead-letter-" ++ integer_to_list(erlang:unique_integer([positive]))),
        application:load(phantom),
        application:set_env(phantom, data_dir, Dir),
        application:set_env(phantom, max_delivery_attempts, 1),
        {ok, _} = application:ensure_all_started(phantom),
        Dir
      end,
      fun(Dir) ->
        application:stop(phantom),
        _ = file:del_dir_r(Dir),
        ok
      end,
      fun(_Dir) -> fun() ->
        Topic = <<"dead-letter.crash">>,
        Parent = self(),
        Consumer = spawn(fun() ->
          receive {phantom_message, Topic, _Id, _Payload} -> Parent ! delivered end
        end),
        ok = phantom:subscribe(Topic, Consumer),
        {ok, _Id} = phantom:publish(Topic, terminal, #{persistent => true}),
        receive delivered -> ok after 1000 -> ?assert(false) end,
        Ref = erlang:monitor(process, Consumer),
        exit(Consumer, kill),
        receive {'DOWN', Ref, process, Consumer, _} -> ok after 1000 -> ?assert(false) end,
        {ok, Dead} = phantom:dead_letters(Topic),
        ?assertEqual(1, length(Dead)),
        [Entry] = Dead,
        ?assertEqual(consumer_crash, maps:get(dead_letter_reason, Entry)),
        application:stop(phantom),
        {ok, _} = application:ensure_all_started(phantom),
        {ok, _} = phantom:ensure_topic(Topic),
        {ok, Recovered} = phantom:dead_letters(Topic),
        ?assertEqual(1, length(Recovered)),
        [RecoveredEntry] = Recovered,
        ?assertEqual(consumer_crash, maps:get(dead_letter_reason, RecoveredEntry)),
        ok = phantom:subscribe(Topic, self()),
        ?assertEqual(ok, phantom:redrive(Topic, _Id)),
        receive
          {phantom_message, Topic, RedrivenId, terminal} ->
            ?assertEqual(ok, phantom:ack(Topic, RedrivenId))
        after 1000 -> ?assert(false)
        end,
        application:stop(phantom),
        {ok, _} = application:ensure_all_started(phantom),
        {ok, _} = phantom:ensure_topic(Topic),
        {ok, []} = phantom:dead_letters(Topic),
        ?assertEqual(0, maps:get(queued, phantom:stats(Topic)))
      end end}.
