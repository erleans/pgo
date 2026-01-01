-module(pgo_listen_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [listen_notify, named_listen_notify, listen_notify_special_name].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             port => 5432,
                                             database => "test",
                                             user => "test",
                                             password => "password"}),

    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    ok.

listen_notify(_Config) ->

    {ok, Pid} = pgo_notifications:start_link(#{database => "test",
                                               user => "test",
                                               password => "password"}),
    Chan1 = <<"chan1">>,
    Chan2 = <<"chan2">>,

    {_, Ref1} = pgo_notifications:listen(Pid, Chan1),
    {_, Ref2} = pgo_notifications:listen(Pid, Chan1),

    ?assertMatch(#{command := notify},
                 pgo:query("NOTIFY chan1, 'message 1'")),

    receive
        {notification, Pid, Ref1, Chan1, Payload} ->
            ?assertEqual(<<"message 1">>, Payload)
    after
        1000 ->
            ct:fail(timeout)
    end,

    receive
        {notification, Pid, Ref2, Chan1, Payload1} ->
            ?assertEqual(<<"message 1">>, Payload1)
    after
        1000 ->
            ct:fail(timeout)
    end,

    %% unlisten and check only Ref1 gets the notification
    ok = pgo_notifications:unlisten(Pid, Ref2),

    ?assertMatch(#{command := notify},
                 pgo:query("NOTIFY chan1, 'message 2'")),

    receive
        {notification, Pid, Ref1, Chan1, Payload2} ->
            ?assertEqual(<<"message 2">>, Payload2)
    after
        1000 ->
            ct:fail(timeout)
    end,

    %% should not get a notification to reference 2
    receive
        {notification, Pid, Ref2, Chan1, _} ->
            ct:fail(unlisten_failed)
    after
        0 ->
            ok
    end,

    {_, Ref3} = pgo_notifications:listen(Pid, Chan2),

    ?assertMatch(#{command := notify},
                 pgo:query("NOTIFY chan2, 'message 3'")),

    receive
        {notification, Pid, Ref3, Chan2, Payload3} ->
            ?assertEqual(<<"message 3">>, Payload3)
    after
        1000 ->
            ct:fail(timeout)
    end,

    pgo_notifications:unlisten(Pid, Ref1),
    pgo_notifications:unlisten(Pid, Ref2),
    pgo_notifications:unlisten(Pid, Ref3),

    ?assertMatch(#{command := notify},
                 pgo:query("NOTIFY chan1, 'Hello, listeners!'")),

    %% should not get a notification
    receive
        {notification, Pid, Ref1, Chan1, _} ->
            ct:fail(unlisten_failed)
    after
        0 ->
            ok
    end,
    ok.

named_listen_notify(_Config) ->
    {ok, Pid} = pgo_notifications:start_link({local, notification_db},
                                             #{database => "test",
                                               user => "test",
                                               password => "password"}),
    Chan1 = <<"chan1">>,

    {_, Ref1} = pgo_notifications:listen(notification_db, Chan1),

    ?assertMatch(#{command := notify},
                 pgo:query("NOTIFY chan1, 'message 1'")),

    receive
        {notification, Pid, Ref1, Chan1, Payload} ->
            ?assertEqual(<<"message 1">>, Payload)
    after
        1000 ->
            ct:fail(timeout)
    end,

    ok.

listen_notify_special_name(_Config) ->
    {ok, Pid} = pgo_notifications:start_link(#{database => "test",
                                               user => "test",
                                               password => "password"}),
    Chan1 = <<"chan1:\"hello\"">>,

    {_, Ref1} = pgo_notifications:listen(Pid, Chan1),

    ?assertMatch(#{command := notify},
                 pgo:query("NOTIFY \"chan1:\"\"hello\"\"\", 'message 1'")),

    receive
        {notification, Pid, Ref1, Chan1, Payload} ->
            ?assertEqual(<<"message 1">>, Payload)
    after
        1000 ->
            ct:fail(timeout)
    end,

    ok.
