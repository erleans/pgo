-module(pgo_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DATABASE, "test").
-define(USER, "test").

-define(UNTIL(X), (fun Until(I) when I =:= 10 -> erlang:error(fail);
                       Until(I) -> case X of true -> ok; false -> timer:sleep(10), Until(I+1) end end)(0)).

all() -> [checkout_checkin, checkout_break, checkout_kill].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(checkout_break, Config) ->
    Name = pool_break,
    pgo:start_pool(Name, [{size, 1},
                          {postgres, [{database, ?DATABASE},
                                      {user, ?USER}]}]),

    Tid = pgo_pool:tid(Name),
    ?UNTIL((catch ets:info(Tid, size)) =:= 1),

    [{pool_name, Name} | Config];
init_per_testcase(checkout_kill, Config) ->
    Name = pool_kill,
    pgo:start_pool(Name, [{size, 10},
                          {postgres, [{database, ?DATABASE},
                                      {user, ?USER}]}]),

    Tid = pgo_pool:tid(Name),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    [{pool_name, Name} | Config];
init_per_testcase(_, Config) ->
    Name = pool_1,
    pgo:start_pool(Name, [{size, 10},
                          {postgres, [{database, ?DATABASE},
                                      {user, ?USER}]}]),
    Tid = pgo_pool:tid(Name),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    [{pool_name, Name} | Config].

end_per_testcase(_, _Config) ->
    ok.

checkout_checkin(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, Ref, {Pid, Socket}} = pgo:checkout(Name),
    {ok, Ref1, {Pid1, Socket1}} = pgo:checkout(Name),

    ?UNTIL((catch ets:info(Tid, size)) =:= 8),

    pgo:checkin(Ref, {Pid, Socket}),
    pgo:checkin(Ref1, {Pid1, Socket1}),

    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    ok.

checkout_break(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, _Ref, {Pid, Socket}} = pgo:checkout(Name),
    pgo_connection:break({Pid, []}),

    ?UNTIL((catch ets:info(Tid, size)) =:= 1),

    %% verify that the connection we broke is not still in the pool
    %% but the Pid for the pgo_connection proc should be the same
    {ok, _Ref1, {Pid1, Socket1}} = pgo:checkout(Name),
    ?assertNotEqual(Socket, Socket1),
    ?assertEqual(Pid, Pid1),

    ok.

checkout_kill(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, _Ref, {_Pid, Socket}} = pgo:checkout(Name),
    {ok, _Ref1, {Pid1, _Socket1}} = pgo:checkout(Name),

    erlang:exit(Socket, kill),
    ?UNTIL((catch ets:info(Tid, size)) =:= 9),

    erlang:exit(Pid1, kill),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    ok.
