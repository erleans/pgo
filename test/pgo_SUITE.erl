-module(pgo_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("pgo_internal.hrl").

-define(DATABASE, "test").
-define(USER, "test").
-define(PASSWORD, "password").

-define(UNTIL(X), (fun Until(I) when I =:= 10 -> erlang:error(fail);
                       Until(I) -> case X of true -> ok; false -> timer:sleep(10), Until(I+1) end end)(0)).

all() -> [checkout_checkin, checkout_break, checkout_kill, checkout_disconnect, checkout_query_crash].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    ok.

init_per_testcase(T, Config) when T =:= checkout_break ->
    Name = pool_break,
    pgo_sup:start_child(Name, #{pool_size => 1,
                                database => ?DATABASE,
                                user => ?USER,
                                password => ?PASSWORD}),

    Tid = pgo_pool:tid(Name),
    ?UNTIL((catch ets:info(Tid, size)) =:= 1),

    [{pool_name, Name} | Config];
init_per_testcase(T, Config) when T =:= checkout_query_crash ->
    Name = pool_query_crash,
    pgo_sup:start_child(Name, #{pool_size => 1,
                                database => ?DATABASE,
                                user => ?USER,
                                password => ?PASSWORD}),

    Tid = pgo_pool:tid(Name),
    ?UNTIL((catch ets:info(Tid, size)) =:= 1),

    [{pool_name, Name} | Config];
init_per_testcase(checkout_kill, Config) ->
    Name = pool_kill,
    pgo_sup:start_child(Name, #{pool_size => 10,
                                database => ?DATABASE,
                                user => ?USER,
                                password => ?PASSWORD}),
    Tid = pgo_pool:tid(Name),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    [{pool_name, Name} | Config];
init_per_testcase(_, Config) ->
    Name = pool_1,
    pgo_sup:start_child(Name, #{pool_size => 10,
                                database => ?DATABASE,
                                user => ?USER,
                                password => ?PASSWORD}),
    Tid = pgo_pool:tid(Name),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    [{pool_name, Name} | Config].

end_per_testcase(_, _Config) ->
    ok.

checkout_checkin(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, Ref, Conn=#conn{}} = pgo:checkout(Name),
    {ok, Ref1, Conn1=#conn{}} = pgo:checkout(Name),

    ?UNTIL((catch ets:info(Tid, size)) =:= 8),

    pgo:checkin(Ref, Conn),
    pgo:checkin(Ref1, Conn1),

    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    ok.

checkout_break(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, Ref, Conn=#conn{owner=Pid, socket=Socket}} = pgo:checkout(Name),
    pgo_connection:break(Conn, Ref),

    ?UNTIL((catch ets:info(Tid, size)) =:= 1),

    %% verify that the connection we broke is not still in the pool
    %% but the Pid for the pgo_connection proc should be the same
    {ok, _Ref1, #conn{owner=Pid1, socket=Socket1}} = pgo:checkout(Name),
    ?assertNotEqual(Socket, Socket1),
    ?assertEqual(Pid, Pid1),

    ok.

checkout_kill(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, _Ref, #conn{socket=Socket}} = pgo:checkout(Name),
    {ok, _Ref1, #conn{owner=Pid1}} = pgo:checkout(Name),

    erlang:exit(Socket, kill),
    ?UNTIL((catch ets:info(Tid, size)) =:= 9),

    erlang:exit(Pid1, kill),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    ok.

checkout_disconnect(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, Ref, Conn} = pgo:checkout(Name),
    {ok, _Ref1, #conn{owner=Pid1}} = pgo:checkout(Name),

    pgo_pool:disconnect(Ref, some_error, Conn, []),
    ?UNTIL((catch ets:info(Tid, size)) =:= 9),

    erlang:exit(Pid1, kill),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    ok.

%% regression test. this would fail with `unexpected message` response to the create query
checkout_query_crash(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, _Ref, Conn} = pgo:checkout(Name),
    pgo:with_conn(Conn, fun() ->
                          pgo:query("select $1::uuid", [<<1,2,3,4,5>>])
                        end),
    pgo:checkin(_Ref, Conn),

    ?UNTIL((catch ets:info(Tid, size)) =:= 1),
    {ok, _Ref1, Conn1} = pgo:checkout(Name),
    pgo:with_conn(Conn1, fun() ->
                           ?assertMatch(#{command := create},
                                        pgo:query("create temporary table foo (_id integer)", []))
                         end),

    ok.
