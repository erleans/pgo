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

all() -> [checkout_checkin, checkout_break, recheckout, kill_socket, kill_pid,
          checkout_kill, checkout_disconnect, checkout_query_crash].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    ok.

init_per_testcase(T, Config) when T =:= checkout_break ;
                                  T =:= checkout_query_crash ;
                                  T =:= recheckout ;
                                  T =:= kill_socket ;
                                  T =:= kill_pid ->
    pgo_sup:start_child(T, #{pool_size => 1,
                             database => ?DATABASE,
                             user => ?USER,
                             password => ?PASSWORD}),

    Tid = pgo_pool:tid(T),
    ?UNTIL((catch ets:info(Tid, size)) =:= 1),

    [{pool_name, T} | Config];
init_per_testcase(T, Config) ->
    pgo_sup:start_child(T, #{pool_size => 10,
                             database => ?DATABASE,
                             user => ?USER,
                             password => ?PASSWORD}),
    Tid = pgo_pool:tid(T),
    ?UNTIL((catch ets:info(Tid, size)) =:= 10),

    [{pool_name, T} | Config].

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

recheckout(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, _, Conn=#conn{socket=Socket}} = pgo:checkout(Name),

    ?UNTIL((catch ets:info(Tid, size)) =:= 0),

    pgo:with_conn(Conn, fun() ->
                                ?assertMatch(#{rows := [{1}]}, pgo:query("select 1", []))
                        end),

    erlang:exit(Socket, kill),
    pgo:with_conn(Conn, fun() ->
                                ?assertEqual({error, closed}, pgo:query("select 1", []))
                        end),

    %% wait for a new connection to be back in the pool
    ?UNTIL((catch ets:info(Tid, size)) =:= 1),
    ?assertMatch(#{rows := [{1}]}, pgo:query("select 1", [], #{pool => Name})),
    {ok, _Ref1={_, _, _, _Holder1}, Conn1} = pgo:checkout(Name),
    pgo:with_conn(Conn1, fun() ->
                                ?assertMatch(#{rows := [{1}]}, pgo:query("select 1", []))
                        end),

    ok.

kill_socket(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, Ref, Conn=#conn{socket=Socket}} = pgo:checkout(Name),

    ?UNTIL((catch ets:info(Tid, size)) =:= 0),

    pgo:with_conn(Conn, fun() ->
                                ?assertMatch(#{rows := [{1}]}, pgo:query("select 1", []))
                        end),

    %% check the conn back in before killing to test what happens if not checked out
    pgo:checkin(Ref, Conn),

    %% socket is killed, a new socket is created and checked in to the pool
    erlang:exit(Socket, kill),

    %% pool should go up to 2 until the next ping that catches the socket is dead
    ?UNTIL((catch ets:info(Tid, size)) =:= 2),

    ok.

kill_pid(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, Ref, Conn=#conn{owner=Pid}} = pgo:checkout(Name),

    ?UNTIL((catch ets:info(Tid, size)) =:= 0),

    pgo:with_conn(Conn, fun() ->
                                ?assertMatch(#{rows := [{1}]}, pgo:query("select 1", []))
                        end),

    %% check the conn back in before killing to test what happens if not checked out
    pgo:checkin(Ref, Conn),

    %% socket is killed, a new socket is created and checked in to the pool
    erlang:exit(Pid, kill),

    %% pool should go up to 2 until the old holder is removed
    ?UNTIL((catch ets:info(Tid, size)) =:= 2),

    ok.

checkout_kill(Config) ->
    Name = ?config(pool_name, Config),
    Tid = pgo_pool:tid(Name),

    {ok, _Ref={_, _, _, Holder}, Conn=#conn{socket=Socket}} = pgo:checkout(Name),
    {ok, {_, _, _, _}, Conn1=#conn{owner=Pid1}} = pgo:checkout(Name),

    ?UNTIL((catch ets:info(Tid, size)) =:= 8),

    pgo:with_conn(Conn, fun() ->
                                ?assertMatch(#{rows := [{1}]}, pgo:query("select 1", []))
                        end),

    ?assertMatch([_], ets:tab2list(Holder)),

    %% socket is killed, a new socket is created and checked in to the pool
    erlang:exit(Socket, kill),
    pgo:with_conn(Conn, fun() ->
                                ?assertEqual({error, closed}, pgo:query("select 1", []))
                        end),
    ?assertMatch([_], ets:tab2list(Holder)),

    %% pool goes back to 9
    ?UNTIL((catch ets:info(Tid, size)) =:= 9),

    %% old conn is still closed
    pgo:with_conn(Conn, fun() ->
                                ?assertEqual({error, closed}, pgo:query("select 1", []))
                        end),

    %% second checked out conn works
    pgo:with_conn(Conn1, fun() ->
                                 ?assertMatch(#{rows := [{1}]}, pgo:query("select 1", []))
                         end),

    %% process owning the socket is killed and pool size will then go to 10
    erlang:exit(Pid1, kill),

    %% process was killed, connection now fails
    pgo:with_conn(Conn1, fun() ->
                                 ?assertMatch({error, _}, pgo:query("select 1", []))
                         end),

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
