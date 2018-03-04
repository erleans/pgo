-module(pgo_SUITE).

-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0,

         checkout_checkin/1,
         checkout_break/1,
         checkout_kill/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DATABASE, "test").
-define(USER, "test").

-define(UNTIL(X), (fun Until(I) when I =:= 10 -> erlang:error(fail);
                       Until(I) -> case X of true -> ok; false -> timer:sleep(10), Until(I+1) end end)(0)).

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    Config.

end_per_suite(_Config) -> ok.

init_per_testcase(checkout_break, Config) ->
    Name = pool_break,
    pgo:start_pool(Name, [{size, 1},
                          {postgres, [{database, ?DATABASE},
                                      {user, ?USER}]}]),

    ?UNTIL(sbroker:len_r(Name, 1000) =:= 1),

    [{pool_name, Name} | Config];
init_per_testcase(checkout_kill, Config) ->
    Name = pool_kill,
    pgo:start_pool(Name, [{size, 1},
                          {postgres, [{database, ?DATABASE},
                                      {user, ?USER}]}]),

    ?UNTIL(sbroker:len_r(Name, 1000) =:= 1),

    [{pool_name, Name} | Config];
init_per_testcase(_, Config) ->
    Name = pool_1,
    pgo:start_pool(Name, [{size, 10},
                          {postgres, [{database, ?DATABASE},
                                      {user, ?USER}]}]),

    ?UNTIL(sbroker:len_r(Name, 1000) =:= 10),

    [{pool_name, Name} | Config].

end_per_testcase(_, _Config) ->
    ok.
    %% Name = ?config(pool_name, Config)
    %% ok = pgo:stop_pool(Name).

all() -> [checkout_checkin, checkout_break, checkout_kill].

checkout_checkin(Config) ->
    Name = ?config(pool_name, Config),

    {ok, _C1, Ref1} = pgo:checkout(Name),
    ?assertEqual(9, sbroker:len_r(Name, 1000)),

    pgo:checkin(Ref1),
    ?UNTIL(sbroker:len_r(Name, 1000) =:= 10),

    {ok, _C2, Ref2} = pgo:checkout(Name),
    ?assertEqual(9, sbroker:len_r(Name, 1000)),

    {ok, _C3, Ref3} = pgo:checkout(Name),
    ?assertEqual(8, sbroker:len_r(Name, 1000)),

    pgo:checkin(Ref2),
    pgo:checkin(Ref3),

    ?UNTIL(sbroker:len_r(Name, 1000) =:= 10),
    ok.

checkout_break(Config) ->
    Name = ?config(pool_name, Config),

    {ok, C1, Ref1} = pgo:checkout(Name),
    pgo:break(Ref1),

    ?UNTIL(sbroker:len_r(Name, 1000) =:= 1),
    {ok, C2, _Ref2} = pgo:checkout(Name),

    %% verify that the connection we broke is not still in the pool
    ?assertNotEqual(C1, C2),

    ok.

checkout_kill(Config) ->
    Name = ?config(pool_name, Config),

    {ok, C1, _Ref1} = pgo:checkout(Name),
    erlang:exit(C1, kill),

    ?UNTIL(sbroker:len_r(Name, 1000) =:= 1),

    {ok, C2, _Ref2} = pgo:checkout(Name),

    %% verify that the connection we broke is not still in the pool
    ?assertNotEqual(C1, C2),

    ok.
