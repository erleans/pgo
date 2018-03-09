-module(pgo_test).

-include_lib("eunit/include/eunit.hrl").
-include("pgo.hrl").

%%%% CREATE ROLE test LOGIN;
%%%% ALTER USER test WITH SUPERUSER
%%%%
%%%% CREATE DATABASE test WITH OWNER=test;
%%%%

-define(UNTIL(X), (fun Until(I) when I =:= 10 -> erlang:error(fail);
                       Until(I) -> case X of true -> ok; false -> timer:sleep(10), Until(I+1) end end)(0)).

kill_sup(SupPid) ->
    OldTrapExit = process_flag(trap_exit, true),
    exit(SupPid, normal),
    receive {'EXIT', SupPid, _Reason} -> ok after 5000 -> throw({error, timeout}) end,
    process_flag(trap_exit, OldTrapExit).

start_pool() ->
    pgo_query_cache:start_link(),
    {ok, Pid} = pgo_pool:start_link(default, [{size, 1}, {database, "test"}, {user, "test"}]),
    Tid = pgo_pool:tid(default),
    ?UNTIL((catch ets:info(Tid, size)) =:= 1),
    {ok, Pid}.

bad_query_parse_test_() ->
    {setup,
     fun() ->
             {ok, PoolPid} = start_pool(),
             PoolPid
     end,
     fun(PoolPid) ->
             kill_sup(PoolPid)
     end,
     fun(_) ->
             [
              ?_assertMatch({error, _}, pgo:query(default, "select $1", []))
             ]
     end}.

select_null_test_() ->
    {setup,
     fun() ->
             {ok, PoolPid} = start_pool(),
             PoolPid
     end,
     fun(PoolPid) ->
             kill_sup(PoolPid)
     end,
     fun(_) ->
             [
              ?_assertMatch(#pg_result{rows=[{null}]}, pgo:query(default, "select null")),
              ?_assertMatch(#pg_result{rows=[{null}]}, pgo:query(default, "select null", [])),
              ?_assertMatch(#pg_result{rows=[{null}]}, pgo:query(default, "select null")),
              ?_assertMatch(#pg_result{rows=[{null}]}, pgo:query(default, "select null", []))
             ]
     end}.

sql_query_test_() ->
    {setup,
     fun() ->
             {ok, PoolPid} = start_pool(),
             PoolPid
     end,
     fun(PoolPid) ->
             pgo:query(default, "drop table foo"),
             kill_sup(PoolPid)
     end,
     fun(_) ->
             [
              {"Create table",
               ?_assertMatch(#pg_result{command=create},
                             pgo:query(default, "create table foo (id integer primary key, some_text text)"))
              },
              {"Insert into",
               ?_assertMatch(#pg_result{command=insert},
                             pgo:query(default, "insert into foo (id, some_text) values (1, 'hello')"))
              },
              {"Update",
               ?_assertMatch(#pg_result{command=update},
                             pgo:query(default, "update foo set some_text = 'hello world'"))
              },
              {"Insert into",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into foo (id, some_text) values (2, 'hello again')"))
              },
              {"Update on matching condition",
               ?_assertMatch(#pg_result{}, pgo:query(default, "update foo set some_text = 'hello world' where id = 1"))
              },
              {"Update on non-matching condition",
               ?_assertMatch(#pg_result{}, pgo:query(default, "update foo set some_text = 'goodbye, all' where id = 3"))
              },
              {"Select *",
               ?_assertMatch(#pg_result{rows=[{1, <<"hello world">>}, {2, <<"hello again">>}]},
                             pgo:query(default, "select * from foo order by id asc"))
              },
              {"Select with named columns",
               ?_assertMatch(#pg_result{rows=[{1, <<"hello world">>}, {2, <<"hello again">>}]},
                             pgo:query(default, "select id as the_id, some_text as the_text from foo order by id asc"))
              },
              {"Select with inverted columns",
               ?_assertMatch(#pg_result{rows=[{<<"hello world">>, 1}, {<<"hello again">>, 2}]},
                             pgo:query(default, "select some_text, id from foo order by id asc"))
              },
              {"Select with matching condition",
               ?_assertMatch(#pg_result{rows=[{<<"hello again">>}]}, pgo:query(default, "select some_text from foo where id = 2"))
              },
              {"Select with non-matching condition",
               ?_assertMatch(#pg_result{}, pgo:query(default, "select * from foo where id = 3"))
              }
             ]
     end}.

types_test_() ->
    {setup,
     fun() ->
             {ok, PoolPid} = start_pool(),
             PoolPid
     end,
     fun(PoolPid) ->
             pgo:query(default, "drop table types"),
             kill_sup(PoolPid)
     end,
     fun(_) ->
             [
              {"Create table for the types",
               ?_assertMatch(#pg_result{command=create},
                             pgo:query(default, "create table types (id integer primary key, an_integer integer, a_bigint bigint, a_text text, a_uuid uuid, a_bytea bytea, a_real real)"))
              },
              {"Insert nulls (literal)",
               ?_assertMatch(#pg_result{command=insert},
                             pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (1, null, null, null, null, null, null)"))
              },
              {"Select nulls (1)",
               ?_assertMatch(#pg_result{rows=[{1, null, null, null, null, null, null}]},
                             pgo:query(default, "select * from types where id = 1"))
              },
              {"Insert nulls (params)",
               ?_assertMatch(#pg_result{command=insert},
                             pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)", [2, null, null, null, null, null, null]))
              },
              {"Select nulls (2)",
               ?_assertMatch(#pg_result{rows=[{2, null, null, null, null, null, null}]},
                             pgo:query(default, "select * from types where id = 2"))
              },
              {"Insert integer",
               ?_assertMatch(#pg_result{command=insert},
                             pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                       [3, 42, null, null, null, null, null]))
              },
              {"Insert bigint",
               ?_assertMatch(#pg_result{command=insert},
                             pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                       [4, null, 1099511627776, null, null, null, null]))
              },
              {"Insert text (binary)",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [6, null, null, <<"And in the end, the love you take is equal to the love you make">>, null, null, null]))
              },
              {"Insert uuid",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [7, null, null, null, <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>, null, null]))
              },
              {"Insert bytea",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [8, null, null, null, null, <<"deadbeef">>, null]))
              },
              {"Insert float",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [9, null, null, null, null, null, 3.1415]))
              },
              {"Insert float",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [19, null, null, null, null, null, 3.0]))
              },
              {"Insert all",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>, <<"deadbeef">>, 3.1415]))
              },
              {"Select values (10)",
               ?_test(begin
                          R = pgo:query(default, "select * from types where id = 10"),
                          ?assertMatch(#pg_result{command=select, rows=[_Row]}, R),
                          #pg_result{command=select, rows=[Row]} = R,
                          ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row),
                          {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID, <<"deadbeef">>, Float} = Row,
                          ?assertMatch(<<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, UUID),
                          ?assert(Float > 3.1413),
                          ?assert(Float < 3.1416)
                      end)
              },
              {"Select values (10) (with bind)",
               ?_test(begin
                          R = pgo:query(default, "select * from types where id = $1", [10]),
                          ?assertMatch(#pg_result{command=select, rows=[_Row]}, R),
                          #pg_result{command=select, rows=[Row]} = R,
                          ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row),
                          {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID, <<"deadbeef">>, Float} = Row,
                          ?assertMatch(<<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, UUID),
                          ?assert(Float > 3.1413),
                          ?assert(Float < 3.1416)
                      end)
              },
              {"Insert bytea",
               ?_assertMatch(#pg_result{command=insert},
                             pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                       [11, null, null, null, null, <<"deadbeef">>, null]))
              },
              {"Insert with returning",
               ?_assertMatch(#pg_result{command=insert, rows=[{15}]},
                             pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
                                       [15, null, null, null, null, <<"deadbeef">>, null]))
              },
              {"Select values (11)",
               ?_test(begin
                          R = pgo:query(default, "select * from types where id = $1", [11]),
                          ?assertMatch(#pg_result{command=select, rows=[_Row]}, R),
                          #pg_result{command=select, rows=[Row]} = R,
                          ?assertMatch({11, null, null, null, null, <<"deadbeef">>, null}, Row)
                      end)
              },
              {"Insert uuid 0",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [199, null, null, null, <<"00000000-0000-0000-0000-000000000000">>, null, null]))
              },
              {"Insert uuid in lowercase",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [16, null, null, null, <<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, null, null]))
              },
              {"Insert uc uuid in text column",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [17, null, null, <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>, null, null, null]))
              },
              {"Insert lc uuid in text column",
               ?_assertMatch(#pg_result{command=insert}, pgo:query(default, "insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                                   [18, null, null, <<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, null, null, null]))
              },
              {"Select text uuid (17 \& 18)",
               ?_test(begin
                          R = pgo:query(default, "select a_text from types where id IN ($1, $2) order by id", [17, 18]),
                          ?assertMatch(#pg_result{command=select, rows=[_Row17, _Row18]}, R),
                          #pg_result{command=select, rows=[Row17, Row18]} = R,
                          ?assertMatch({<<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>}, Row17),
                          ?assertMatch({<<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>}, Row18)
                      end)
              }
             ]
     end}.

text_types_test_() ->
    {setup,
     fun() ->
             {ok, PoolPid} = start_pool(),
             PoolPid
     end,
     fun(PoolPid) ->
             kill_sup(PoolPid)
     end,
     fun(_) ->
             [
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo">>}]}, pgo:query("select 'foo'::text")),
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo">>}]}, pgo:query("select $1::text", [<<"foo">>])),
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo         ">>}]}, pgo:query("select 'foo'::char(12)")),
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo         ">>}]}, pgo:query("select $1::char(12)", [<<"foo">>])),
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo">>}]}, pgo:query("select 'foo'::varchar(12)")),
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo">>}]}, pgo:query("select $1::varchar(12)", [<<"foo">>])),
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo">>}]}, pgo:query("select 'foobar'::char(3)")),
              ?_assertMatch(#pg_result{command=select,rows=[{<<"foo">>}]}, pgo:query("select $1::char(3)", [<<"foobar">>]))
             ]
     end
    }.

%% array_types_test_() ->
%%     {setup,
%%      fun() ->
%%              {ok, SupPid} = pgo_sup:start_link(),
%%              {ok, PoolPid} = start_pool(),
%%              PoolPid
%%      end,
%%      fun(PoolPid) ->
%%              supervisor:terminate_child(pgo_sup, PoolPid),
%%              kill_sup(PoolPid)
%%      end,
%%      fun(_) ->
%%              [
%% ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:query("select '{2,3}'::text[]", Conn)),
%% ?_assertEqual({{select,1},[{{array,[2,3]}}]}, pgsql_connection:query("select '{2,3}'::int[]", Conn)),
%% ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:query("select '{}'::text[]", Conn)),
%% ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:query("select '{}'::int[]", Conn)),
%% ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:query("select ARRAY[]::text[]", Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::text[]", ["{\"2\", \"3\"}"], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, ["2", "3"]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2">>, <<"3">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2,3">>,<<"4">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2,3">>, <<"4">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2,,3">>,<<"4">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2,,3">>, <<"4">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2\"3">>,<<"4">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2\"3">>, <<"4">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2\",,\"3">>,<<"4">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2\",,\"3">>, <<"4">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2'3">>,<<"4">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2'3">>, <<"4">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2\\3">>,<<"4">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2\\3">>, <<"4">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::bytea[]", [{array, [<<"2">>, <<"3">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2  ">>,<<"3  ">>]}}]}, pgsql_connection:extended_query("select $1::char(3)[]", [{array, [<<"2">>, <<"3">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::varchar(3)[]", [{array, [<<"2">>, <<"3">>]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[{array,[<<"2">>]},{array, [<<"3">>]}]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [{array, [<<"2">>]}, {array, [<<"3">>]}]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[{array,[1,2]},{array, [3,4]}]}}]}, pgsql_connection:extended_query("select $1::int[]", [{array, [{array, [1,2]}, {array, [3,4]}]}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:extended_query("select '{}'::text[]", [], Conn)),
%% ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:extended_query("select '{}'::int[]", [], Conn)),
%% ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:extended_query("select ARRAY[]::text[]", [], Conn)),

%% ?_assertEqual({{select,1},[{{array,[{array,[<<"2">>]},{array, [<<"3">>]}]}}]}, pgsql_connection:query("select '{{\"2\"}, {\"3\"}}'::text[][]", Conn)),
%% ?_assertEqual({{select,1},[{{array,[{array,[1,2]}, {array, [3,4]}]}}]}, pgsql_connection:query("select ARRAY[ARRAY[1,2], ARRAY[3,4]]", Conn)),
%% ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:extended_query("select $1::bytea[]", [{array, []}], Conn)),
%% ?_assertEqual({{select,1},[{{array,[]},{array,[<<"foo">>]}}]}, pgsql_connection:extended_query("select $1::bytea[], $2::bytea[]", [{array, []}, {array, [<<"foo">>]}], Conn)),

%% ?_assertEqual({{select,1},[{{array,[1,2]}}]}, pgsql_connection:query("select ARRAY[1,2]::int[]", Conn)),
%% {timeout, 20, ?_test(
%%     begin
%%             {{create, table}, []} = pgsql_connection:query("create table tmp (id integer primary key, ints integer[])", Conn),
%%             Array = lists:seq(1,1000000),
%%             R = pgsql_connection:extended_query("insert into tmp(id, ints) values($1, $2)", [1, {array, Array}], Conn),
%%             ?assertEqual({{insert, 0, 1}, []}, R)
%%     end)},
%% ?_test(
%%     begin
%%             {{create, table}, []} = pgsql_connection:query("create table tmp2 (id integer primary key, bins bytea[])", Conn),
%%             R = pgsql_connection:extended_query("insert into tmp2(id, bins) values($1, $2)", [1, {array, [<<2>>, <<3>>]}], Conn),
%%             ?assertEqual({{insert, 0, 1}, []}, R),
%%             R2 = pgsql_connection:extended_query("insert into tmp2(id, bins) values($1, $2)", [2, {array, [<<16#C2,1>>]}], Conn),
%%             ?assertEqual({{insert, 0, 1}, []}, R2),
%%             R3 = pgsql_connection:extended_query("insert into tmp2(id, bins) values($1, $2)", [3, {array, [<<2,0,3>>, <<4>>]}], Conn),
%%             ?assertEqual({{insert, 0, 1}, []}, R3)
%%     end)
%%      ]
%%     end
%% }.

%% % https://github.com/semiocast/pgsql/issues/28
%% quoted_array_values_test_() ->
%%     {setup,
%%         fun() ->
%%                 {ok, SupPid} = saap_sup:start_link(),
%%                 Conn = pgsql_connection:open("test", "test"),
%%                 {SupPid, Conn}
%%         end,
%%         fun({SupPid, Conn}) ->
%%                 pgsql_connection:close(Conn),
%%                 kill_sup(PoolPid)
%%         end,
%%         fun({_SupPid, Conn}) ->
%%                 [
%%                     ?_assertEqual({{select,1},[{{array,[<<"foo bar">>]}}]}, pgsql_connection:query("select ARRAY['foo bar'];", Conn)),
%%                     ?_assertEqual({{select,1},[{{array,[<<"foo, bar">>]}}]}, pgsql_connection:query("select ARRAY['foo, bar'];", Conn)),
%%                     ?_assertEqual({{select,1},[{{array,[<<"foo} bar">>]}}]}, pgsql_connection:query("select ARRAY['foo} bar'];", Conn)),
%%                     ?_assertEqual({{select,1},[{{array,[<<"foo \" bar">>]}}]}, pgsql_connection:query("select ARRAY['foo \" bar'];", Conn)),
%%                     ?_assertEqual({{select,1},[{{array,[{{2014,1,1},{12,12,12}}]}}]}, pgsql_connection:query("select ARRAY['2014-01-01T12:12:12Z'::timestamp];", Conn))
%%                 ]
%%         end
%%     }.

%% geometric_types_test_() ->
%%     [{setup,
%%       fun() ->
%%               {ok, SupPid} = saap_sup:start_link(),
%%               Conn = pgsql_connection:open("test", "test"),
%%               {SupPid, Conn}
%%       end,
%%       fun({SupPid, Conn}) ->
%%               pgsql_connection:close(Conn),
%%               kill_sup(PoolPid)
%%       end,
%%       fun({_SupPid, Conn}) ->
%%               [
%%                ?_assertEqual({{select,1},[{{point,{2.0,-3.0}}}]}, pgsql_connection:query("select '(2,-3)'::point", Conn)),
%%                ?_assertEqual({{select,1},[{{point,{2.0,1.45648}}}]}, pgsql_connection:query("select '(2,1.45648)'::point", Conn)),
%%                ?_assertEqual({{select,1},[{{point,{-3.154548,-3.0}}}]}, pgsql_connection:query("select '(-3.154548,-3)'::point", Conn)),
%%                ?_assertEqual({{select,1},[{{point,{-3.154548,1.45648}}}]}, pgsql_connection:query("select '(-3.154548,1.45648)'::point", Conn)),
%%                ?_assertEqual({{select,1},[{{point,{2.0,-3.0}}}]}, pgsql_connection:extended_query("select '(2,-3)'::point", [], Conn)),
%%                ?_assertEqual({{select,1},[{{point,{2.0,1.45648}}}]}, pgsql_connection:extended_query("select '(2,1.45648)'::point", [], Conn)),
%%                ?_assertEqual({{select,1},[{{point,{-3.154548,-3.0}}}]}, pgsql_connection:extended_query("select '(-3.154548,-3)'::point", [], Conn)),
%%                ?_assertEqual({{select,1},[{{point,{-3.154548,1.45648}}}]}, pgsql_connection:extended_query("select '(-3.154548,1.45648)'::point", [], Conn)),

%%                ?_assertEqual({{select,1},[{{lseg,{2.0,1.45648},{-3.154548,-3.0}}}]}, pgsql_connection:query("select '[(2,1.45648),(-3.154548,-3)]'::lseg", Conn)),
%%                ?_assertEqual({{select,1},[{{lseg,{2.0,1.45648},{-3.154548,-3.0}}}]}, pgsql_connection:extended_query("select '[(2,1.45648),(-3.154548,-3))'::lseg", [], Conn)),

%%                ?_assertEqual({{select,1},[{{box,{2.0,1.45648},{-3.154548,-3.0}}}]}, pgsql_connection:query("select '((-3.154548,-3),(2,1.45648))'::box", Conn)),
%%                ?_assertEqual({{select,1},[{{box,{2.0,1.45648},{-3.154548,-3.0}}}]}, pgsql_connection:extended_query("select '((-3.154548,-3),(2,1.45648))'::box", [], Conn)),

%%                ?_assertEqual({{select,1},[{{polygon,[{-3.154548,-3.0},{2.0,1.45648}]}}]}, pgsql_connection:query("select '((-3.154548,-3),(2,1.45648))'::polygon", Conn)),
%%                ?_assertEqual({{select,1},[{{polygon,[{-3.154548,-3.0},{2.0,1.45648}]}}]}, pgsql_connection:extended_query("select '((-3.154548,-3),(2,1.45648))'::polygon", [], Conn)),

%%                ?_assertEqual({{select,1},[{{path,closed,[{-3.154548,-3.0},{2.0,1.45648}]}}]}, pgsql_connection:query("select '((-3.154548,-3),(2,1.45648))'::path", Conn)),
%%                ?_assertEqual({{select,1},[{{path,closed,[{-3.154548,-3.0},{2.0,1.45648}]}}]}, pgsql_connection:extended_query("select '((-3.154548,-3),(2,1.45648))'::path", [], Conn)),

%%                ?_assertEqual({{select,1},[{{path,open,[{-3.154548,-3.0},{2.0,1.45648}]}}]}, pgsql_connection:query("select '[(-3.154548,-3),(2,1.45648)]'::path", Conn)),
%%                ?_assertEqual({{select,1},[{{path,open,[{-3.154548,-3.0},{2.0,1.45648}]}}]}, pgsql_connection:extended_query("select '[(-3.154548,-3),(2,1.45648)]'::path", [], Conn)),

%%                {setup,
%%                 fun() ->
%%                         {updated, 1} = pgo:query(default, "create table tmp (id integer primary key, mypoint point, mylseg lseg, mybox box, mypath path, mypolygon polygon)", Conn),
%%                         ok
%%                 end,
%%                 fun(_) ->
%%                         ok
%%                 end,
%%                 fun(_) ->
%%                         [
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{1, {point,{2.0,3.0}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypoint) values($1, $2) returning id, mypoint", [1, {point,{2,3}}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{2, {point,{-10.0,3.254}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypoint) values($1, $2) returning id, mypoint", [2, {point,{-10,3.254}}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{3, {point,{-10.0,-3.5015}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypoint) values($1, $2) returning id, mypoint", [3, {point,{-10,-3.5015}}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{4, {point,{2.25,-3.59}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypoint) values($1, $2) returning id, mypoint", [4, {point,{2.25,-3.59}}], Conn)
%%                            ),

%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{101, {lseg,{2.54,3.14},{-10.0,-3.5015}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mylseg) values($1, $2) returning id, mylseg", [101, {lseg,{2.54,3.14},{-10,-3.5015}}], Conn)
%%                            ),

%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{201, {box,{2.0,3.0},{-10.14,-3.5015}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mybox) values($1, $2) returning id, mybox", [201, {box,{2,3},{-10.14,-3.5015}}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{202, {box,{2.0,3.0},{-10.14,-3.5015}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mybox) values($1, $2) returning id, mybox", [202, {box,{-10.14,3},{2,-3.5015}}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{203, {box,{2.0,3.0},{-10.14,-3.5015}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mybox) values($1, $2) returning id, mybox", [203, {box,{2,-3.5015},{-10.14,3}}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{204, {box,{2.0,3.0},{-10.14,-3.5015}}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mybox) values($1, $2) returning id, mybox", [204, {box,{-10.14,-3.5015},{2,3}}], Conn)
%%                            ),

%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{301, {path,open,[{-10.85,-3.5015}]}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypath) values($1, $2) returning id, mypath", [301, {path,open,[{-10.85,-3.5015}]}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{302, {path,open,[{-10.85,-3.5015},{2.0,3.0}]}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypath) values($1, $2) returning id, mypath", [302, {path,open,[{-10.85,-3.5015},{2,3}]}], Conn)
%%                            ),

%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{351, {path,closed,[{-10.85,-3.5015}]}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypath) values($1, $2) returning id, mypath", [351, {path,closed,[{-10.85,-3.5015}]}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{352, {path,closed,[{-10.85,-3.5015},{2.0,3.0}]}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypath) values($1, $2) returning id, mypath", [352, {path,closed,[{-10.85,-3.5015},{2,3}]}], Conn)
%%                            ),

%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{401, {polygon,[{-10.85,-3.5015}]}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypolygon) values($1, $2) returning id, mypolygon", [401, {polygon,[{-10.85,-3.5015}]}], Conn)
%%                            ),
%%                          ?_assertEqual(
%%                             {{insert, 0, 1}, [{402, {polygon,[{-10.85,-3.5015},{2.0,3.0}]}}]},
%%                             pgsql_connection:extended_query("insert into tmp(id, mypolygon) values($1, $2) returning id, mypolygon", [402, {polygon,[{-10.85,-3.5015},{2,3}]}], Conn)
%%                            )
%%                         ]
%%                 end
%%                }
%%               ]
%%       end
%%      },
%%      {setup,
%%       fun() ->
%%               {ok, SupPid} = saap_sup:start_link(),
%%               Conn = pgsql_connection:open("test", "test"),
%%               {updated, 1} = pgo:query(pool_1, "create table tmp (id integer primary key, mypath path)", Conn),
%%               {SupPid, Conn}
%%       end,
%%       fun({SupPid, Conn}) ->
%%               pgsql_connection:close(Conn),
%%               kill_sup(PoolPid)
%%       end,
%%       fun({_SupPid, Conn}) ->
%%               ?_assertMatch(
%%                   {error, {badarg, {path,open,[]}}},
%%                   pgsql_connection:extended_query("insert into tmp(id, mypath) values($1, $2) returning id, mypath", [300, {path,open,[]}], Conn)
%%               )
%%       end},
%%      {setup,
%%       fun() ->
%%               {ok, SupPid} = saap_sup:start_link(),
%%               Conn = pgsql_connection:open("test", "test"),
%%               {updated, 1} = pgo:query(pool_1, "create table tmp (id integer primary key, mypath path)", Conn),
%%               {SupPid, Conn}
%%       end,
%%       fun({SupPid, Conn}) ->
%%               pgsql_connection:close(Conn),
%%               kill_sup(PoolPid)
%%       end,
%%       fun({_SupPid, Conn}) ->
%%               ?_assertMatch(
%%                   {error, {badarg, {path,closed,[]}}},
%%                   pgsql_connection:extended_query("insert into tmp(id, mypath) values($1, $2) returning id, mypath", [350, {path,closed,[]}], Conn)
%%               )
%%       end},
%%      {setup,
%%       fun() ->
%%               {ok, SupPid} = saap_sup:start_link(),
%%               Conn = pgsql_connection:open("test", "test"),
%%               {updated, 1} = pgo:query(pool_1, "create table tmp (id integer primary key, mypolygon polygon)", Conn),
%%               {SupPid, Conn}
%%       end,
%%       fun({SupPid, Conn}) ->
%%               pgsql_connection:close(Conn),
%%               kill_sup(PoolPid)
%%       end,
%%       fun({_SupPid, Conn}) ->
%%               ?_assertMatch(
%%                   {error, {badarg, {polygon, []}}},
%%                   pgsql_connection:extended_query("insert into tmp(id, mypolygon) values($1, $2) returning id, mypolygon", [400, {polygon,[]}], Conn)
%%               )
%%       end}
%%     ].

%% float_types_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_assertEqual({selected, [{1.0}]}, pgo:query(pool_1, "select 1.0::float4", Conn)),
%%         ?_assertEqual({selected, [{1.0}]}, pgsql_connection:sql_query("select 1.0::float8", Conn)),
%%         ?_assertEqual({selected, [{1.0}]}, pgsql_connection:param_query("select 1.0::float4", [], Conn)),
%%         ?_assertEqual({selected, [{1.0}]}, pgsql_connection:param_query("select 1.0::float8", [], Conn)),

%%         ?_assertEqual({selected, [{3.14159}]}, pgsql_connection:sql_query("select 3.141592653589793::float4", Conn)),
%%         ?_assertEqual({selected, [{3.14159265358979}]}, pgsql_connection:sql_query("select 3.141592653589793::float8", Conn)),
%%         ?_assertEqual({selected, [{3.1415927410125732}]}, pgsql_connection:param_query("select 3.141592653589793::float4", [], Conn)),
%%         ?_assertEqual({selected, [{3.141592653589793}]}, pgsql_connection:param_query("select 3.141592653589793::float8", [], Conn)),

%%         ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:sql_query("select 'NaN'::float4", Conn)),
%%         ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:sql_query("select 'NaN'::float8", Conn)),
%%         ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:param_query("select 'NaN'::float4", [], Conn)),
%%         ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:param_query("select 'NaN'::float8", [], Conn)),

%%         ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:sql_query("select 'Infinity'::float4", Conn)),
%%         ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:sql_query("select 'Infinity'::float8", Conn)),
%%         ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:param_query("select 'Infinity'::float4", [], Conn)),
%%         ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:param_query("select 'Infinity'::float8", [], Conn)),

%%         ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:sql_query("select '-Infinity'::float4", Conn)),
%%         ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:sql_query("select '-Infinity'::float8", Conn)),
%%         ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:param_query("select '-Infinity'::float4", [], Conn)),
%%         ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:param_query("select '-Infinity'::float8", [], Conn))
%%     ]
%%     end}.

%% boolean_type_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_assertEqual({selected, [{true}]}, pgsql_connection:sql_query("select true::boolean", Conn)),
%%         ?_assertEqual({selected, [{false}]}, pgsql_connection:sql_query("select false::boolean", Conn)),
%%         ?_assertEqual({selected, [{true}]}, pgsql_connection:param_query("select true::boolean", [], Conn)),
%%         ?_assertEqual({selected, [{false}]}, pgsql_connection:param_query("select false::boolean", [], Conn))
%%     ]
%%     end}.

%% null_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select null", Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select null", [], Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select null::int2", Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select null::int2", [], Conn))
%%     ]
%%     end}.

%% integer_types_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_assertEqual({selected, [{127}]}, pgsql_connection:sql_query("select 127::int2", Conn)),
%%         ?_assertEqual({selected, [{-126}]}, pgsql_connection:sql_query("select -126::int2", Conn)),
%%         ?_assertEqual({selected, [{127}]}, pgsql_connection:sql_query("select 127::int4", Conn)),
%%         ?_assertEqual({selected, [{-126}]}, pgsql_connection:sql_query("select -126::int4", Conn)),
%%         ?_assertEqual({selected, [{127}]}, pgsql_connection:sql_query("select 127::int8", Conn)),
%%         ?_assertEqual({selected, [{-126}]}, pgsql_connection:sql_query("select -126::int8", Conn)),
%%         ?_assertEqual({selected, [{127}]}, pgsql_connection:param_query("select 127::int2", [], Conn)),
%%         ?_assertEqual({selected, [{-126}]}, pgsql_connection:param_query("select -126::int2", [], Conn)),
%%         ?_assertEqual({selected, [{127}]}, pgsql_connection:param_query("select 127::int4", [], Conn)),
%%         ?_assertEqual({selected, [{-126}]}, pgsql_connection:param_query("select -126::int4", [], Conn)),
%%         ?_assertEqual({selected, [{127}]}, pgsql_connection:param_query("select 127::int8", [], Conn)),
%%         ?_assertEqual({selected, [{-126}]}, pgsql_connection:param_query("select -126::int8", [], Conn))
%%     ]
%%     end}.

%% % Numerics can be either integers or floats.
%% numeric_types_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         % text values (query)
%%         ?_assertEqual({{select, 1}, [{127}]}, pgsql_connection:query("select 127::numeric", Conn)),
%%         ?_assertEqual({{select, 1}, [{-126}]}, pgsql_connection:query("select -126::numeric", Conn)),
%%         ?_assertEqual({{select, 1}, [{123456789012345678901234567890}]}, pgsql_connection:query("select 123456789012345678901234567890::numeric", Conn)),
%%         ?_assertEqual({{select, 1}, [{-123456789012345678901234567890}]}, pgsql_connection:query("select -123456789012345678901234567890::numeric", Conn)),
%%         ?_assertEqual({{select, 1}, [{'NaN'}]}, pgsql_connection:query("select 'NaN'::numeric", Conn)),
%%         ?_assertEqual({{select, 1}, [{123456789012345678901234.567890}]}, pgsql_connection:query("select 123456789012345678901234.567890::numeric", Conn)),
%%         ?_assertEqual({{select, 1}, [{-123456789012345678901234.567890}]}, pgsql_connection:query("select -123456789012345678901234.567890::numeric", Conn)),
%%         ?_assertEqual({{select, 1}, [{1000000.0}]}, pgsql_connection:query("select 1000000.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{10000.0}]}, pgsql_connection:query("select 10000.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{100.0}]}, pgsql_connection:query("select 100.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{1.0}]}, pgsql_connection:query("select 1.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.0}]}, pgsql_connection:query("select 0.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.1}]}, pgsql_connection:query("select 0.1::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.00001}]}, pgsql_connection:query("select 0.00001::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.0000001}]}, pgsql_connection:query("select 0.0000001::numeric", [], Conn)),

%%         % binary values (extended_query)
%%         ?_assertEqual({{select, 1}, [{127}]}, pgsql_connection:extended_query("select 127::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{-126}]}, pgsql_connection:extended_query("select -126::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{123456789012345678901234567890}]}, pgsql_connection:extended_query("select 123456789012345678901234567890::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{-123456789012345678901234567890}]}, pgsql_connection:extended_query("select -123456789012345678901234567890::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{'NaN'}]}, pgsql_connection:extended_query("select 'NaN'::numeric", [], Conn)),
%%         ?_test(begin
%%             {{select, 1}, [{Val}]} = pgsql_connection:extended_query("select 123456789012345678901234.567890::numeric", [], Conn),
%%             ?assert(Val > 123456789012345500000000.0),
%%             ?assert(Val < 123456789012345700000000.0)
%%         end),
%%         ?_test(begin
%%             {{select, 1}, [{Val}]} = pgsql_connection:extended_query("select -123456789012345678901234.567890::numeric", [], Conn),
%%             ?assert(Val > -123456789012345700000000.0),
%%             ?assert(Val < -123456789012345500000000.0)
%%         end),
%%         ?_assertEqual({{select, 1}, [{1000000.0}]}, pgsql_connection:extended_query("select 1000000.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{10000.0}]}, pgsql_connection:extended_query("select 10000.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{100.0}]}, pgsql_connection:extended_query("select 100.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{1.0}]}, pgsql_connection:extended_query("select 1.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.0}]}, pgsql_connection:extended_query("select 0.0::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.1}]}, pgsql_connection:extended_query("select 0.1::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.00001}]}, pgsql_connection:extended_query("select 0.00001::numeric", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{0.0000001}]}, pgsql_connection:extended_query("select 0.0000001::numeric", [], Conn))
%%     ]
%%     end}.

%% datetime_types_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("127.0.0.1", "test", "test", "", [{timezone, "UTC"}]),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_assertEqual({selected, [{{2012,1,17}}]},    pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::date", Conn)),
%%         ?_assertEqual({selected, [{{10,54,3}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03'::time", Conn)),
%%         ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::time", Conn)),
%%         ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::timetz", Conn)),
%%         ?_assertEqual({selected, [{{{2012,1,17},{10,54,3}}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03'::timestamp", Conn)),
%%         ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::timestamp", Conn)),
%%         ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::timestamptz", Conn)),
%%         ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '1972-01-17 10:54:03.45'::timestamp", Conn)),
%%         ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '1972-01-17 10:54:03.45'::timestamptz", Conn)),
%%         ?_assertEqual({selected, [{{1970,1,1}}]},   pgsql_connection:sql_query("select 'epoch'::date", Conn)),
%%         ?_assertEqual({selected, [{{0,0,0}}]},   pgsql_connection:sql_query("select 'allballs'::time", Conn)),
%%         ?_assertEqual({selected, [{infinity}]},   pgsql_connection:sql_query("select 'infinity'::timestamp", Conn)),
%%         ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:sql_query("select '-infinity'::timestamp", Conn)),
%%         ?_assertEqual({selected, [{infinity}]},   pgsql_connection:sql_query("select 'infinity'::timestamptz", Conn)),
%%         ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:sql_query("select '-infinity'::timestamptz", Conn)),

%%         ?_assertEqual({selected, [{{2012,1,17}}]},    pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::date", [], Conn)),
%%         ?_assertEqual({selected, [{{10,54,3}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03'::time", [], Conn)),
%%         ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::time", [], Conn)),
%%         ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::timetz", [], Conn)),
%%         ?_assertEqual({selected, [{{{2012,1,17},{10,54,3}}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03'::timestamp", [], Conn)),
%%         ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::timestamp", [], Conn)),
%%         ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::timestamptz", [], Conn)),
%%         ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '1972-01-17 10:54:03.45'::timestamp", [], Conn)),
%%         ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '1972-01-17 10:54:03.45'::timestamptz", [], Conn)),
%%         ?_assertEqual({selected, [{{1970,1,1}}]},   pgsql_connection:param_query("select 'epoch'::date", [], Conn)),
%%         ?_assertEqual({selected, [{{0,0,0}}]},   pgsql_connection:param_query("select 'allballs'::time", [], Conn)),
%%         ?_assertEqual({selected, [{infinity}]},   pgsql_connection:param_query("select 'infinity'::timestamp", [], Conn)),
%%         ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:param_query("select '-infinity'::timestamp", [], Conn)),
%%         ?_assertEqual({selected, [{infinity}]},   pgsql_connection:param_query("select 'infinity'::timestamptz", [], Conn)),
%%         ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:param_query("select '-infinity'::timestamptz", [], Conn)),

%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3}}}]},   pgsql_connection:extended_query("select $1::timestamptz", [{{2012,1,17},{10,54,3}}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{2012,1,17}}]},   pgsql_connection:extended_query("select $1::date", [{2012,1,17}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{10,54,3}}]},   pgsql_connection:extended_query("select $1::time", [{10,54,3}], Conn)),

%%         {"Create table for the times", ?_assertEqual({updated, 1}, pgsql_connection:sql_query("create table times (a_timestamp timestamp, a_time time)", Conn))},
%%         {"Insert timestamp with micro second resolution",
%%             ?_assertEqual({{insert, 0, 1}, []}, pgsql_connection:extended_query("insert into times (a_timestamp, a_time) values ($1, $2)", [{{2014, 5, 15}, {12, 12, 12.999999}}, null], Conn))
%%         },
%%         {"Insert timestamp without micro second resolution",
%%             ?_assertEqual({{insert, 0, 1}, []}, pgsql_connection:extended_query("insert into times (a_timestamp, a_time) values ($1, $2)", [{{2014, 5, 15}, {12, 12, 12}}, null], Conn))
%%         },
%%         {"Insert a time with micro second resolution",
%%             ?_assertEqual({{insert, 0, 1}, []}, pgsql_connection:extended_query("insert into times (a_timestamp, a_time) values ($1, $2)", [null, {12, 12, 12.999999}], Conn))
%%         },
%%         {"Insert a time without micro second resolution",
%%             ?_assertEqual({{insert, 0, 1}, []}, pgsql_connection:extended_query("insert into times (a_timestamp, a_time) values ($1, $2)", [null, {12, 12, 12}], Conn))
%%         }
%%     ]
%%     end}.

%% subsecond_datetime_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("127.0.0.1", "test", "test", "", [{timezone, "UTC"}]),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.52}}}]}, pgsql_connection:query("select '2012-01-17T10:54:03.52'::timestamptz", Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,4}}}]}, pgsql_connection:query("select '2012-01-17T10:54:03.52'::timestamptz", [{datetime_float_seconds, round}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.52}}}]}, pgsql_connection:query("select '2012-01-17T10:54:03.52'::timestamptz", [{datetime_float_seconds, as_available}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.52}}}]}, pgsql_connection:query("select '2012-01-17T10:54:03.52'::timestamptz", [{datetime_float_seconds, always}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3}}}]}, pgsql_connection:query("select '2012-01-17T10:54:03'::timestamptz", [{datetime_float_seconds, round}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3}}}]}, pgsql_connection:query("select '2012-01-17T10:54:03'::timestamptz", [{datetime_float_seconds, as_available}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.0}}}]}, pgsql_connection:query("select '2012-01-17T10:54:03'::timestamptz", [{datetime_float_seconds, always}], Conn)),

%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.52}}}]}, pgsql_connection:extended_query("select '2012-01-17T10:54:03.52'::timestamptz", [], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,4}}}]}, pgsql_connection:extended_query("select '2012-01-17T10:54:03.52'::timestamptz", [], [{datetime_float_seconds, round}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.52}}}]}, pgsql_connection:extended_query("select '2012-01-17T10:54:03.52'::timestamptz", [], [{datetime_float_seconds, as_available}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.52}}}]}, pgsql_connection:extended_query("select '2012-01-17T10:54:03.52'::timestamptz", [], [{datetime_float_seconds, always}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3}}}]}, pgsql_connection:extended_query("select '2012-01-17T10:54:03'::timestamptz", [], [{datetime_float_seconds, round}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3}}}]}, pgsql_connection:extended_query("select '2012-01-17T10:54:03'::timestamptz", [], [{datetime_float_seconds, as_available}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3.0}}}]}, pgsql_connection:extended_query("select '2012-01-17T10:54:03'::timestamptz", [], [{datetime_float_seconds, always}], Conn)),

%%         ?_assertEqual({{select, 1}, [{{10,54,3.52}}]}, pgsql_connection:query("select '10:54:03.52'::time", Conn)),
%%         ?_assertEqual({{select, 1}, [{{10,54,4}}]}, pgsql_connection:query("select '10:54:03.52'::time", [{datetime_float_seconds, round}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{10,54,3.52}}]}, pgsql_connection:query("select '10:54:03.52'::time", [{datetime_float_seconds, as_available}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{10,54,3.52}}]}, pgsql_connection:query("select '10:54:03.52'::time", [{datetime_float_seconds, always}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{10,54,3}}]}, pgsql_connection:query("select '10:54:03'::time", [{datetime_float_seconds, round}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{10,54,3}}]}, pgsql_connection:query("select '10:54:03'::time", [{datetime_float_seconds, as_available}], Conn)),
%%         ?_assertEqual({{select, 1}, [{{10,54,3.0}}]}, pgsql_connection:query("select '10:54:03'::time", [{datetime_float_seconds, always}], Conn))
%%     ]
%%     end}.

%% tz_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         PosTZConn = pgsql_connection:open("127.0.0.1", "test", "test", "", [{timezone, "UTC+2"}]),
%%         NegTZConn = pgsql_connection:open("127.0.0.1", "test", "test", "", [{timezone, "UTC-2"}]),
%%         {SupPid, PosTZConn, NegTZConn}
%%     end,
%%     fun({SupPid, PosTZConn, NegTZConn}) ->
%%         pgsql_connection:close(PosTZConn),
%%         pgsql_connection:close(NegTZConn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, PosTZConn, NegTZConn}) ->
%%     [
%%         ?_assertEqual({{select,1},[{{11,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::time", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{13,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::timetz", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{11,4,3}}]},    pgsql_connection:extended_query("select '2015-01-03 11:04:03'::time", [], PosTZConn)),
%%         ?_assertEqual({{select,1},[{{13,4,3}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03'::timetz", [], PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{11,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::timestamp", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{13,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::timestamptz", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{11,4,3}}}]},    pgsql_connection:extended_query("select '2015-01-03 11:04:03'::timestamp", [], PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{13,4,3}}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03'::timestamptz", [], PosTZConn)),

%%         ?_assertEqual({{select,1},[{{8,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03+0300'::timetz", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{8,4,3}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03+0300'::timetz", [], PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{8,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03+0300'::timestamptz", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{8,4,3}}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03+0300'::timestamptz", [], PosTZConn)),
%%         ?_assertEqual({{select,1},[{{14,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03-0300'::timetz", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{14,4,3}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03-0300'::timetz", [], PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{14,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03-0300'::timestamptz", PosTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{14,4,3}}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03-0300'::timestamptz", [], PosTZConn)),


%%         ?_assertEqual({{select,1},[{{11,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::time", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{9,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::timetz", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{11,4,3}}]},    pgsql_connection:extended_query("select '2015-01-03 11:04:03'::time", [], NegTZConn)),
%%         ?_assertEqual({{select,1},[{{9,4,3}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03'::timetz", [], NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{11,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::timestamp", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{9,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03'::timestamptz", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{11,4,3}}}]},    pgsql_connection:extended_query("select '2015-01-03 11:04:03'::timestamp", [], NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{9,4,3}}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03'::timestamptz", [], NegTZConn)),

%%         ?_assertEqual({{select,1},[{{8,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03+0300'::timetz", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{8,4,3}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03+0300'::timetz", [], NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{8,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03+0300'::timestamptz", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{8,4,3}}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03+0300'::timestamptz", [], NegTZConn)),
%%         ?_assertEqual({{select,1},[{{14,4,3}}]},   pgsql_connection:query("select '2015-01-03 11:04:03-0300'::timetz", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{14,4,3}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03-0300'::timetz", [], NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{14,4,3}}}]},   pgsql_connection:query("select '2015-01-03 11:04:03-0300'::timestamptz", NegTZConn)),
%%         ?_assertEqual({{select,1},[{{{2015,1,3},{14,4,3}}}]},   pgsql_connection:extended_query("select '2015-01-03 11:04:03-0300'::timestamptz", [], NegTZConn))
%%     ]
%%     end}.


%% timeout_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", [], infinity, Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], [], infinity, Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", [], 2500, Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], [], 2500, Conn)),
%%         ?_assertMatch({error, {pgsql_error, _}}, pgsql_connection:sql_query("select pg_sleep(2)", [], 1500, Conn)),
%%         ?_assertMatch({error, {pgsql_error, _}}, pgsql_connection:param_query("select pg_sleep(2)", [], [], 1500, Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", Conn)),
%%         ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], Conn)),
%%         ?_test(begin
%%             ShowResult1 = pgsql_connection:query("show statement_timeout", Conn),
%%             ?assertMatch({show, [{_}]}, ShowResult1),
%%             {show, [{Value1}]} = ShowResult1,
%%             ?assertEqual({{select, 1}, [{1}]}, pgsql_connection:query("select 1", [], 2500, Conn)),
%%             ?assertEqual({{select, 1}, [{1}]}, pgsql_connection:query("select 1", [], Conn)),
%%             ShowResult2 = pgsql_connection:query("show statement_timeout", Conn),
%%             ?assertMatch({show, [{_}]}, ShowResult2),
%%             {show, [{Value2}]} = ShowResult2,
%%             ?assertEqual({set, []}, pgsql_connection:query("set statement_timeout to 2500", Conn)),
%%             ?assertEqual({{select, 1}, [{1}]}, pgsql_connection:query("select 1", [], 2500, Conn)),
%%             ?assertEqual({{select, 1}, [{1}]}, pgsql_connection:query("select 1", [], Conn)),
%%             ShowResult3 = pgsql_connection:query("show statement_timeout", Conn),
%%             ?assertMatch({show, [{_}]}, ShowResult3),

%%             % Only guarantee is that if the default was 0 (infinity), it is maintained
%%             % after a query with a default (infinity) timeout.
%%             if
%%                 Value1 =:= <<"0">> -> ?assertEqual(Value1, Value2);
%%                 true -> ok
%%             end
%%         end)
%%     ]
%%     end}.

json_types_test_() ->
    {setup,
     fun() ->
             {ok, PoolPid} = start_pool(),
             PoolPid
     end,
     fun(PoolPid) ->
             pgo:query(default, "drop table tmp"),
             pgo:query(default, "drop table tmp_b"),
             kill_sup(PoolPid)
     end,
     fun(_) ->
             [?_test(begin
                         #pg_result{command=create} = pgo:query(default, "create table tmp (id integer primary key, a_json json, b_json json)"),
                         #pg_result{command=insert} = pgo:query(default, "insert into tmp (id, b_json) values ($1, $2)",
                                                                [2, <<"[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]">>]),
                         #pg_result{command=insert} = pgo:query(default, "insert into tmp (id, b_json) values ($1, $2)",
                                                                [3, <<"[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]">>]),
                         #pg_result{command=select, rows=Rows} =
                             pgo:query(default, "select '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::json"),
                         ?assertMatch([[#{<<"a">> := <<"foo">>},
                                        #{<<"b">> := <<"bar">>},
                                        #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{json, R}} <- Rows]),
                         #pg_result{command=select, rows=Rows2} =
                             pgo:query(default, "select b_json from tmp where id = 2"),
                         ?assertMatch([[#{<<"a">> := <<"foo">>},
                                        #{<<"b">> := <<"bar">>},
                                        #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{json, R}} <- Rows2])
                     end),
              ?_test(begin
                         #pg_result{command=create} =
                             pgo:query(default, "create table tmp_b (id integer primary key, a_json jsonb, b_json json)"),
                         #pg_result{command=select,rows=Rows} =
                             pgo:query(default, "select '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::jsonb"),
                         ?assertMatch([[#{<<"a">> := <<"foo">>},
                                        #{<<"b">> := <<"bar">>},
                                        #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{jsonb, R}} <- Rows]),

                         #pg_result{command=insert} = pgo:query(default, "insert into tmp_b (id, a_json) values ($1, $2)",
                                                                [1, <<"[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]">>]),
                         #pg_result{command=select, rows=Rows1} = pgo:query(default, "select a_json from tmp_b where id = 1"),
                         ?assertMatch([[#{<<"a">> := <<"foo">>},
                                        #{<<"b">> := <<"bar">>},
                                        #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{jsonb, R}} <- Rows1])

                     end)
             ]
     end
    }.


%% postgression_ssl_test_() ->
%%     {setup,
%%     fun() ->
%%         ssl:start(),
%%         {ok, SupPid} = saap_sup:start_link(),
%%         ok = application:ensure_started(inets),
%%         {ok, Result} = httpc:request("http://api.postgression.com/"),
%%         ConnInfo = case Result of
%%             {{"HTTP/1.1", 200, "OK"}, _Headers, ConnectionString} ->
%%                 {match, [User, Password, Host, PortStr, Database]} =
%%                     re:run(ConnectionString, "^postgres://(.*):(.*)@(.*):([0-9]+)/(.*)$", [{capture, all_but_first, list}]),
%%                 Port = list_to_integer(PortStr),
%%                 {Host, Database, User, Password, Port};
%%             {{"HTTP/1.1", 500, HTTPStatus}, _Headers, FailureDescription} ->
%%                 ?debugFmt("Postgression unavailable: ~s\n~s\n", [HTTPStatus, FailureDescription]),
%%                 unavailable
%%         end,
%%         {SupPid, ConnInfo}
%%     end,
%%     fun({SupPid, _ConnInfo}) ->
%%         kill_sup(PoolPid),
%%         ssl:stop()
%%     end,
%%     fun({_SupPid, ConnInfo}) ->
%%         case ConnInfo of
%%             unavailable ->
%%                 ?debugMsg("Skipped.\n"),
%%                 [];
%%             {Host, Database, User, Password, Port} ->
%%                 [
%%                     {"Postgression requires SSL",
%%                     ?_test(begin
%%                         try
%%                             pgsql_connection:open(Host, Database, User, Password, [{port, Port}]),
%%                             ?assert(false)
%%                         catch throw:{pgsql_error, _} ->
%%                             ok
%%                         end
%%                     end)
%%                     },
%%                     {"SSL Connection test",
%%                     ?_test(begin
%%                         Conn = pgsql_connection:open(Host, Database, User, Password, [{port, Port}, {ssl, true}, {ssl_options, [{verify, verify_none}]}]),
%%                         ?assertEqual({show, [{<<"on">>}]}, pgsql_connection:query("show ssl", Conn)),
%%                         pgsql_connection:close(Conn)
%%                     end)
%%                     }
%%                 ]
%%         end
%%     end}.

%% constraint_violation_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_test(begin
%%             {updated, 1} = pgsql_connection:sql_query("create table tmp (id integer primary key, a_text text)", Conn),
%%             {updated, 1} = pgsql_connection:param_query("insert into tmp (id, a_text) values (?, ?)", [1, <<"hello">>], Conn),
%%             E = pgsql_connection:param_query("insert into tmp (id, a_text) values (?, ?)", [1, <<"world">>], Conn),
%%             ?assertMatch({error, {pgsql_error, _}}, E),
%%             {error, Err} = E,
%%             ?assert(pgsql_error:is_integrity_constraint_violation(Err))
%%         end)
%%     ]
%%     end}.

custom_enum_test_() ->
    {setup,
     fun() ->
             {ok, PoolPid} = start_pool(),
             PoolPid
     end,
     fun(PoolPid) ->
             #pg_result{command=drop} = pgo:query("DROP TYPE mood;"),
             kill_sup(PoolPid)
     end,
     fun(_) ->
             [
              ?_test(begin
                         %% #pg_result{command=commit} = pgo:query("BEGIN"),
                         #pg_result{command=create} = pgo:query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');"),
                         %% ?assertMatch(#pg_result{command=select, rows=[{{MoodOID, <<"sad">>}}]}
                         %%              when is_integer(MoodOID), pgo:query("select 'sad'::mood;")),
                         %% ?assertMatch(#pg_result{command=select, rows=[{{MoodOID, <<"sad">>}}]}
                         %%              when is_integer(MoodOID), pgo:query("select 'sad'::mood;", [])),
                         %% #pg_result{command=commit} = pgo:query("COMMIT"),
                         ?assertMatch(#pg_result{command=select, rows=[{{mood, <<"sad">>}}]}, pgo:query("select 'sad'::mood;", [])),
                         ?assertMatch(#pg_result{command=select, rows=[{{mood, <<"sad">>}}]}, pgo:query("select 'sad'::mood;", [])),
                         ?assertMatch(#pg_result{command=select, rows=[{{mood, <<"sad">>}}]}, pgo:query("select $1::mood;", [<<"sad">>])),
                         ?assertMatch(#pg_result{command=select, rows=[{{array, [{mood, <<"sad">>}]}}]}, pgo:query("select '{sad}'::mood[];")),
                         ?assertMatch(#pg_result{command=select, rows=[{{array, [{mood, <<"sad">>}]}}]}, pgo:query("select $1::mood[];", [{array, [<<"sad">>]}]))
                     end)
             ]
     end}.

%% custom_enum_native_test_() ->
%%     {setup,
%%     fun() ->
%%         {ok, SupPid} = saap_sup:start_link(),
%%         Conn = pgsql_connection:open("test", "test"),
%%         pgsql_connection:query("DROP TYPE IF EXISTS mood;", Conn),
%%         {SupPid, Conn}
%%     end,
%%     fun({SupPid, Conn}) ->
%%         {{drop, type}, []} = pgsql_connection:query("DROP TYPE mood;", Conn),
%%         pgsql_connection:close(Conn),
%%         kill_sup(PoolPid)
%%     end,
%%     fun({_SupPid, Conn}) ->
%%     [
%%         ?_test(begin
%%             {'begin', []} = pgsql_connection:query("BEGIN", Conn),
%%             {{create, type}, []} = pgsql_connection:query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');", Conn),
%%             ?assertMatch({{select, 1}, [{{MoodOID, <<"sad">>}}]} when is_integer(MoodOID), pgsql_connection:query("select 'sad'::mood;", Conn)),
%%             ?assertMatch({{select, 1}, [{{MoodOID, <<"sad">>}}]} when is_integer(MoodOID), pgsql_connection:extended_query("select 'sad'::mood;", [], Conn)),
%%             {'commit', []} = pgsql_connection:query("COMMIT", Conn),
%%             ?assertMatch({{select, 1}, [{{mood, <<"sad">>}}]}, pgsql_connection:query("select 'sad'::mood;", Conn)),
%%             ?assertMatch({{select, 1}, [{{mood, <<"sad">>}}]}, pgsql_connection:extended_query("select 'sad'::mood;", [], Conn)),
%%             ?assertMatch({{select, 1}, [{{mood, <<"sad">>}}]}, pgsql_connection:extended_query("select $1::mood;", [<<"sad">>], Conn)),
%%             ?assertMatch({{select, 1}, [{{array, [{mood, <<"sad">>}]}}]}, pgsql_connection:query("select '{sad}'::mood[];", Conn)),
%%             ?assertMatch({{select, 1}, [{{array, [{mood, <<"sad">>}]}}]}, pgsql_connection:extended_query("select $1::mood[];", [{array, [<<"sad">>]}], Conn))
%%         end)
%%     ]
%%     end}.

%% invalid_query_test_() ->
%%     {setup,
%%         fun() ->
%%                 {ok, SupPid} = saap_sup:start_link(),
%%                 Conn = pgsql_connection:open("test", "test"),
%%                 {{create, table}, []} = pgsql_connection:query("CREATE TABLE tmp(id integer primary key, other text)", Conn),
%%                 {SupPid, Conn}
%%         end,
%%         fun({SupPid, Conn}) ->
%%                 pgsql_connection:close(Conn),
%%                 kill_sup(PoolPid)
%%         end,
%%         fun({_SupPid, Conn}) ->
%%                 [
%%                     ?_test(begin
%%                                 ?assertEqual({error, {badarg, toto}}, pgsql_connection:extended_query("insert into tmp(id, other) values (1, $1)", [toto], Conn)),
%%                                 % connection still usable
%%                                 R = pgsql_connection:extended_query("insert into tmp(id, other) values (1, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R)
%%                         end),
%%                     ?_test(begin
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:query("FOO", Conn)),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:query("FOO", [], Conn)),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:query("FOO", [], 5000, Conn)),
%%                                 % connection still usable
%%                                 R = pgsql_connection:extended_query("insert into tmp(id, other) values (2, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R)
%%                         end),
%%                     ?_test(begin
%%                                 {'begin',[]} = pgsql_connection:query("BEGIN", Conn),
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (3, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:query("FOO", [], 5000, Conn)),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:query("FOO", [], 5000, Conn)),
%%                                 {'rollback',[]} = pgsql_connection:query("COMMIT", Conn),
%%                                 % row 3 was not inserted.
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (3, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1)
%%                         end),
%%                     ?_test(begin
%%                                 {'begin',[]} = pgsql_connection:query("BEGIN", Conn),
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (4, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:extended_query("FOO", [], [], 5000, Conn)),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:extended_query("FOO", [], [], 5000, Conn)),
%%                                 {'rollback',[]} = pgsql_connection:query("COMMIT", Conn),
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (4, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1)
%%                         end),
%%                     ?_test(begin
%%                                 {'begin',[]} = pgsql_connection:query("BEGIN", Conn),
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (5, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:extended_query("FOO", [], [], 5000, Conn)),
%%                                 {'rollback',[]} = pgsql_connection:query("ROLLBACK", Conn),
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (5, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1)
%%                         end),
%%                     ?_test(begin
%%                                 {'begin',[]} = pgsql_connection:query("BEGIN", Conn),
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (6, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1),
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:extended_query("FOO", [], [], 5000, Conn)),
%%                                 {'rollback',[]} = pgsql_connection:query("ROLLBACK", [], 5000, Conn),
%%                                 R1 = pgsql_connection:extended_query("insert into tmp(id, other) values (6, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R1)
%%                         end),
%%                     ?_test(begin
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:extended_query("FOO", [], Conn)),
%%                                 % Empty array forces a Describe command, thus we end the normal sequence with Flush and not with Sync
%%                                 % Error recovery therefore requires a Sync to get the ReadyForQuery message.
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:extended_query("FOO", [{array, [<<>>]}], Conn)),
%%                                 % Likewise, cursor mode does send a Flush instead of a Sync after Bind
%%                                 ?assertMatch({error, {pgsql_error, _Error}}, pgsql_connection:foreach(fun(_Row) -> ok end, "FOO", Conn)),
%%                                 % connection still usable
%%                                 R = pgsql_connection:extended_query("insert into tmp(id, other) values (7, $1)", ["toto"], Conn),
%%                                 ?assertEqual({{insert, 0, 1}, []}, R)
%%                         end)
%%                 ]
%%         end
%%     }.

