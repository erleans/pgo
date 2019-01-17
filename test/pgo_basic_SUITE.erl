-module(pgo_basic_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(UUID, <<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>).
%% <<114,127,66,166,230,160,66,35,155,114,106,94,183,67,106,181>>).
-define(TXT_UUID, <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>).

all() ->
    [select, insert_update, text_types, json_jsonb, types, validate_telemetry].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, [{size, 1}, {database, "test"}, {user, "test"}]),

    Config.

end_per_suite(_Config) ->
    pgo:query("drop table tmp"),
    pgo:query("drop table tmp_b"),
    pgo:query("drop table foo"),
    pgo:query("drop table types"),

    application:stop(pgo),
    application:stop(opencensus),
    ok.

validate_telemetry(_Config) ->
    Self = self(),
    telemetry:attach(<<"send-query-time">>, [pgo, query],
                     fun(Event, Latency, Metadata, _) -> Self ! {Event, Latency, Metadata} end, []),

    ?assertMatch(#{rows := [{null}]}, pgo:query("select null")),

    receive
        {[pgo, query], Latency, #{query_time := QueryTime}} ->
            ?assertEqual(Latency, QueryTime)
    after
        500 ->
            ct:fail(timeout)
    end.

select(_Config) ->
    ?assertMatch({error, _}, pgo:query("select $1", [])),

    ?assertMatch(#{rows := [{null}]}, pgo:query("select null")),
    ?assertMatch(#{rows := [{null}]}, pgo:query("select null", [])),
    ?assertMatch(#{rows := [{null}]}, pgo:query("select null")),
    ?assertMatch(#{rows := [{null}]}, pgo:query("select null", [])).

insert_update(_Config) ->
    ?assertMatch(#{command := create},
                             pgo:query("create table foo (id integer primary key, some_text text)")),
    ?assertMatch(#{command := insert},
                  pgo:query("insert into foo (id, some_text) values (1, 'hello')")),
    ?assertMatch(#{command := update},
                  pgo:query("update foo set some_text = 'hello world'")),
    ?assertMatch(#{command := insert}, pgo:query("insert into foo (id, some_text) values (2, 'hello again')")),
    ?assertMatch(#{}, pgo:query("update foo set some_text = 'hello world' where id = 1")),
    ?assertMatch(#{}, pgo:query("update foo set some_text = 'goodbye, all' where id = 3")),
    ?assertMatch(#{rows := [{1, <<"hello world">>}, {2, <<"hello again">>}]},
                  pgo:query("select * from foo order by id asc")),
    ?assertMatch(#{rows := [{1, <<"hello world">>}, {2, <<"hello again">>}]},
                  pgo:query("select id as the_id, some_text as the_text from foo order by id asc")),
    ?assertMatch(#{rows := [{<<"hello world">>, 1}, {<<"hello again">>, 2}]},
                  pgo:query("select some_text, id from foo order by id asc")),
    ?assertMatch(#{rows := [{<<"hello again">>}]}, pgo:query("select some_text from foo where id = 2")),
    ?assertMatch(#{}, pgo:query("select * from foo where id = 3")).

text_types(_Config) ->
    ?assertMatch(#{command := select,rows := [{<<"foo">>}]}, pgo:query("select 'foo'::text")),
    ?assertMatch(#{command := select,rows := [{<<"foo">>}]}, pgo:query("select $1::text", [<<"foo">>])),
    ?assertMatch(#{command := select,rows := [{<<"foo         ">>}]}, pgo:query("select 'foo'::char(12)")),
    ?assertMatch(#{command := select,rows := [{<<"foo         ">>}]}, pgo:query("select $1::char(12)", [<<"foo">>])),
    ?assertMatch(#{command := select,rows := [{<<"foo">>}]}, pgo:query("select 'foo'::varchar(12)")),
    ?assertMatch(#{command := select,rows := [{<<"foo">>}]}, pgo:query("select $1::varchar(12)", [<<"foo">>])),
    ?assertMatch(#{command := select,rows := [{<<"foo">>}]}, pgo:query("select 'foobar'::char(3)")),
    ?assertMatch(#{command := select,rows := [{<<"foo">>}]}, pgo:query("select $1::char(3)", [<<"foobar">>])).

json_jsonb(_Config) ->
    #{command := create} = pgo:query("create table tmp (id integer primary key, a_json json, b_json json)"),
    #{command := insert} = pgo:query("insert into tmp (id, b_json) values ($1, $2)",
                                     [2, <<"[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]">>]),
    #{command := insert} = pgo:query("insert into tmp (id, b_json) values ($1, $2)",
                                     [3, <<"[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]">>]),
    #{command := select, rows := Rows} =
        pgo:query("select '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::json"),
    ?assertMatch([[#{<<"a">> := <<"foo">>},
                   #{<<"b">> := <<"bar">>},
                   #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{json, R}} <- Rows]),
    #{command := select, rows := Rows1} =
        pgo:query("select b_json from tmp where id = 2"),
    ?assertMatch([[#{<<"a">> := <<"foo">>},
                   #{<<"b">> := <<"bar">>},
                   #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{json, R}} <- Rows1]),

    #{command := create} =
        pgo:query("create table tmp_b (id integer primary key, a_json jsonb, b_json json)"),
    #{command := select,rows := Rows2} =
        pgo:query("select '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::jsonb"),
    ?assertMatch([[#{<<"a">> := <<"foo">>},
                   #{<<"b">> := <<"bar">>},
                   #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{jsonb, R}} <- Rows2]),

    #{command := insert} = pgo:query("insert into tmp_b (id, a_json) values ($1, $2)",
                                     [1, <<"[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]">>]),
    #{command := select, rows := Rows3} = pgo:query("select a_json from tmp_b where id = 1"),
    ?assertMatch([[#{<<"a">> := <<"foo">>},
                   #{<<"b">> := <<"bar">>},
                   #{<<"c">> := <<"baz">>}]], [jsx:decode(R, [return_maps]) || {{jsonb, R}} <- Rows3]).

types(_Config) ->
    ?assertMatch(#{command := create},
                  pgo:query("create table types (id integer primary key, an_integer integer, a_bigint bigint, a_text text, a_uuid uuid, a_bytea bytea, a_real real)")),
    ?assertMatch(#{command := insert},
                  pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (1, null, null, null, null, null, null)")),
    ?assertMatch(#{rows := [{1, null, null, null, null, null, null}]},
                  pgo:query("select * from types where id = 1")),
    ?assertMatch(#{command := insert},
                  pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)", [2, null, null, null, null, null, null])),
    ?assertMatch(#{rows := [{2, null, null, null, null, null, null}]},
                  pgo:query("select * from types where id = 2")),
    ?assertMatch(#{command := insert},
                  pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                            [3, 42, null, null, null, null, null])),
    ?assertMatch(#{command := insert},
                  pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                            [4, null, 1099511627776, null, null, null, null])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                  [6, null, null, <<"And in the end, the love you take is equal to the love you make">>, null, null, null])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                  [7, null, null, null, ?UUID, null, null])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                  [21, null, null, null, <<236,0,0,208,169,204,65,147,163,9,107,147,233,112,253,222>>, null, null])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                  [8, null, null, null, null, <<"deadbeef">>, null])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                  [9, null, null, null, null, null, 3.1415])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                  [19, null, null, null, null, null, 3.0])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                  [10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, ?UUID, <<"deadbeef">>, 3.1415])),


    R = pgo:query("select * from types where id = 10"),
    ?assertMatch(#{command := select, rows := [_Row]}, R),
    #{command := select, rows := [Row]} = R,
    ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row),
    {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID, <<"deadbeef">>, Float} = Row,
    ?assertMatch(?UUID, UUID),
    ?assert(Float > 3.1413),
    ?assert(Float < 3.1416),

    R1 = pgo:query("select * from types where id = $1", [10]),
    ?assertMatch(#{command := select, rows := [_Row1]}, R1),
    #{command := select, rows := [Row1]} = R1,
    ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row1),
    {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID1, <<"deadbeef">>, Float1} = Row1,
    ?assertMatch(<<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, UUID1),
    ?assert(Float1 > 3.1413),
    ?assert(Float1 < 3.1416),
    ?assertMatch(#{command := insert},
                 pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                           [11, null, null, null, null, <<"deadbeef">>, null])),
    ?assertMatch(#{command := insert, rows := [{15}]},
                 pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
                           [15, null, null, null, null, <<"deadbeef">>, null])),


    R2 = pgo:query("select * from types where id = $1", [11]),
    ?assertMatch(#{command := select, rows := [_Row2]}, R2),
    #{command := select, rows := [Row2]} = R2,
    ?assertMatch({11, null, null, null, null, <<"deadbeef">>, null}, Row2),

    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                 [199, null, null, null, <<"00000000-0000-0000-0000-000000000000">>, null, null])),

    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                 [16, null, null, null, ?UUID, null, null])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                 [17, null, null, ?TXT_UUID, null, null, null])),
    ?assertMatch(#{command := insert}, pgo:query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values ($1, $2, $3, $4, $5, $6, $7)",
                                                 [18, null, null, ?TXT_UUID, null, null, null])),


    R3 = pgo:query("select a_text from types where id IN ($1, $2) order by id", [17, 18]),
    ?assertMatch(#{command := select, rows := [_Row17, _Row18]}, R3),
    #{command := select, rows := [Row17, Row18]} = R3,
    ?assertMatch({?TXT_UUID}, Row17),
    ?assertMatch({?TXT_UUID}, Row18).
