-module(pgo_basic_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(UUID, <<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>).
-define(BIN_UUID, <<114,127,66,166,230,160,66,35,155,114,106,94,183,67,106,181>>).
-define(TXT_UUID, <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>).

all() ->
    case os:getenv("CIRCLECI") of
        false ->
            [{group, clear}, {group, ssl}];
        _ ->
            [{group, clear}]
    end.

groups() ->
    [{clear, [], cases()},
     {ssl, [shuffle, parallel], cases()}].

cases() ->
    [select, insert_update, text_types, rows_as_maps,
     json_jsonb, types, validate_telemetry,
     int4_range, ts_range, tstz_range, numerics,
     hstore, records, circle, path, polygon, line,
     line_segment, tid, bit_string, arrays, tsvector].

init_per_suite(Config) ->
    application:load(pg_types),
    application:set_env(pg_types, json_config, {jsone, [], [{keys, atom}]}),
    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    ok.

init_per_group(clear, Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             port => 5432,
                                             database => "test",
                                             user => "test"}),

    Config;
init_per_group(ssl, Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             port => 5434,
                                             ssl => true,
                                             database => "test",
                                             user => "test"}),

    Config.

end_per_group(_, _Config) ->
    pgo:query("drop table tmp"),
    pgo:query("drop table tmp_b"),
    pgo:query("drop table foo"),
    pgo:query("drop table types"),
    pgo:query("drop table numeric_tmp"),
    pgo:query("drop table hstore_tmp"),
    pgo:query("drop extension hstore"),

    application:stop(pgo),
    ok.

int4_range(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table foo_range (id integer primary key, some_range int4range)")
),

    ?assertMatch(#{rows := [{empty}]},
                 pgo:query("select '[1,1)'::int4range")),

    ?assertMatch(#{rows := [{{{1, 3},
                              {true, false}}}]},
                 pgo:query("select '[1,3)'::int4range")),

    ?assertMatch(#{rows := [{{{1, unbound},
                              {true, false}}}]},
                 pgo:query("select '[1,]'::int4range")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into foo_range (id, some_range) values (1, $1)", [{4, 12}])),
    ?assertMatch(#{rows := [{1, {{4, 13}, {true, false}}}]},
                 pgo:query("select * from foo_range where id=1")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into foo_range (id, some_range) values (2, $1)",
                           [{{4, 12}, {true, false}}])),
    ?assertMatch(#{rows := [{2, {{4, 12}, {true, false}}}]},
                 pgo:query("select * from foo_range where id=2")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into foo_range (id, some_range) values (3, $1)",
                           [{{4, unbound}, {true, false}}])),
    ?assertMatch(#{rows := [{3, {{4, unbound}, {true, false}}}]},
                 pgo:query("select * from foo_range where id=3")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into foo_range (id, some_range) values (4, $1)",
                           [empty])),
    ?assertMatch(#{rows := [{4, empty}]},
                 pgo:query("select * from foo_range where id=4")).

ts_range(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table foo_ts_range (id integer primary key, some_range tsrange)")),
    ?assertMatch(#{command := insert},
                  pgo:query("insert into foo_ts_range (id, some_range) values (1, $1)", [{{{2001, 1, 1}, {4, 10, 0}},
                                                                                          {{2001, 1, 1}, {5, 10, 0.0}}}])),
    ?assertMatch(#{rows := [{1, {{{{2001, 1, 1}, {4, 10, 0}}, {{2001, 1, 1}, {5, 10, 0}}},
                                 {true, true}}}]},
                 pgo:query("select * from foo_ts_range order by id asc")),


    ok.

tstz_range(_Config) ->
    ?assertMatch(#{rows := [{{{{{2019,6,22},{8,0,0}}, {{2019,6,22},{9,0,0}}},
                              {true, true}}}]}, pgo:query("SELECT tstzrange('2019-06-22 08:00:00.000+00','2019-06-22 09:00:00.000+00', '[]');")),

    ?assertMatch(#{rows := [{{{{{2019,6,22},{8,0,0}}, {{2019,6,22},{9,0,0}}},
                              {true, false}}}]},pgo:query("SELECT tstzrange('2019-06-22 08:00:00.000+00','2019-06-22 09:00:00.000+00', '[)');")),

    ?assertMatch(#{rows := [{{{{{2019,6,22},{8,0,0}}, infinity},
                              {true, true}}}]},pgo:query("SELECT tstzrange('2019-06-22 08:00:00.000+00','infinity', '[]');")),

    ?assertMatch(#{rows := [{{{'-infinity', infinity},
                              {true, true}}}]}, pgo:query("SELECT tstzrange('-infinity','infinity', '[]');")),

    ?assertMatch(#{command := create},
                 pgo:query("create temporary table foo_tstz_range (id integer primary key, some_range tstzrange)")),
    ?assertMatch(#{command := insert},
                 pgo:query("insert into foo_tstz_range (id, some_range) values (1, $1)", [{{{2001, 1, 1}, {4, 10, 0}},
                                                                                           {{2001, 1, 1}, {5, 10, 0.0}}}])),
    ?assertMatch(#{rows := [{1, {{{{2001, 1, 1}, {4, 10, 0}}, {{2001, 1, 1}, {5, 10, 0}}},
                                 {true, true}}}]},
                 pgo:query("select * from foo_tstz_range order by id asc")),

    ok.

validate_telemetry(_Config) ->
    Self = self(),
    telemetry:attach(<<"send-query-time">>, [pgo, query],
                     fun(Event, Latency, Metadata, _) -> Self ! {Event, Latency, Metadata} end, []),

    ?assertMatch(#{rows := [{null}]}, pgo:query("select null")),

    receive
        {[pgo, query], #{latency := Latency}, #{query_time := QueryTime,
                                                pool := default}} ->
            ?assertEqual(Latency, QueryTime),
            telemetry:detach(<<"send-query-time">>)
    after
        500 ->
            ct:fail(timeout)
    end.

select(_Config) ->
    {error, {Module, Reason}} = pgo:query("select $1", []),
    ?assertEqual("parameters needed for query not equal to number of arguments 1 != 0", Module:format_error(Reason)),
    {error, {Module1, Reason1}} = pgo:query("select $1", [1, 2]),
    ?assertEqual("parameters needed for query not equal to number of arguments 1 != 2", Module1:format_error(Reason1)),
    ?assertMatch(#{rows := [{null}]}, pgo:query("select null")),
    ?assertMatch(#{rows := [{null}]}, pgo:query("select null", [])),
    ?assertMatch(#{rows := [{null}]}, pgo:query("select null")),
    ?assertMatch(#{rows := [{null}]}, pgo:query("select null", [])).

insert_update(_Config) ->
    ?assertMatch(#{command := create},
                             pgo:query("create temporary table foo (id integer primary key, some_text text)")),
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

rows_as_maps(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table foo_1 (id integer primary key, some_text text)")),
    ?assertMatch(#{command := insert},
                 pgo:query("insert into foo_1 (id, some_text) values (1, 'hello')")),

    ?assertMatch(#{command := select,rows := [#{<<"id">> := 1,<<"some_text">> := <<"hello">>}]},
                 pgo:query("select * from foo_1", [], #{decode_opts => [return_rows_as_maps]})).

json_jsonb(_Config) ->
    #{command := create} = pgo:query("create table tmp (id integer primary key, a_json json, b_json jsonb)"),
    #{command := insert} = pgo:query("insert into tmp (id, a_json, b_json) values ($1, $2, $3)",
                                     [3, #{<<"a">> => <<"foo">>}, #{<<"b">> => <<"bar">>}]),

    #{command := select, rows := [{#{b := <<"bar">>}}]} =
        pgo:query("select b_json from tmp where id=3"),

    #{command := select, rows := Rows} =
        pgo:query("select '[{\"a\":\"foo\"},{\"b\":\"bar\"},{\"c\":\"baz\"}]'::json"),
    ?assertMatch([{[#{a := <<"foo">>},
                    #{b := <<"bar">>},
                    #{c := <<"baz">>}]}], Rows).

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
                                                  [10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, ?BIN_UUID, <<"deadbeef">>, 3.1415])),


    R = pgo:query("select * from types where id = 10"),
    ?assertMatch(#{command := select, rows := [_Row]}, R),
    #{command := select, rows := [Row]} = R,
    ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row),
    {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID, <<"deadbeef">>, Float} = Row,
    ?assertMatch(?BIN_UUID, UUID),
    ?assert(Float > 3.1413),
    ?assert(Float < 3.1416),

    R1 = pgo:query("select * from types where id = $1", [10]),
    ?assertMatch(#{command := select, rows := [_Row1]}, R1),
    #{command := select, rows := [Row1]} = R1,
    ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row1),
    {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID1, <<"deadbeef">>, Float1} = Row1,
    ?assertMatch(?BIN_UUID, UUID1),
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
    ?assertMatch({?BIN_UUID}, Row17),
    ?assertMatch({?TXT_UUID}, Row18).

numerics(_Config) ->
    BasicQuery = "select $1::numeric",
    ?assertMatch(#{rows := [{1}]}, pgo:query(BasicQuery, [1])),
    ?assertMatch(#{rows := [{-1}]}, pgo:query(BasicQuery, [-1])),
    ?assertMatch(#{rows := [{1.1}]}, pgo:query(BasicQuery, [1.1])),
    ?assertMatch(#{rows := [{-1.1}]}, pgo:query(BasicQuery, [-1.1])),
    ?assertMatch(#{rows := [{1.12345}]}, pgo:query(BasicQuery, [1.12345])),
    ?assertMatch(#{rows := [{1.0e-9}]}, pgo:query(BasicQuery, [0.000000001])),
    ?assertMatch(#{rows := [{-1.0e-9}]}, pgo:query(BasicQuery, [-0.000000001])),
    ?assertMatch(#{rows := [{1.0e-10}]}, pgo:query(BasicQuery, [0.0000000001])),
    ?assertMatch(#{rows := [{-1.0e-10}]}, pgo:query(BasicQuery, [-0.0000000001])),
    ?assertMatch(#{rows := [{1.0e-11}]}, pgo:query(BasicQuery, [0.00000000001])),
    ?assertMatch(#{rows := [{-1.0e-11}]}, pgo:query(BasicQuery, [-0.00000000001])),
    ?assertMatch(#{rows := [{1.0e-32}]}, pgo:query(BasicQuery, [1.0e-32])),

    ?assertMatch(#{rows := [{1.0e-32}]}, pgo:query(BasicQuery, [<<"1.0e-32">>])),
    ?assertMatch(#{rows := [{1.0e+32}]}, pgo:query(BasicQuery, [<<"1.0e+32">>])),


    #{command := create} = pgo:query("create table numeric_tmp (id integer primary key, a_int integer, a_num numeric,
                                      b_num numeric(5,3))"),
    ?assertMatch(#{command := insert}, pgo:query("insert into numeric_tmp (id, a_int, a_num) values ($1, $2, $3)", [1,1,
                                                                                                                    0.010000001])),

    ?assertMatch(#{command := insert}, pgo:query("insert into numeric_tmp (id, a_int, a_num) values ($1, $2, $3)", [2,1,
                                                                                                                    0.00000000000000000000000000000001])),
    ?assertMatch(#{rows := [{1.0}]}, pgo:query("select a_int::numeric(2, 1) from numeric_tmp where id = 1", [])),
    ?assertMatch(#{rows := [{1.00}]}, pgo:query("select a_int::numeric(3, 1) from numeric_tmp where id = 1", [])),
    ?assertMatch(#{rows := [{0.01}]}, pgo:query("select a_num::numeric(10, 3) from numeric_tmp where id = 1", [])),
    ?assertMatch(#{rows := [{0.01}]}, pgo:query("select a_num::numeric(10, 3) from numeric_tmp where id = 1", [])),

    ?assertMatch(#{rows := [{1.0e-32}]}, pgo:query("select a_num::numeric from numeric_tmp where id = 2", [])).

hstore(_Config) ->
    #{command := create} = pgo:query("CREATE EXTENSION hstore"),
    #{command := create} = pgo:query("create table hstore_tmp (id integer primary key, labels hstore)"),

    ?assertMatch(#{command := insert}, pgo:query("insert into hstore_tmp (id, labels) values ($1, $2)", [1, #{<<"key-1">> => <<"value-1">>, <<"key-2">> => <<"value-2">>}])),
    ?assertMatch(#{command := select,
                   rows := [{1, #{<<"key-1">> := <<"value-1">>, <<"key-2">> := <<"value-2">>}}]},
                 pgo:query("select id, labels from hstore_tmp where id=1")),

    ok.

records(_Config) ->
    pgo:query("DROP TABLE IF EXISTS composite1"),
    #{command := create} = pgo:query("CREATE TABLE composite1 (a int, b text)"),

    ?assertMatch(#{rows := [{{1, <<"2">>}}]},pgo:query("SELECT (1, '2')::composite1", [])),
    ?assertMatch(#{rows := [{[{1, <<"2">>}]}]}, pgo:query("SELECT ARRAY[(1, '2')::composite1]", [])),

    ?assertMatch(#{rows := [{{null, <<"2">>}}]},pgo:query("SELECT $1::composite1", [{null, <<"2">>}])),
    ?assertMatch(#{rows := [{{1, <<"2">>}}]},pgo:query("SELECT $1::composite1", [{1, <<"2">>}])).

circle(_Config) ->
    ?assertMatch(#{rows := [{#{center := #{x := 1.0,
                                           y := 1.0},
                               radius := 10.0}}]}, pgo:query("SELECT $1::circle", [#{center => #{x => 1,
                                                                                                 y => 1},
                                                                                     radius => 10}])).

path(_Config) ->
    ?assertMatch(#{rows := [{#{open := true,
                               points := [#{x := 1.0, y := 1.0}, #{x := 2.0, y:= 3.0}]}}]},
                 pgo:query("SELECT $1::path", [#{points => [#{x => 1, y => 1}, #{x => 2, y => 3}],
                                                 open => true}])).

polygon(_Config) ->
    ?assertMatch(#{rows := [{#{vertices := [#{x := 1.0, y := 1.0}, #{x := 2.0, y:= 3.0}]}}]},
                 pgo:query("SELECT $1::polygon", [#{vertices => [#{x => 1, y => 1}, #{x => 2, y => 3}]}])).

line(_Config) ->
    ?assertMatch(#{rows := [{#{a := 1.0, b := 2.0, c := 3.0}}]},
                 pgo:query("SELECT $1::line", [#{a => 1, b => 2, c => 3}])).

line_segment(_Config) ->
    ?assertMatch(#{rows := [{#{point1 := #{x := 1.0, y := 1.0}, point2 := #{x := 2.0, y := 3.0}}}]},
                 pgo:query("SELECT $1::lseg", [#{point1 => #{x => 1, y => 1},
                                                 point2 => #{x => 2, y => 3}}])).

tid(_Config) ->
    ?assertMatch(#{rows := [{{1, 2}}]},
                 pgo:query("SELECT $1::tid", [{1, 2}])).

bit_string(_Config) ->
    ?assertMatch(#{rows := [{<<1:1,0:1,1:1,0:1,0:1>>}]},
                 pgo:query("SELECT bit '101'::bit(5)", [])),

    ?assertMatch(#{rows := [{<<1:1,0:1,1:1,0:1,0:1>>}]},
                 pgo:query("SELECT $1::bit(5)", [<<1:1,0:1,1:1>>])),

    ?assertMatch(#{rows := [{<<1:1,1:1,0:1>>}]}, pgo:query("SELECT bit '110' :: varbit", [])).

arrays(_Config) ->
    ?assertMatch(#{rows := [{[<<"s1">>,null]}]}, pgo:query("SELECT $1::text[]", [[<<"s1">>, null]])).

tsvector(_Config) ->
    ?assertMatch(#{rows := [{[{<<"fat">>,[{2,null}]},{<<"rat">>,[{3,null}]}]}]},
                 pgo:query("SELECT to_tsvector('english', 'The Fat Rats')")),
    ?assertMatch(#{rows := [{[{<<"a">>,[{1,'A'}]},
                              {<<"cat">>,[{5,null}]},
                              {<<"fat">>,[{2,'B'},{4,'C'}]}]}]},
                 pgo:query("SELECT 'a:1A fat:2B,4C cat:5D'::tsvector")),
    ?assertMatch(#{rows := [{[{<<"a">>,[{1,'A'}]},
                              {<<"cat">>,[{5,null}]},
                              {<<"fat">>,[{2,'B'},{4,'C'}]}]}]},
                 pgo:query("SELECT $1::tsvector", [[{<<"a">>,[{1,'A'}]},
                                                    {<<"cat">>,[{5,null}]},
                                                    {<<"fat">>,[{2,'B'},{4,'C'}]}]])).
