-module(pgo_datetime_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [{group, erl_datetime}, {group, as_integer}, {group, as_float}, {group, as_micro}].

groups() ->
    [{erl_datetime, [], [select, insert, interval]},
     {as_micro, [], [as_micro]},
     {as_integer, [], [as_integer]},
     {as_float, [], [as_float]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(erl_datetime, Config) ->
    application:load(pg_types),
    application:set_env(pg_types, timestamp_config, []),

    {ok, _} = application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                                   database => "test",
                                                   user => "test",
                                                   password => "password"}),
    Config;
init_per_group(as_micro, Config) ->
    application:load(pg_types),
    application:set_env(pg_types, timestamp_config, integer_system_time_microseconds),

    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             database => "test",
                                             user => "test",
                                             password => "password"}),
    Config;
init_per_group(as_integer, Config) ->
    application:load(pg_types),
    application:set_env(pg_types, timestamp_config, integer_system_time_seconds),

    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             database => "test",
                                             user => "test",
                                             password => "password"}),
    Config;
init_per_group(as_float, Config) ->
    application:load(pg_types),
    application:set_env(pg_types, timestamp_config, float_system_time_seconds),

    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             database => "test",
                                             user => "test",
                                             password => "password"}),
    Config.

end_per_group(_, _Config) ->
    application:unset_env(pg_types, timestamp_config),
    application:stop(pgo),
    application:unload(pg_types),

    pgo_test_utils:clear_types(default),

    ok.

select(_Config) ->
    pgo:query("SET TIMEZONE TO 'UTC'"),
    
    ?assertMatch(#{command := select,
                   rows := [{{2012,1,17}}]},
                 pgo:query("select '2012-01-17 10:54:03.45'::date")),

    ?assertMatch(#{command := select,
                   rows := [{{{2012,1,17},{10,54,3.45}}}]},
                 pgo:query("select '2012-01-17 10:54:03.45'::timestamp")),

    ?assertMatch(#{command := select,
                   rows := [{{{2012,1,17},{10,54,3.45}}}]},
                 pgo:query("select '2012-01-17 10:54:03.45'::timestamptz")).

insert(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table times (a_timestamp timestamp, a_time time)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into times (a_timestamp, a_time) VALUES ($1, $2)",
                           [{{2012,1,17},{10,54,3.45}}, {10,54,3.45}])),

    ?assertMatch(#{command := select,
                   rows := [{{{2012,1,17},{10,54,3.45}}, {10,54,3.45}}]},
                 pgo:query("select a_timestamp, a_time from times")).

interval(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table interval_times (a_timestamp timestamp, b_timestamp timestamp)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into interval_times (a_timestamp, b_timestamp) VALUES ($1, $2)",
                           [{{2012,1,17},{10,54,3.45}}, {{2012,1,20},{10,54,3.45}}])),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into interval_times (a_timestamp, b_timestamp) VALUES ($1, $2)",
                           [{{2013,1,17},{10,54,3.45}}, {{2013,1,28},{10,54,3.45}}])),

    ?assertMatch(#{command := select,
                   rows := [{{{2012,1,17},{10,54,3.45}}, {{2012,1,20},{10,54,3.45}}}]},
                 pgo:query("select a_timestamp, b_timestamp from interval_times where b_timestamp - a_timestamp < $1", [{interval, {0, 5, 0}}])),

    ?assertMatch(#{command := select,
                   rows := [{{interval, {{3,2,1}, 5, 0}}}]},
                 pgo:query("select $1::interval", [{interval, {{3,2,1}, 5, 0}}])),

    ?assertMatch(#{rows := [{{interval, {{0,0,0}, 7, 0}}}]},
                 pgo:query(<<"SELECT '7 days'::interval">>)),
    ?assertMatch(#{rows := [{{interval, {{0,0,0}, 0, 7}}}]},
                 pgo:query(<<"SELECT '7 months'::interval">>)),
    ?assertMatch(#{rows := [{{interval, {{0,0,0}, 0, 84}}}]},
                 pgo:query(<<"SELECT '7 years'::interval">>)),
    ?assertMatch(#{rows := [{{interval, {{3,2,1}, 4, 77}}}]},
                 pgo:query(<<"SELECT '6 years 5 months 4 days 3 hours 2 minutes 1 second'::interval">>)).


as_integer(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table times (a_timestamp timestamp, b_timestamp timestamp)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into times (a_timestamp, b_timestamp) VALUES ($1, $2)",
                           [{{2012,1,17},{10,54,3.45}}, 1326797643 * 1000000])),

    ?assertMatch(#{command := select,
                   rows := [{1326797643, 1326797643}]},
                 pgo:query("select a_timestamp, b_timestamp from times")).

as_float(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table times (a_timestamp timestamp, b_timestamp timestamp)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into times (a_timestamp, b_timestamp) VALUES ($1, $2)",
                           [{{2012,1,17},{10,54,3.45}}, 1326797643 * 1000000])),

    ?assertMatch(#{command := select,
                   rows := [{1326797643.45, 1326797643.0}]},
                 pgo:query("select a_timestamp, b_timestamp from times")).

as_micro(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table times (a_timestamp timestamp, b_timestamp timestamp)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into times (a_timestamp, b_timestamp) VALUES ($1, $2)",
                           [{{2012,1,17},{10,54,3.45}}, 1326797643450000])),

    ?assertMatch(#{command := select,
                   rows := [{1326797643450000, 1326797643450000}]},
                 pgo:query("select a_timestamp, b_timestamp from times")).
