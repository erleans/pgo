-module(pgo_datetime_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [select, insert, interval].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             database => "test",
                                             user => "test"}),

    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    ok.

select(_Config) ->
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
