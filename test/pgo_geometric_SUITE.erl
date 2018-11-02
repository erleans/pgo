-module(pgo_geometric_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [select, insert].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, [{size, 1}, {database, "test"}, {user, "test"}]),

    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    application:stop(opencensus),
    ok.

select(_Config) ->
    ?assertMatch(#{command := select,
                  rows := [{{point, {2.0,0.0}}}]},
                 pgo:query("select '(2.0,0)'::point")).

insert(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table points (a_point point)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into points (a_point) VALUES ($1)",
                           [{3.2, 4.5}])),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into points (a_point) VALUES ($1)",
                           [{point, {0, 5}}])),

    ?assertMatch(#{command := select,
                  rows := [{{point, {3.2, 4.5}}},
                           {{point, {0.0, 5.0}}}]},
                 pgo:query("select a_point from points")).
