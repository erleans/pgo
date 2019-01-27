-module(pgo_geometric_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [select, insert].

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
                  rows := [{#{lat := 2.0, long := 0.0}}]},
                 pgo:query("select '(0,2.0)'::point")).

insert(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table points (a_point point)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into points (a_point) VALUES ($1)",
                           [#{long => 1.1, lat => 74.9}])),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into points (a_point) VALUES ($1)",
                           [#{long => 3.2, lat => 4.5}])),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into points (a_point) VALUES ($1)",
                           [#{long => 0, lat => 5}])),

    ?assertMatch(#{command := select,
                  rows := [{#{long := 1.1, lat := 74.9}},
                           {#{long := 3.2, lat := 4.5}},
                           {#{long := 0.0, lat := 5.0}}]},
                 pgo:query("select a_point from points")).
