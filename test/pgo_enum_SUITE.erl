-module(pgo_enum_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [select, insert].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, [{size, 1}, {database, "test"}, {user, "test"}]),

    ?assertMatch(#{command := create},
                 pgo:query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")),

    Config.

end_per_suite(_Config) ->
    #{command := drop} = pgo:query("DROP TYPE mood CASCADE;"),

    application:stop(pgo),
    application:stop(opencensus),
    ok.

select(_Config) ->

    ?assertMatch(#{command := select, rows := [{{mood, <<"sad">>}}]},
                 pgo:query("select 'sad'::mood", [])),

    ?assertMatch(#{command := select, rows := [{{mood, <<"sad">>}}]},
                 pgo:query("select 'sad'::mood;", [])),
    ?assertMatch(#{command := select, rows := [{{mood, <<"sad">>}}]},
                 pgo:query("select $1::mood;", [<<"sad">>])),
    ?assertMatch(#{command := select, rows := [{{array, [{mood, <<"sad">>}]}}]},
                 pgo:query("select '{sad}'::mood[];")),
    ?assertMatch(#{command := select, rows := [{{array, [{mood, <<"sad">>}]}}]},
                 pgo:query("select $1::mood[];", [{array, [<<"sad">>]}])).

insert(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table moods (a_mood mood)")),

    ?assertMatch(#{command := insert},
                 pgo:query("insert into moods (a_mood) VALUES ($1)",
                           [<<"sad">>])),

    ?assertMatch(#{command := select,
                   rows := [{{mood, <<"sad">>}}]},
                 pgo:query("select a_mood from moods")).
