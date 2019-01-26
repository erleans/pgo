-module(pgo_query_opts_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [default_rows_as_maps].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, [{size, 1}, {database, "test"}, {user, "test"}],
                                  [{default_query_opts, [return_rows_as_maps]}]),

    Config.

end_per_suite(_Config) ->
    pgo:query("drop table tmp"),
    pgo:query("drop table tmp_b"),
    pgo:query("drop table foo"),
    pgo:query("drop table types"),

    application:stop(pgo),
    ok.

default_rows_as_maps(_Config) ->
    ?assertMatch(#{command := create},
                 pgo:query("create temporary table foo_1 (id integer primary key, some_text text)")),
    ?assertMatch(#{command := insert},
                 pgo:query("insert into foo_1 (id, some_text) values (1, 'hello')")),

    ?assertMatch(#{command := select,rows := [#{<<"id">> := 1,<<"some_text">> := <<"hello">>}]},
                 pgo:query("select * from foo_1")).
