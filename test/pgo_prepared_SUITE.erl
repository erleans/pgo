-module(pgo_prepared_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [prepare_select,
     prepare_returns_oids,
     prepared_query_select,
     prepared_query_insert,
     prepared_query_with_params,
     prepared_query_multiple_rows,
     prepared_query_no_rows,
     prepared_query_wrong_params,
     prepare_invalid_sql,
     prepared_query_not_prepared,
     prepare_cache_stores_metadata,
     prepared_query_rows_as_maps,
     with_conn_prepare_and_query,
     auto_prepare_across_pool].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             port => 5432,
                                             database => "test",
                                             user => "test",
                                             password => "password"}),
    pgo:query("CREATE TABLE IF NOT EXISTS prepared_test ("
              "  id BIGSERIAL PRIMARY KEY,"
              "  name VARCHAR(255) NOT NULL,"
              "  value INTEGER"
              ")"),
    pgo:query("TRUNCATE prepared_test RESTART IDENTITY"),
    pgo:query("INSERT INTO prepared_test (name, value) VALUES ('alice', 10)"),
    pgo:query("INSERT INTO prepared_test (name, value) VALUES ('bob', 20)"),
    pgo:query("INSERT INTO prepared_test (name, value) VALUES ('charlie', 30)"),
    Config.

end_per_suite(_Config) ->
    pgo:query("DROP TABLE IF EXISTS prepared_test"),
    application:stop(pgo),
    ok.

init_per_testcase(TestCase, Config) ->
    %% Deallocate all prepared statements between tests
    pgo:query("DEALLOCATE ALL"),
    [{testcase, TestCase} | Config].

end_per_testcase(_, _Config) ->
    ok.

%%----------------------------------------------------------------------
%% prepare/2,3 tests
%%----------------------------------------------------------------------

prepare_select(_Config) ->
    {ok, _, OIDs} = pgo:prepare("test_select", "SELECT * FROM prepared_test WHERE id = $1"),
    ?assert(is_list(OIDs)),
    ?assertEqual(1, length(OIDs)).

prepare_returns_oids(_Config) ->
    {ok, Name, OIDs} = pgo:prepare("test_oids", "SELECT * FROM prepared_test WHERE id = $1 AND name = $2"),
    ?assertEqual("test_oids", Name),
    ?assertEqual(2, length(OIDs)),
    %% OIDs should be integers
    lists:foreach(fun(Oid) -> ?assert(is_integer(Oid)) end, OIDs).

prepare_invalid_sql(_Config) ->
    Result = pgo:prepare("bad_sql", "SELECTT * FROMM nonexistent"),
    ?assertMatch({error, {pgsql_error, _}}, Result).

prepare_cache_stores_metadata(_Config) ->
    pgo_prepared_cache:init(),
    {ok, _, _OIDs} = pgo:prepare("cached_stmt", "SELECT 1"),
    ?assertMatch({ok, _, _}, pgo_prepared_cache:lookup(<<"cached_stmt">>)).

%%----------------------------------------------------------------------
%% query_prepared/3,4 tests
%%----------------------------------------------------------------------

prepared_query_select(_Config) ->
    {ok, _, OIDs} = pgo:prepare("q_select", "SELECT id, name, value FROM prepared_test WHERE id = $1"),
    Result = pgo:query_prepared("q_select", [1], OIDs),
    ?assertMatch(#{command := select, num_rows := 1, rows := [{1, <<"alice">>, 10}]}, Result).

prepared_query_insert(_Config) ->
    {ok, _, OIDs} = pgo:prepare("q_insert", "INSERT INTO prepared_test (name, value) VALUES ($1, $2)"),
    Result = pgo:query_prepared("q_insert", [<<"dave">>, 40], OIDs),
    ?assertMatch(#{command := insert, num_rows := 1}, Result),
    %% Verify it was inserted
    ?assertMatch(#{rows := [{<<"dave">>, 40}]},
                 pgo:query("SELECT name, value FROM prepared_test WHERE name = $1", [<<"dave">>])),
    %% Clean up
    pgo:query("DELETE FROM prepared_test WHERE name = $1", [<<"dave">>]).

prepared_query_with_params(_Config) ->
    {ok, _, OIDs} = pgo:prepare("q_params", "SELECT name FROM prepared_test WHERE value > $1 ORDER BY value"),
    Result = pgo:query_prepared("q_params", [15], OIDs),
    ?assertMatch(#{command := select, rows := [{<<"bob">>}, {<<"charlie">>}]}, Result).

prepared_query_multiple_rows(_Config) ->
    {ok, _, OIDs} = pgo:prepare("q_multi", "SELECT id, name FROM prepared_test ORDER BY id"),
    Result = pgo:query_prepared("q_multi", [], OIDs),
    ?assertMatch(#{command := select, num_rows := 3}, Result).

prepared_query_no_rows(_Config) ->
    {ok, _, OIDs} = pgo:prepare("q_empty", "SELECT * FROM prepared_test WHERE id = $1"),
    Result = pgo:query_prepared("q_empty", [999], OIDs),
    ?assertMatch(#{command := select, num_rows := 0, rows := []}, Result).

prepared_query_wrong_params(_Config) ->
    {ok, _, OIDs} = pgo:prepare("q_wrong", "SELECT * FROM prepared_test WHERE id = $1"),
    %% Wrong number of parameters
    Result = pgo:query_prepared("q_wrong", [1, 2], OIDs),
    ?assertMatch({error, _}, Result).

prepared_query_not_prepared(_Config) ->
    %% Query a statement that doesn't exist on the connection
    Result = pgo:query_prepared("nonexistent_stmt", [1], [23]),
    ?assertMatch({error, {pgsql_error, _}}, Result).

prepared_query_rows_as_maps(_Config) ->
    {ok, _, OIDs} = pgo:prepare("q_maps", "SELECT id, name, value FROM prepared_test WHERE id = $1"),
    Result = pgo:query_prepared("q_maps", [1], OIDs, #{decode_opts => [return_rows_as_maps, column_name_as_atom]}),
    ?assertMatch(#{command := select, rows := [#{id := 1, name := <<"alice">>, value := 10}]}, Result).

%%----------------------------------------------------------------------
%% with_conn tests (prepare + query on same connection)
%%----------------------------------------------------------------------

with_conn_prepare_and_query(_Config) ->
    %% Use with_conn to ensure prepare and query happen on same connection
    Result = pgo:with_conn(default, fun() ->
        {ok, _, OIDs} = pgo:prepare("wc_test", "SELECT name FROM prepared_test WHERE id = $1"),
        pgo:query_prepared("wc_test", [2], OIDs)
    end),
    ?assertMatch(#{command := select, rows := [{<<"bob">>}]}, Result).

auto_prepare_across_pool(_Config) ->
    %% Start a pool with multiple connections
    {ok, _} = pgo_sup:start_child(multi_pool, #{pool_size => 5,
                                                 port => 5432,
                                                 database => "test",
                                                 user => "test",
                                                 password => "password"}),
    %% Prepare on one connection
    {ok, _, OIDs} = pgo:prepare("auto_prep_test",
                                "SELECT name FROM prepared_test WHERE id = $1",
                                #{pool => multi_pool}),
    %% Execute many times — will hit different connections, auto-prepare should kick in
    Results = [pgo:query_prepared("auto_prep_test", [I], OIDs, #{pool => multi_pool})
               || I <- lists:seq(1, 20)],
    %% All should succeed (no "statement not found" errors)
    lists:foreach(
        fun(R) -> ?assertMatch(#{command := select}, R) end,
        Results
    ),
    %% Verify correct data comes back
    ?assertMatch(#{rows := [{<<"alice">>}]},
                 pgo:query_prepared("auto_prep_test", [1], OIDs, #{pool => multi_pool})),
    ?assertMatch(#{rows := [{<<"bob">>}]},
                 pgo:query_prepared("auto_prep_test", [2], OIDs, #{pool => multi_pool})),
    application:stop(pgo),
    application:ensure_all_started(pgo),
    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             port => 5432,
                                             database => "test",
                                             user => "test",
                                             password => "password"}),
    ok.
