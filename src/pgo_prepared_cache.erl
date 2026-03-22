-module(pgo_prepared_cache).
-moduledoc """
ETS-based cache for prepared statement metadata.

Stores statement name → {query, parameter OIDs} mappings, and tracks
which connections have each statement prepared.
""".

-export([init/0, store/3, lookup/1, is_conn_prepared/1, mark_conn_prepared/1]).

-define(TABLE, pgo_prepared_cache).
-define(CONN_TABLE, pgo_prepared_conn_cache).

-doc "Initialize cache tables. Safe to call multiple times.".
init() ->
    init_table(?TABLE),
    init_table(?CONN_TABLE).

-doc "Store a prepared statement's query and parameter OIDs.".
-spec store(iodata(), iodata(), [pg_types:oid()]) -> ok.
store(Name, Query, OIDs) ->
    ets:insert(?TABLE, {iolist_to_binary(Name), iolist_to_binary(Query), OIDs}),
    ok.

-doc "Look up a prepared statement's query and OIDs by name.".
-spec lookup(iodata()) -> {ok, binary(), [pg_types:oid()]} | not_found.
lookup(Name) ->
    case ets:lookup(?TABLE, iolist_to_binary(Name)) of
        [{_, Query, OIDs}] -> {ok, Query, OIDs};
        [] -> not_found
    end.

-doc "Check if a statement has been prepared on a specific connection.".
-spec is_conn_prepared({pid(), binary()}) -> boolean().
is_conn_prepared(Key) ->
    ets:member(?CONN_TABLE, Key).

-doc "Mark a statement as prepared on a specific connection.".
-spec mark_conn_prepared({pid(), binary()}) -> ok.
mark_conn_prepared(Key) ->
    ets:insert(?CONN_TABLE, {Key}),
    ok.

init_table(Name) ->
    case ets:whereis(Name) of
        undefined ->
            ets:new(Name, [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ok
    end.
