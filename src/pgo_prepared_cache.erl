-module(pgo_prepared_cache).

-export([init/0, store/3, lookup/1]).

-define(TABLE, pgo_prepared_cache).

init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ok
    end.

-spec store(iodata(), iodata(), [pg_types:oid()]) -> ok.
store(Name, Query, OIDs) ->
    ets:insert(?TABLE, {iolist_to_binary(Name), iolist_to_binary(Query), OIDs}),
    ok.

-spec lookup(iodata()) -> {ok, binary(), [pg_types:oid()]} | not_found.
lookup(Name) ->
    case ets:lookup(?TABLE, iolist_to_binary(Name)) of
        [{_, Query, OIDs}] -> {ok, Query, OIDs};
        [] -> not_found
    end.
