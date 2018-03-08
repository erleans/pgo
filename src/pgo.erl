-module(pgo).

-export([start_pool/2,
         query/1,
         query/2,
         query/3,
         checkout/1,
         checkin/2,
         break/1]).

-include("pgo.hrl").

-export_type([result/0]).
-type result() :: #pg_result{}.

start_pool(Name, PoolConfig) ->
    pgo_sup:start_child(Name, PoolConfig).

query(Query) ->
    query(default, Query).

query(Pool, Query) when is_atom(Pool) ->
    {ok, Ref, {Pid, Socket}} = checkout(Pool),
    try
        pgo_handler:simple_query({Pid, Socket}, Pool, Query)
    after
        checkin(Ref, {Pid, Socket})
    end;
query(Query, Params) ->
    query(default, Query, Params).

query(Pool, Query, Params) ->
    {ok, Ref, {Pid, Socket}} = checkout(Pool),
    try
        pgo_handler:extended_query({Pid, Socket}, Pool, Query, Params)
    after
        checkin(Ref, {Pid, Socket})
    end.

checkout(Pool) ->
    pgo_pool:checkout(Pool, [{queue, true}]).

checkin(Ref, Conn) ->
    pgo_pool:checkin(Ref, Conn, []).

break(Ref) ->
    pgo_connection:break(Ref).
