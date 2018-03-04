-module(pgo).

-export([start_pool/2,
         query/1,
         query/2,
         query/3,
         checkout/1,
         checkin/1,
         break/1]).

start_pool(Name, PoolConfig) ->
    pgo_sup:start_child(Name, PoolConfig).

query(Query) ->
    query(default, Query).

query(Pool, Query) when is_atom(Pool) ->
    {ok, Socket, Ref} = checkout(Pool),
    try
        pgo_handler:simple_query(Socket, Pool, Query)
    after
        pgo_connection:done(Ref)
    end;
query(Query, Params) ->
    query(default, Query, Params).

query(Pool, Query, Params) ->
    {ok, Socket, Ref} = checkout(Pool),
    try
        pgo_handler:extended_query(Socket, Pool, Query, Params)
    after
        pgo_connection:done(Ref)
    end.

checkout(Name) ->
    case sbroker:ask(Name) of
        {go, Ref,  {Pid, C}, _, _} ->
            {ok, C, {Pid, Ref}};
        {drop, N} ->
            {drop, N}
    end.

checkin(Ref) ->
    pgo_connection:done(Ref).

break(Ref) ->
    pgo_connection:break(Ref).
