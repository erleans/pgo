-module(pp).

-export([start_pool/2,
         stop_pool/1,
         checkout/1,
         checkin/1,
         break/1]).

start_pool(Name, PoolConfig) ->
    pgsql_pool_sup:start_child(Name, PoolConfig).

stop_pool(Name) ->
    supervisor:terminate_child(pgsql_pool_sup, Name).

checkout(Name) ->
    case sbroker:ask(Name) of
        {go, Ref, {Pid, C}, _, _} ->
            {ok, C, {Pid, Ref}};
        {drop, N} ->
            {drop, N}
    end.

checkin(Ref) ->
    pp_worker:done(Ref).

break(Ref) ->
    pp_worker:break(Ref).
