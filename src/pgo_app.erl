%%%-------------------------------------------------------------------
%% @doc pgo application
%% @end
%%%-------------------------------------------------------------------
-module(pgo_app).

-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    {ok, Vsn} = application:get_key(pgo, vsn),
    _ = opentelemetry:register_tracer(pgo, Vsn),

    pgo_query_cache:start_link(),
    Pools = application:get_env(pgo, pools, []),
    {ok, Pid} = pgo_sup:start_link(),
    [{ok, _} = pgo_sup:start_child(Name, PoolConfig) || {Name, PoolConfig} <- Pools],
    {ok, Pid}.

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
