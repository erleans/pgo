-module(pgo_app).
-moduledoc """
pgo application
""".

-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
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
