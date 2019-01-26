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
    pgo_query_cache:start_link(),
    Pools = application:get_env(pgo, pools, []),
    {ok, Pid} = pgo_sup:start_link(),
    [{ok, _} = pgo_sup:start_child(Name, PoolConfig, []) || {Name, PoolConfig} <- Pools],
    [{ok, _} = pgo_sup:start_child(Name, PoolConfig, Options) || {Name, PoolConfig, Options} <- Pools],
    {ok, Pid}.


                %% #{id => query_cache ,
                %%   start => {pgo_query_cache, start_link, [PoolName, DBConfig]},
                %%   type => worker,
                %%   shutdown => 5000}
%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
