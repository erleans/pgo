%%%-------------------------------------------------------------------
%% @doc pgsql_pool top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(pgsql_pool_sup).

-behaviour(supervisor).

%% API
-export([start_link/1,
         start_child/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Pools) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Pools]).

start_child(Name, PoolConfig) ->
    ChildSpec = pool_spec(Name, PoolConfig),
    supervisor:start_child(?SERVER, ChildSpec).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Pools]) ->
    Children = [pool_spec(Name, PoolConfig) || {Name, PoolConfig} <- Pools],
    {ok, {{one_for_one, 5, 10}, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

pool_spec(Name, PoolConfig) ->
    #{id => Name,
      start => {pp_broker_sup, start_link, [Name, PoolConfig]},
      restart => permanent,
      shutdown => 5000,
      type => supervisor,
      modules => []}.
