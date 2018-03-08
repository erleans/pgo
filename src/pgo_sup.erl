-module(pgo_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_child/1,
         start_child/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child({Name, PoolConfig}) ->
    supervisor:start_child(?SERVER, [Name, PoolConfig]).

start_child(Name, PoolConfig) ->
    supervisor:start_child(?SERVER, [Name, PoolConfig]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpec = #{id => pgo_pool,
                  start => {pgo_pool, start_link, []},
                  shutdown => 1000},
    {ok, {SupFlags, [ChildSpec]}}.

%%====================================================================
%% Internal functions
%%====================================================================
