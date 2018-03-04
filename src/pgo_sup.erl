-module(pgo_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_child/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Name, PoolConfig) ->
    supervisor:start_child(?SERVER, [Name, PoolConfig]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [#{id => pgo_pool_sup,
                    start => {pgo_pool_sup, start_link, []},
                    shutdown => 1}],
    {ok, {SupFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================
