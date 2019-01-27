-module(pgo_connection_sup).

-behaviour(supervisor).

-export([start_link/4,
         start_child/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(QueueTid, PoolPid, PoolName, PoolConfig) ->
    supervisor:start_link(?MODULE, [QueueTid, self(), PoolPid, PoolName, PoolConfig]).

start_child(Sup) ->
    supervisor:start_child(Sup, []).

init([QueueTid, SupPid, PoolPid, PoolName, PoolConfig]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpecs = [#{id => pgo_connection,
                    start => {pgo_connection, start_link, [QueueTid, PoolPid, PoolName,
                                                           SupPid, PoolConfig]},
                    shutdown => 100}],
    {ok, {SupFlags, ChildSpecs}}.
