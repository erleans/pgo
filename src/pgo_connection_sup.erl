-module(pgo_connection_sup).

-behaviour(supervisor).

-export([start_link/4,
         start_child/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Queue, PoolPid, PoolName, DBOptions) ->
    supervisor:start_link(?MODULE, [Queue, self(), PoolPid, PoolName, DBOptions]).

start_child(Sup) ->
    supervisor:start_child(Sup, []).

init([Queue, SupPid, PoolPid, PoolName, DBOptions]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpecs = [#{id => pgo_connection,
                    start => {pgo_connection, start_link, [Queue, PoolPid, PoolName, SupPid, DBOptions, []]},
                    shutdown => 100}],
    {ok, {SupFlags, ChildSpecs}}.
