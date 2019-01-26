-module(pgo_connection_sup).

-behaviour(supervisor).

-export([start_link/5,
         start_child/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Queue, PoolPid, PoolName, DBSettings, Options) ->
    supervisor:start_link(?MODULE, [Queue, self(), PoolPid, PoolName, DBSettings, Options]).

start_child(Sup) ->
    supervisor:start_child(Sup, []).

init([Queue, SupPid, PoolPid, PoolName, DBSettings, Options]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpecs = [#{id => pgo_connection,
                    start => {pgo_connection, start_link, [Queue, PoolPid, PoolName,
                                                           SupPid, DBSettings, Options]},
                    shutdown => 100}],
    {ok, {SupFlags, ChildSpecs}}.
