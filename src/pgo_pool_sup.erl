-module(pgo_pool_sup).

-behaviour(supervisor).

-export([start_link/3,
         whereis_child/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Queue, Pool, PoolConfig) ->
    supervisor:start_link(?MODULE, [Queue, self(), Pool, PoolConfig]).

whereis_child(Sup, Id) ->
    Children = supervisor:which_children(Sup),
    {_, Pid, _, _}  = lists:keyfind(Id, 1, Children),
    Pid.

init([Queue, PoolPid, PoolName, PoolConfig]) ->
    Size = proplists:get_value(size, PoolConfig, 5),
    DBConfig = proplists:get_value(postgres, PoolConfig, []),
    Children = [#{id => connection_sup,
                  start => {pgo_connection_sup, start_link, [Queue, PoolPid, PoolName, DBConfig]},
                  type => supervisor,
                  shutdown => 5000},
                #{id => connection_starter,
                  start => {pgo_connection_starter, start_link, [PoolPid, Size]},
                  type => worker,
                  shutdown => 5000},
                #{id => type_server,
                  start => {pgo_type_server, start_link, [PoolName, DBConfig]},
                  type => worker,
                  shutdown => 5000}],
    {ok, {{rest_for_one, 5, 10}, Children}}.
