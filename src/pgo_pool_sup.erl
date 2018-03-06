-module(pgo_pool_sup).

-behaviour(supervisor).

-export([start_link/2,
         whereis_child/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Name, PoolConfig) ->
    supervisor:start_link(?MODULE, [Name, PoolConfig]).

whereis_child(Sup, Id) ->
    Children = supervisor:which_children(Sup),
    {_, Pid, _, _}  = lists:keyfind(Id, 1, Children),
    Pid.

init([Name, PoolConfig]) ->
    Size = proplists:get_value(size, PoolConfig, 5),
    DBConfig = proplists:get_value(postgres, PoolConfig, []),
    Children = [#{id => broker,
                  start => {pgo_broker, start_link, [Name]},
                  shutdown => 5000},
                #{id => type_server,
                  start => {pgo_type_server, start_link, [Name, DBConfig]},
                  shutdown => 5000},
                #{id => connection_sup,
                  start => {pgo_connection_sup, start_link, [Name, DBConfig]},
                  shutdown => 5000},
                #{id => connection_starter,
                  start => {pgo_connection_starter, start_link, [Name, Size]},
                  shutdown => 5000,
                  type => worker}],
    {ok, {{rest_for_one, 5, 10}, Children}}.
