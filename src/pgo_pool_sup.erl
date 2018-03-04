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
    Children = [#{id => broker,
                  start => {pgo_broker, start_link, [Name]},
                  restart => transient,
                  shutdown => 5000,
                  type => worker,
                  modules => [pgo_broker]},
                #{id => type_server,
                  start => {pgo_type_server, start_link, [Name]},
                  restart => transient,
                  shutdown => 5000,
                  type => worker,
                  modules => []},
                #{id => connection_sup,
                  start => {pgo_connection_sup, start_link, [Name, PoolConfig]},
                  restart => transient,
                  shutdown => 5000,
                  type => supervisor,
                  modules => []},
                #{id => connection_starter,
                  start => {pgo_connection_starter, start_link, [Name, Size]},
                  restart => transient,
                  shutdown => 5000,
                  type => worker,
                  modules => []}],
    {ok, {{one_for_one, 5, 10}, Children}}.
