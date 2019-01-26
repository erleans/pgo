-module(pgo_pool_sup).

-behaviour(supervisor).

-export([start_link/4,
         whereis_child/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Queue, Pool, DBSettings, Options) ->
    supervisor:start_link(?MODULE, [Queue, self(), Pool, DBSettings, Options]).

whereis_child(Sup, Id) ->
    Children = supervisor:which_children(Sup),
    {_, Pid, _, _}  = lists:keyfind(Id, 1, Children),
    Pid.

init([Queue, PoolPid, PoolName, DBSettings, Options]) ->
    Size = proplists:get_value(size, DBSettings, 5),
    Children = [#{id => connection_sup,
                  start => {pgo_connection_sup, start_link, [Queue, PoolPid, PoolName,
                                                             DBSettings, Options]},
                  type => supervisor,
                  shutdown => 5000},
                #{id => connection_starter,
                  start => {pgo_connection_starter, start_link, [PoolPid, Size]},
                  type => worker,
                  shutdown => 5000},
                #{id => type_server,
                  start => {pgo_type_server, start_link, [PoolName, DBSettings]},
                  type => worker,
                  shutdown => 5000}],
    {ok, {{rest_for_one, 5, 10}, Children}}.
