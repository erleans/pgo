%%%-------------------------------------------------------------------
%% @doc pgsql_pool top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(pp_broker_sup).

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Name, PoolConfig) ->
    supervisor:start_link(?MODULE, [Name, PoolConfig]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Name, PoolConfig]) ->
    PoolSize = proplists:get_value(size, PoolConfig, 5),
    NumWorkers = proplists:get_value(workers, PoolConfig, 1),
    DBOptions = proplists:get_value(postgres, PoolConfig, []),

    WorkerPoolSizes = worker_pool_sizes(PoolSize, NumWorkers),

    Broker = {pp_broker, {pp_broker, start_link, [Name]},
              permanent, 5000, worker, [pp_broker]},
    Workers = [{{pp_worker, X}, {pp_worker, start_link, [Name, Size, DBOptions]},
                permanent, 5000, worker, []} || {X, Size} <- WorkerPoolSizes],

    {ok, {{rest_for_one, 5, 10}, [Broker | Workers]}}.

%%====================================================================
%% Internal functions
%%====================================================================

-spec worker_pool_sizes(pos_integer(), pos_integer()) -> [{pos_integer(), pos_integer()}].
worker_pool_sizes(PoolSize, NumWorkers) ->
    Remainder = PoolSize rem NumWorkers,
    Size = PoolSize div NumWorkers,
    [{1, Size + Remainder} | [{X, Size} || X <- lists:seq(2, NumWorkers)]].


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

worker_pool_sizes_test() ->
    WorkerPoolSizes = worker_pool_sizes(5, 2),
    ?assertEqual([{1, 3}, {2, 2}],  WorkerPoolSizes),

    WorkerPoolSizes1 = worker_pool_sizes(10, 5),
    ?assertEqual([{1, 2}, {2, 2}, {3, 2}, {4, 2}, {5, 2}],  WorkerPoolSizes1).

-endif.
