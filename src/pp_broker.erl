-module(pp_broker).

-behaviour(sbroker).

-export([start_link/1]).

-export([init/1]).

start_link(Name) ->
    sbroker:start_link({local, Name}, ?MODULE, [], [{read_time_after, 2}]).

init(_) ->
    QueueSpec = {sbroker_timeout_queue, {out, 5000, drop, 128}},
    WorkerSpec = {sbroker_drop_queue, {out_r, drop, infinity}},
    {ok, {QueueSpec, WorkerSpec, 100}}.
