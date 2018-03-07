-module(pgo_query_cache).

-export([start_link/0,
         lookup/2,
         insert/3,
         reset/0,
         delete/1]).

-export([init/1,
         callback_mode/0,
         ready/3,
         terminate/3]).

-include("pgo.hrl").

-record(data, {}).

start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

lookup(Pool, Query) ->
    case ets:lookup(?MODULE, {Pool, Query}) of
        [{_Query, RowDescription}] ->
            RowDescription;
        [] ->
            not_found
    end.

insert(Pool, Query, RowDescription) ->
    ets:insert(?MODULE, {{Pool, Query}, RowDescription}).

reset() ->
    gen_statem:call(?MODULE, reset).

delete(Query) ->
    gen_statem:cast(?MODULE, {delete, Query}).

init([]) ->
    erlang:process_flag(trap_exit, true),
    ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    {ok, ready, #data{}}.

callback_mode() ->
    state_functions.

ready(_, _, _Data=#data{}) ->
    keep_state_and_data.

terminate(_, _, #data{}) ->
    ok.
