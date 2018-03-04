-module(pgo_type_server).

-export([start_link/1,
         reload/1]).

-export([init/1,
         callback_mode/0,
         ready/3]).

-include("pgo.hrl").

-record(data, {pool}).

start_link(Pool) ->
    gen_statem:start_link(?MODULE, [Pool], []).

reload(Pid) ->
    gen_statem:cast(Pid, reload).

init([Pool]) ->
    ets:new(Pool, [named_table, protected, {read_concurrency, true}]),
    {ok, ready, #data{pool=Pool},
     {next_event, internal, load}}.

callback_mode() ->
    state_functions.

ready(internal, load, #data{pool=Pool}) ->
    load(Pool),
    keep_state_and_data;
ready(cast, reload, #data{pool=Pool}) ->
    load(Pool),
    keep_state_and_data.

load(Pool) ->
    #pg_result{rows=Oids} = pgo:query(Pool, "SELECT oid, typname FROM pg_type"),
    [ets:insert(Pool, {Oid, binary_to_atom(Typename, utf8)}) || {Oid, Typename} <- Oids].
