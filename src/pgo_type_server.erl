-module(pgo_type_server).

-export([start_link/2,
         reload/1,
         reload_cast/1]).

-export([init/1,
         callback_mode/0,
         ready/3,
         terminate/3]).

-include_lib("pg_datatypes/include/pg_datatypes.hrl").

-record(data, {pool        :: atom(),
               pool_config  :: pgo:pool_config(),
               last_reload :: integer() | undefined}).

start_link(Pool, PoolConfig) ->
    gen_statem:start_link(?MODULE, [Pool, PoolConfig], []).

reload(Pid) ->
    gen_statem:call(Pid, {reload, erlang:monotonic_time()}).

reload_cast(Pid) ->
    gen_statem:cast(Pid, {reload, erlang:monotonic_time()}).

init([Pool, PoolConfig]) ->
    erlang:process_flag(trap_exit, true),
    ets:new(Pool, [named_table, protected, {keypos, 2}]),
    {ok, ready, #data{pool=Pool, pool_config=PoolConfig},
     {next_event, internal, load}}.

callback_mode() ->
    state_functions.

ready(internal, load, Data=#data{pool=Pool,
                                 pool_config=PoolConfig}) ->
    case load(Pool, -1, 0, PoolConfig) of
        failed ->
            %% not using a timer because this initial load, so want to block
            timer:sleep(500),
            {keep_state_and_data, [{next_event, internal, load}]};
        _ ->
            {keep_state, Data#data{last_reload=erlang:monotonic_time()}}
    end;
ready({call, From}, {reload, RequestTime}, Data=#data{pool=Pool,
                                                      pool_config=PoolConfig,
                                                      last_reload=LastReload}) ->
    load(Pool, LastReload, RequestTime, PoolConfig),
    {keep_state, Data#data{last_reload=erlang:monotonic_time()}, [{reply, From, ok}]};
ready(cast, {reload, RequestTime}, Data=#data{pool=Pool,
                                              pool_config=PoolConfig,
                                              last_reload=LastReload}) ->
    load(Pool, LastReload, RequestTime, PoolConfig),
    {keep_state, Data#data{last_reload=erlang:monotonic_time()}};
ready(_, _, _Data) ->
    keep_state_and_data.

terminate(_, _, #data{pool=Pool}) ->
    ets:delete(Pool).

load(Pool, LastReload, RequestTime, PoolConfig) when LastReload < RequestTime ->
    try pgo_handler:open(Pool, PoolConfig) of
        {ok, Conn} ->
            load_and_update_types(Conn, Pool),
            pg_datatypes:update(Pool);
        {error, _} ->
            failed
    catch
        _:_ ->
            failed
    end;
load(_, _, _, _) ->
    ok.

-define(BOOTSTRAP_QUERY, ["SELECT oid, typname, typsend::text, typreceive::text,"
                          "typoutput::text, typinput::text, typelem FROM pg_type"]).

load_and_update_types(Conn, Pool) ->
    try
        #{rows := Oids} = pgo_handler:extended_query(Conn, ?BOOTSTRAP_QUERY, [],
                                                     [no_reload_types], #{queue_time => undefined}),
        [ets:insert(Pool, #type_info{oid=Oid,
                                     name=binary:copy(Name),
                                     typsend=binary:copy(Send),
                                     typreceive=binary:copy(Receive),
                                     output=binary:copy(Output),
                                     input=binary:copy(Input),
                                     array_elem=ArrayOid})
         || {Oid, Name, Send, Receive, Output, Input, ArrayOid} <- Oids]
    catch
        _:_ ->
            failed
    after
        pgo_handler:close(Conn)
    end.
