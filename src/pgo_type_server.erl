-module(pgo_type_server).

-export([start_link/2,
         reload/1,
         reload_cast/1]).

-export([init/1,
         callback_mode/0,
         ready/3,
         terminate/3]).

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
    ets:new(Pool, [named_table, protected]),
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
    try pgo_handler:pgsql_open(Pool, PoolConfig) of
        {ok, Conn} ->
            load_and_update_types(Conn, Pool);
        {error, _} ->
            failed
    catch
        _:_ ->
            failed
    end;
load(_, _, _, _) ->
    ok.

load_and_update_types(Conn, Pool) ->
    try
        #{rows := Oids} = pgo_handler:extended_query(Conn, "SELECT oid, typname FROM pg_type", [],
                                                     [no_reload_types], #{queue_time => undefined}),
        [ets:insert(Pool, {Oid, binary_to_atom(Typename, utf8)}) || {Oid, Typename} <- Oids]
    catch
        _:_ ->
            failed
    after
        pgo_handler:close(Conn)
    end.
