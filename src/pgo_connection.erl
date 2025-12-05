-module(pgo_connection).

-behaviour(gen_statem).

-export([start_link/5,
         ping/3,
         stop/3,
         disconnect/4,
         pool_name/1,
         reload_types/1,
         break/1,
         break/2,
         report_cb/1]).

-export([init/1,
         callback_mode/0,
         connected/3,
         disconnected/3,
         enqueued/3,
         dequeued/3,
         terminate/3]).

-include("pgo_internal.hrl").
-include_lib("kernel/include/logger.hrl").

-record(data, {monitor :: reference() | undefined,
               ref :: reference() | undefined,
               queue :: reference(),
               conn :: #conn{} | undefined,
               pool_config :: pgo:pool_config(),
               holder :: ets:tid() | undefined,
               broker :: atom(),
               pool :: pid(),
               sup :: pid(),
               backoff :: backoff:backoff()}).

start_link(QueueTid, PoolPid, PoolName, Sup, PoolConfig) ->
    gen_statem:start_link(?MODULE, {QueueTid, PoolPid, PoolName, Sup, PoolConfig}, []).

pool_name({_Pid, Holder}) ->
    [PoolName] = ets:lookup(Holder, pool_name),
    PoolName.

-spec ping(pid(), ets:tid(), pgo_pool:conn()) -> ok.
ping(Pid, Holder, _Conn) ->
    gen_statem:cast(Pid, {ping, Holder}).

-spec stop(pid(), ets:tid(), pgo_pool:conn()) -> ok.
stop(Pid, Holder, _Conn) ->
    gen_statem:cast(Pid, {stop, Holder}).

-spec disconnect(pid(), ets:tid(), {error, atom()}, pg_pool:conn()) -> ok.
disconnect(Pid, Holder, _Err, _Conn) ->
    %% maybe log Err?
    gen_statem:cast(Pid, {disconnect, Holder}).

-spec reload_types(pgo_pool:conn()) -> ok.
reload_types(#conn{owner=Pid}) ->
    gen_statem:call(Pid, reload_types).

-spec break(pgo_pool:conn(), pgo_pool:ref()) -> ok.
break(#conn{owner=Pid}, {_Pool, _Ref, _Deadline, Holder}) ->
    gen_statem:cast(Pid, {break, Holder}).

-spec break(pgo_pool:conn()) -> ok.
break(#conn{owner=Pid}) ->
    gen_statem:cast(Pid, break).

init({QueueTid, Pool, PoolName, Sup, PoolConfig}) ->
    erlang:process_flag(trap_exit, true),
    B = backoff:init(1000, 10000),
    {ok, disconnected, #data{backoff=B,
                             pool=Pool,
                             broker=PoolName,
                             queue=QueueTid,
                             sup=Sup,
                             pool_config=PoolConfig},
     {next_event, internal, connect}}.

callback_mode() ->
    state_functions.

disconnected(EventType, _, Data=#data{broker=Broker,
                                      backoff=B,
                                      queue=QueueTid,
                                      pool=Pool,
                                      pool_config=PoolConfig}) when EventType =:= internal
                                                                    ; EventType =:= timeout
                                                                    ; EventType =:= state_timeout ->
    try pgo_handler:open(Broker, PoolConfig) of
        {ok, Conn} ->
            Holder = pgo_pool:update(Pool, QueueTid, ?MODULE, Conn),
            {_, B1} = backoff:succeed(B),
            {next_state, enqueued, Data#data{conn=Conn,
                                             holder=Holder,
                                             backoff=B1}};
        {error, Error} ->
            ?LOG_DEBUG("full error connecting to database: ~p", [Error]),
            ?LOG_INFO(#{at => connecting,
                        reason => Error},
                      #{report_cb => fun ?MODULE:report_cb/1}),
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, Data#data{broker=Broker,
                                                 holder=undefined,
                                                 backoff=B1},
             [{state_timeout, Backoff, connect}]}
    catch
        throw:Reason ->
            ?LOG_INFO(#{at => connecting,
                        reason => Reason},
                      #{report_cb => fun ?MODULE:report_cb/1}),
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, Data#data{broker=Broker,
                                                 holder=undefined,
                                                 backoff=B1},
             [{state_timeout, Backoff, connect}]}
    end;
disconnected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

connected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

enqueued(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

dequeued(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

handle_event(cast, {ping, Holder}, Data=#data{pool=Pool,
                                              holder=Holder,
                                              queue=QueueTid,
                                              conn=Conn}) ->
    case pgo_handler:ping(Conn) of
        ok ->
            NewHolder = pgo_pool:update(Pool, QueueTid, ?MODULE, Conn),
            {keep_state, Data#data{holder=NewHolder}};
        {error, Reason} ->
            ?LOG_INFO(#{at => ping,
                        reason => Reason},
                      #{report_cb => fun ?MODULE:report_cb/1}),

            close_and_reopen(Data)
    end;
%% ignore `ping' for a different holder -- means it is an old message
handle_event(cast, {ping, _}, _Data) ->
    keep_state_and_data;
handle_event(cast, {stop, Holder}, Data=#data{holder=Holder,
                                              conn=Conn}) ->
    pgo_handler:close(Conn),
    {stop, Data#data{conn=undefined,
                     holder=undefined}};
%% ignore `stop' for a different holder -- means it is an old message
handle_event(cast, {stop, _}, _Data) ->
    keep_state_and_data;
handle_event(cast, {disconnect, Holder}, Data=#data{holder=Holder}) ->
    close_and_reopen(Data);
%% ignore `disconnect' for a different holder -- means it is an old message
handle_event(cast, {disconnect, _}, _Data) ->
    keep_state_and_data;
handle_event(cast, {break, Holder}, Data=#data{holder=Holder}) ->
    close_and_reopen(Data);
handle_event(cast, break, Data) ->
    close_and_reopen(Data);
handle_event({call, From}, reload_types, #data{sup=Sup}) ->
    TypeServer = pgo_pool_sup:whereis_child(Sup, type_server),
    pgo_type_server:reload(TypeServer),
    {keep_state_and_data, [{reply, From, ok}]};
handle_event(info, {'EXIT', Socket, _Reason}, Data=#data{conn=#conn{socket=Socket}}) ->
    %% socket died, go to disconnected state
    close_and_reopen(Data);
%% ignore `EXIT' for a different Socket -- means it is an old message
handle_event(info, {'EXIT', _, _Reason}, _Data) ->
    keep_state_and_data;
%% nothing to do for `ssl_closed' -- it should be handled by the `EXIT' handling
handle_event(info, {ssl_closed, _}, _Data) ->
    keep_state_and_data.

-doc hidden.
terminate(_Reason, _, #data{conn=undefined}) ->
    ok;
terminate(_Reason, _, #data{conn=Conn}) ->
    pgo_handler:close(Conn),
    ok.

%%

close_and_reopen(Data=#data{conn=Conn}) ->
    pgo_handler:close(Conn),
    {next_state, disconnected, Data#data{conn=undefined,
                                         holder=undefined},
     [{next_event, internal, connect}]}.

report_cb(#{at := ping,
            reason := Reason}) ->
    {"disconnecting after database failed ping with reason ~p", [Reason]};
report_cb(#{at := connecting,
            reason := {pgo_error, #{message := Message}}}) ->
    {"error connecting to database: ~s", [Message]};
report_cb(#{at := connecting,
            reason := Reason}) ->
    {"unknown error when connecting to database: ~p", [Reason]}.
