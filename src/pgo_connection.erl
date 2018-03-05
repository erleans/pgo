-module(pgo_connection).

-behaviour(gen_statem).

-export([start_link/6,
         ping/2,
         reload_types/1,
         break/1]).

-export([init/1,
         callback_mode/0,
         connected/3,
         disconnected/3,
         enqueued/3,
         dequeued/3,
         terminate/3]).

-record(data, {monitor :: reference(),
               ref :: reference(),
               queue :: reference(),
               conn :: gen_tcp:socket(),
               db_options :: list(),
               holder :: reference(),
               broker :: atom(),
               pool :: pid(),
               sup :: pid(),
               backoff :: backoff:backoff()}).

start_link(Queue, PoolPid, PoolName, Sup, Args, Opts) ->
    gen_statem:start_link(?MODULE, {Queue, PoolPid, PoolName, Sup, Args}, Opts).

ping({Pid, Holder}, _) ->
    gen_statem:cast(Pid, {ping, Holder}).

reload_types(Pid) ->
    gen_statem:call(Pid, reload_types).

break({Pid, Ref}) ->
    gen_statem:cast(Pid, {break, Ref});
break(undefined) ->
    ok.

init({Queue, Pool, PoolName, Sup, Settings}) ->
    erlang:process_flag(trap_exit, true),
    B = backoff:init(1000, 10000),
    {ok, disconnected, #data{backoff=B,
                             pool=Pool,
                             broker=PoolName,
                             queue=Queue,
                             sup=Sup,
                             db_options=Settings},
     {next_event, internal, connect}}.

callback_mode() ->
    state_functions.

disconnected(EventType, _, Data=#data{broker=Broker,
                                      backoff=B,
                                      queue=Queue,
                                      pool=Pool,
                                      db_options=DBOptions}) when EventType =:= internal
                                                                  ; EventType =:= timeout
                                                                  ; EventType =:= state_timeout ->
    try pgo_handler:pgsql_open(DBOptions) of
        {ok, Socket} ->
            Holder = pgo_pool:update(Pool, Queue, {self(), Socket}),
            {_, B1} = backoff:succeed(B),
            {next_state, enqueued, Data#data{conn=Socket, holder=Holder, backoff=B1}};
        _Error ->
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, #data{broker=Broker,
                                             backoff=B1,
                                             db_options=DBOptions},
             [{state_timeout, Backoff, connect}]}
    catch
        throw:_Reason ->
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, #data{broker=Broker,
                                             backoff=B1,
                                             db_options=DBOptions},
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

handle_event(cast, {ping, _Holder}, Data=#data{pool=Pool,
                                               queue=Queue,
                                               conn=Socket}) ->
    NewHolder = pgo_pool:update(Pool, Queue, {self(), Socket}),
    {keep_state, Data#data{holder=NewHolder}};
handle_event(cast, {break, _Holder}, Data=#data{conn=Socket}) ->
    erlang:unlink(Socket),
    pgo_handler:close(Socket),
    {next_state, disconnected, Data,
     [{next_event, internal, connect}]};
handle_event({call, From}, reload_types, #data{sup=Sup}) ->
    TypeServer = pgo_pool_sup:whereis_child(Sup, type_server),
    pgo_type_server:reload(TypeServer),
    {keep_state_and_data, [{reply, From, ok}]};
handle_event(info, {'EXIT', Socket, _Reason}, Data=#data{conn=Socket}) ->
    %% socket died, go to disconnected state
    {next_state, disconnected, Data#data{conn=undefined},
     [{next_event, internal, connect}]}.

%% @private
terminate(_Reason, _, _) ->
    ok.
