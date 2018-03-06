-module(pgo_connection).

-behaviour(gen_statem).

-export([start_link/4,
         reload_types/1,
         done/1,
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
               conn :: gen_tcp:socket(),
               db_options :: list(),
               broker :: atom(),
               sup :: pid(),
               backoff :: backoff:backoff()}).

start_link(Sup, Broker, Args, Opts) ->
    gen_statem:start_link(?MODULE, {Sup, Broker, Args}, Opts).

reload_types(Pid) ->
    gen_statem:call(Pid, reload_types).

done({Pid, Ref}) ->
    gen_statem:cast(Pid, {done, Ref});
done(undefined) ->
    ok.

break({Pid, Ref}) ->
    gen_statem:cast(Pid, {break, Ref});
break(undefined) ->
    ok.

init({Sup, Broker, Settings}) ->
    erlang:process_flag(trap_exit, true),
    B = backoff:init(1000, 10000),
    {ok, disconnected, #data{broker=Broker,
                             backoff=B,
                             ref=make_ref(),
                             sup=Sup,
                             db_options=Settings},
     {next_event, internal, connect}}.

callback_mode() ->
    state_functions.

disconnected(EventType, _, Data=#data{broker=Broker,
                                      backoff=B,
                                      ref=Ref,
                                      db_options=DBOptions}) when EventType =:= internal
                                                                  ; EventType =:= timeout
                                                                  ; EventType =:= state_timeout ->
    try pgo_handler:pgsql_open(DBOptions) of
        {ok, Socket} ->
            enqueue(Broker, Socket, Ref),
            {_, B1} = backoff:succeed(B),
            {next_state, enqueued, Data#data{conn=Socket, backoff=B1}};
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

connected(internal, {enqueue, Socket}, Data=#data{broker=Broker,
                                                  ref=Ref}) ->
    enqueue(Broker, Socket, Ref),
    {next_state, enqueued, Data#data{}};
connected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

enqueued(info, {_, {go, Ref, Pid, _, _}}, Data=#data{}) ->
    Mon = erlang:monitor(process, Pid),
    {next_state, dequeued, Data#data{ref=Ref, monitor=Mon}};
enqueued(info, {Pid, {drop, _}}, Data) ->
    {next_state, connected, Data, {next_event, internal, {enqueue, Pid}}};
enqueued(info, {'EXIT', Pid, _}, Data=#data{broker=Broker}) ->
    cancel_or_await(Broker, Pid),
    {next_state, disconnected, Data#data{ref=undefined,
                                         monitor=undefined},
     {next_event, internal, connect}};
enqueued(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

dequeued(_, {done, _Ref}, Data=#data{monitor=Monitor,
                                     ref=Ref,
                                     broker=Broker,
                                     conn=Socket}) ->
    erlang:demonitor(Monitor, [flush]),
    enqueue(Broker, Socket, Ref),
    {next_state, enqueued, Data};
dequeued(info, {'DOWN', Mon, process, _Pid, _}, Data=#data{conn=Conn,
                                                           monitor=Mon}) ->
    {next_state, connected, Data#data{ref=undefined,
                                      monitor=undefined},
     {next_event, internal, {enqueue, Conn}}};
dequeued(info, {'EXIT', Pid, _}, Data=#data{conn=Pid}) ->
    {next_state, disconnected, Data#data{ref=undefined,
                                         monitor=undefined},
     {next_event, internal, connect}};
dequeued(cast, {break, _Ref}, Data=#data{monitor=Monitor,
                                         conn=Socket}) ->
    erlang:demonitor(Monitor, [flush]),
    erlang:unlink(Socket),
    pgo_handler:close(Socket),
    {next_state, disconnected, Data,
     [{next_event, internal, connect}]};
dequeued(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

cancel_or_await(Broker, Tag) ->
    case sbroker:cancel(Broker, Tag, 5000) of
        false ->
            sbroker:await(Tag, 0);
        _ ->
            cancelled
    end.

handle_event({call, From}, reload_types, #data{sup=Sup}) ->
    TypeServer = pgo_pool_sup:whereis_child(Sup, type_server),
    pgo_type_server:reload(TypeServer),
    {keep_state_and_data, [{reply, From, ok}]};
handle_event(_, _, _) ->
    keep_state_and_data.

%% @private
terminate(_Reason, _, _) ->
    ok.

enqueue(Broker, Conn, Ref) ->
    {await, Ref, _} = sbroker:async_ask_r(Broker, {self(), Conn}, {self(), Ref}).
