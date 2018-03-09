%% shamelessly stolen from James Fish's pool found in
%% db_connectin https://github.com/elixir-ecto/db_connection/pull/108
-module(pgo_pool).

-export([start_link/2,
         checkout/2,
         checkin/3,
         disconnect/4,
         stop/4,
         update/3]).

-export([init/1,
         handle_call/3,
         handle_info/2]).

-include("pgo_internal.hrl").

-define(TIMEOUT, 5000).
-define(QUEUE, true).
-define(QUEUE_TARGET, 50).
-define(QUEUE_INTERVAL, 1000).
-define(IDLE_INTERVAL, 1000).
-define(TIME_UNIT, 1000).
-define(HOLDER_KEY, '__info__').

-export_type([ref/0,
              conn/0]).

-type conn() :: #conn{}.
-type ref() :: {Pool :: pid(),
                Ref :: reference(),
                TimerRef :: reference() | undefined,
                Holder :: reference()}.

-ifdef(TEST).
-export([tid/1]).
%% return the ets table id so tests can check state
tid(Pool) ->
    gen_server:call(Pool, tid).
-endif.

start_link(Pool, Opts) ->
    gen_server:start_link({local, Pool}, ?MODULE, {Pool, Opts}, []).

-spec checkout(atom(), list()) -> {ok, ref(), conn()} | {drop, any()}.
checkout(Pool, Opts) ->
    MaybeQueue = proplists:get_value(queue, Opts, ?QUEUE),
    Now = erlang:monotonic_time(?TIME_UNIT),
    Timeout = abs_timeout(Now, Opts),
    case gen_server:call(Pool, {checkout, Now, MaybeQueue}, infinity) of
        {ok, Holder} ->
            recv_holder(Holder, Now, Timeout);
        {error, _Err} = Error ->
            Error
    end.

checkin({Pool, Ref, Deadline, Holder}, Conn, _) ->
    cancel_deadline(Deadline),
    Now = erlang:monotonic_time(?TIME_UNIT),
    checkin_holder(Holder, Pool, Conn, {checkin, Ref, Now}).

disconnect({Pool, Ref, Deadline, Holder}, Err, Conn, _) ->
    cancel_deadline(Deadline),
    checkin_holder(Holder, Pool, Conn, {disconnect, Ref, Err}).

stop({Pool, Ref, Deadline, Holder}, Err, Conn, _) ->
    cancel_deadline(Deadline),
    checkin_holder(Holder, Pool, Conn, {stop, Ref, Err}).

update(Pool, Ref, State) ->
    Holder = start_holder(Pool, Ref, State),
    Now = erlang:monotonic_time(?TIME_UNIT),
    checkin_holder(Holder, Pool, State, {checkin, Ref, Now}),
    Holder.

init({Pool, Opts}) ->
    process_flag(trap_exit, true),
    Queue = ets:new(?MODULE, [protected, ordered_set]),
    {ok, _} = pgo_pool_sup:start_link(Queue, Pool, Opts),

    Target = proplists:get_value(queue_target, Opts, ?QUEUE_TARGET),
    Interval = proplists:get_value(queue_interval, Opts, ?QUEUE_INTERVAL),
    IdleInterval = proplists:get_value(idle_interval, Opts, ?IDLE_INTERVAL),
    Now = erlang:monotonic_time(?TIME_UNIT),
    Codel = #{target => Target, interval => Interval, delay => 0, slow => false,
              next => Now, poll => undefined, idle_interval => IdleInterval, idle => undefined},
    Codel1 = start_idle(Now, Now, start_poll(Now, Now, Codel)),
    {ok, {busy, Queue, Codel1}}.

handle_call(tid, _From, {_, Queue, _} = D) ->
    {reply, Queue, D};
handle_call({checkout, Now, MaybeQueue}, From, {busy, Queue, _} = Busy) ->
    case MaybeQueue of
      true ->
        ets:insert(Queue, {{Now, erlang:unique_integer(), From}}),
        {noreply, Busy};
      false ->
        Message = "connection not available and queuing is disabled",
        {reply, {error, Message}, Busy}
    end;
handle_call({checkout, _Now, _MaybeQueue} = Checkout, From, Ready) ->
    {ready, Queue, Codel} = Ready,
    case ets:first(Queue) of
        {_Time, Holder} = Key ->
            checkout_holder(Holder, From, Queue) andalso ets:delete(Queue, Key),
            {noreply, Ready};
        '$end_of_table' ->
            handle_call(Checkout, From, {busy, Queue, Codel})
    end.

handle_info({'ETS-TRANSFER', Holder, _Pid, Queue}, {_, Queue, _} = Data) ->
    disconnect_holder(Holder, "client disconnected"),
    {noreply, Data};

handle_info({'ETS-TRANSFER', Holder, _, {Msg, Queue, Extra}}, {_, Queue, _} = Data) ->
    case Msg of
        checkin ->
            handle_checkin(Holder, Extra, Data);
        disconnect ->
            disconnect_holder(Holder, Extra),
            {noreply, Data};
        stop ->
            stop_holder(Holder, Extra),
            {noreply, Data}
    end;

handle_info({timeout, Deadline, {Queue, Holder, _Pid, _Len}}, {_, Queue, _} = Data) ->
    %% Check that timeout refers to current holder (and not previous)
    case
        ets:lookup_element(Holder, ?HOLDER_KEY, 3)
    of
        Deadline ->
            ets:update_element(Holder, ?HOLDER_KEY, {3, undefined}),
            Message = "client timed out",
            disconnect_holder(Holder, {error, Message});
        _ ->
            ok
    end,
    {noreply, Data};

handle_info({timeout, Poll, {Time, LastSent}}, {_, _, #{poll := Poll}} = Data) ->
    {Status, Queue, Codel} = Data,
    %% If no queue progress since last poll check queue
    case ets:first(Queue) of
        {Sent, _, _} when Sent =< LastSent andalso Status == busy ->
            Delay = Time - Sent,
            timeout(Delay, Time, Queue, start_poll(Time, Sent, Codel));
        {Sent, _, _} ->
            {noreply, {Status, Queue, start_poll(Time, Sent, Codel)}};
        _ ->
            {noreply, {Status, Queue, start_poll(Time, Time, Codel)}}
    end;

handle_info({timeout, Idle, {Time, LastSent}}, {_, Queue, #{idle := Idle}} = Data) ->
    {Status, Queue, Codel} = Data,
    %% If no queue progress since last idle check oldest connection
    case ets:first(Queue) of
        {Sent, Holder} = Key when Sent =< LastSent andalso Status == ready ->
            ets:delete(Queue, Key),
            ping(Holder, Queue, start_idle(Time, LastSent, Codel));
        {Sent, _} ->
            {noreply, {Status, Queue, start_idle(Time, Sent, Codel)}};
        _ ->
            {noreply, {Status, Queue, start_idle(Time, Time, Codel)}}
    end.


timeout(Delay, Time, Queue, Codel) ->
    case Codel of
        #{delay := MinDelay, next := Next, target := Target, interval := Interval}
          when Time >= Next andalso MinDelay > Target ->
            Codel1 = Codel#{slow := true, delay := Delay, next := Time + Interval},
            drop_slow(Time, Target * 2, Queue),
            {noreply, {busy, Queue, Codel1}};
        #{next := Next, interval := Interval} when Time >= Next ->
            Codel1 = Codel#{slow := false, delay := Delay, next := Time + Interval},
            {noreply, {busy, Queue, Codel1}};
        _ ->
            {noreply, {busy, Queue, Codel}}
    end.

drop_slow(Time, Timeout, Queue) ->
    MinSent = Time - Timeout,
    Match = {{'$1', '_', '$2'}},
    Guards = [{'<', '$1', MinSent}],
    SelectSlow = [{Match, Guards, [{{'$1', '$2'}}]}],
    [drop(Time - Sent, From) || {Sent, From} <- ets:select(Queue, SelectSlow)],
    ets:select_delete(Queue, [{Match, Guards, [true]}]).

ping(Holder, Queue, Codel) ->
    [{_, Conn, _, State}] = ets:lookup(Holder, ?HOLDER_KEY),
    pgo_connection:ping({Conn, Holder}, State),
    ets:delete(Holder),
    {noreply, {ready, Queue, Codel}}.

handle_checkin(Holder, Now, {ready, Queue, _} = Data) ->
    ets:insert(Queue, {{Now, Holder}}),
    {noreply, Data};
handle_checkin(Holder, Now, {busy, Queue, Codel}) ->
    dequeue(Now, Holder, Queue, Codel).

dequeue(Time, Holder, Queue, Codel) ->
    case Codel of
      #{next := Next, delay := Delay, target := Target} when Time >= Next  ->
        dequeue_first(Time, Delay > Target, Holder, Queue, Codel);
      #{slow := false} ->
        dequeue_fast(Time, Holder, Queue, Codel);
      #{slow := true, target := Target} ->
        dequeue_slow(Time, Target * 2, Holder, Queue, Codel)
    end.

dequeue_first(Time, Slow, Holder, Queue, Codel) ->
    #{interval := Interval} = Codel,
    Next = Time + Interval,
    case ets:first(Queue) of
        {Sent, _, From} = Key ->
            ets:delete(Queue, Key),
            Delay = Time - Sent,
            Codel1 =  Codel#{next => Next, delay => Delay, slow => Slow},
            go(Delay, From, Time, Holder, Queue, Codel1);
        '$end_of_table' ->
            Codel1 = Codel#{next => Next, delay => 0, slow => Slow},
            ets:insert(Queue, {{Time, Holder}}),
            {noreply, {ready, Queue, Codel1}}
    end.

dequeue_fast(Time, Holder, Queue, Codel) ->
    case ets:first(Queue) of
        {Sent, _, From} = Key ->
            ets:delete(Queue, Key),
            go(Time - Sent, From, Time, Holder, Queue, Codel);
        '$end_of_table' ->
            ets:insert(Queue, {{Time, Holder}}),
            {noreply, {ready, Queue, Codel#{delay => 0}}}
    end.

dequeue_slow(Time, Timeout, Holder, Queue, Codel) ->
    case ets:first(Queue) of
        {Sent, _, From} = Key when Time - Sent > Timeout ->
            ets:delete(Queue, Key),
            drop(Time - Sent, From),
            dequeue_slow(Time, Timeout, Holder, Queue, Codel);
        {Sent, _, From} = Key ->
            ets:delete(Queue, Key),
            go(Time - Sent, From, Time, Holder, Queue, Codel);
        '$end_of_table' ->
            ets:insert(Queue, {{Time, Holder}}),
            {noreply, {ready, Queue, Codel#{delay => 0}}}
    end.

go(Delay, From, Time, Holder, Queue, Codel=#{delay := Min}) ->
    case checkout_holder(Holder, From, Queue) of
        true when Delay < Min ->
            {noreply, {busy, Queue, Codel#{delay => Delay}}};
        true ->
            {noreply, {busy, Queue, Codel}};
        false ->
            dequeue(Time, Holder, Queue, Codel)
    end.

drop(_Delay, From) ->
    gen_server:reply(From, {error, "connection not available"}).

abs_timeout(Now, Opts) ->
    case proplists:get_value(timeout, Opts, ?TIMEOUT) of
        infinity ->
            proplists:get_value(deadline, Opts);
        Timeout ->
            min(Now + Timeout, proplists:get_value(deadline, Opts))
    end.

start_deadline(undefined, _, _, _, _) ->
    undefined;
start_deadline(Timeout, Pid, Ref, Holder, Start) ->
    Deadline = erlang:start_timer(Timeout, Pid, {Ref, Holder, self(), Start}, [{abs, true}]),
    ets:update_element(Holder, ?HOLDER_KEY, {3, Deadline}),
    Deadline.

cancel_deadline(undefined) ->
    ok;
cancel_deadline(Deadline) ->
    erlang:cancel_timer(Deadline, [{async, true}, {info, false}]).

start_poll(Now, LastSent, #{interval := Interval} = Codel) ->
    Timeout = Now + Interval,
    Poll = erlang:start_timer(Timeout, self(), {Timeout, LastSent}, [{abs, true}]),
    Codel#{poll => Poll}.

start_idle(Now, LastSent, Codel=#{idle_interval := Interval}) ->
    Timeout = Now + Interval,
    Idle = erlang:start_timer(Timeout, self(), {Timeout, LastSent}, [{abs, true}]),
    Codel#{idle => Idle}.

start_holder(Pool, Ref, State) ->
    %% Insert before setting heir so that pool can't receive empty table
    Holder = ets:new(?MODULE, [public, ordered_set]),
    true = ets:insert_new(Holder, {?HOLDER_KEY, self(), undefined, State}),
    ets:setopts(Holder, {heir, Pool, Ref}),
    Holder.

checkout_holder(Holder, {Pid, _} = From, Ref) ->
    ets:give_away(Holder, Pid, Ref),
    gen_server:reply(From, {ok, Holder}),
    true.

recv_holder(Holder, Start, Timeout) ->
    receive
        {'ETS-TRANSFER', Holder, Pool, Ref} ->
            Deadline = start_deadline(Timeout, Pool, Ref, Holder, Start),
            [{_, _, _, State}] = ets:lookup(Holder, ?HOLDER_KEY),
            {ok, {Pool, Ref, Deadline, Holder}, State}
    end.

checkin_holder(Holder, Pool, State, Msg) ->
    ets:update_element(Holder, ?HOLDER_KEY, [{3, undefined}, {4, State}]),
    ets:give_away(Holder, Pool, Msg),
    ok.

disconnect_holder(Holder, Err) ->
    delete_holder(Holder, Err).

stop_holder(Holder, Err) ->
    delete_holder(Holder, Err).

delete_holder(Holder, _Err) ->
    [{_, _Conn, Deadline, _State}] = ets:lookup(Holder, ?HOLDER_KEY),
    ets:delete(Holder),
    cancel_deadline(Deadline).
    %% disconnect({Conn, Holder}, Err, State, []).
