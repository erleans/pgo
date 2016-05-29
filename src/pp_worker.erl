-module(pp_worker).

-behaviour(gen_server).

-export([start_link/3,
         done/1,
         break/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {refs      :: maps:map(),
                monitors  :: maps:map(),
                options   :: [{atom(), any()}],
                broker    :: pid()}).

start_link(Broker, Size, DBOptions) ->
    gen_server:start_link(?MODULE, [Broker, Size, DBOptions], []).

done({Pid, Ref}) ->
    gen_server:cast(Pid, {done, Ref}).

break({Pid, Ref}) ->
    gen_server:cast(Pid, {break, Ref}).

init([Broker, Size, DBOptions]) ->
    process_flag(trap_exit, true),
    [self() ! connect || _ <- lists:seq(1, Size)],
    {ok, #state{broker=Broker,
                options=DBOptions,
                refs=#{},
                monitors=#{}}}.

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast({done, Ref}, State=#state{broker=Broker,
                                      refs=Refs,
                                      monitors=Monitors}) ->
    case maps:get(Ref, Refs, undefined) of
        {Mon, C} ->
            erlang:demonitor(Mon, [flush]),
            ask(Broker, C),
            {noreply, State#state{refs=maps:remove(Ref, Refs),
                                  monitors=maps:remove(Mon, Monitors)}};
        undefined ->
            {noreply, State}
    end;
handle_cast({break, Ref}, State=#state{broker=Broker,
                                       options=Options,
                                       refs=Refs,
                                       monitors=Monitors}) ->
    {Mon, C} = maps:get(Ref, Refs),
    erlang:demonitor(Mon, [flush]),
    erlang:unlink(element(2, C)),
    pgsql_connection:close(C),
    connect(Broker, Options),
    {noreply, State#state{refs=maps:remove(Ref, Refs),
                          monitors=maps:remove(Mon, Monitors)}}.

handle_info({C, {go, Ref, Pid, _, _}}, State=#state{refs=Refs,
                                                    monitors=Monitors}) ->
    Mon = erlang:monitor(process, Pid),
    {noreply, State#state{refs = Refs#{Ref => {Mon, C}},
                          monitors = Monitors#{Mon => {Ref, C}}}};
handle_info({C, {drop, _}}, State=#state{options=Options,
                                         broker=Broker}) ->
    pgsql_connection:close(C),
    connect(Broker, Options),
    {noreply, State};
handle_info(connect, State=#state{options=Options,
                                  broker=Broker}) ->
    connect(Broker, Options),
    {noreply, State};
handle_info({'DOWN', Mon, process, _Pid, _}, State=#state{broker=Broker,
                                                          refs=Refs,
                                                          monitors=Monitors}) ->
    {Ref, C} = maps:get(Mon, Monitors),
    ask(Broker, C),
    {noreply, State#state{refs=maps:remove(Ref, Refs),
                          monitors=maps:remove(Mon, Monitors)}};
handle_info({'EXIT', P, _}, State=#state{refs=Refs,
                                         monitors=Monitors,
                                         options=Options,
                                         broker=Broker}) ->
    C = {pgsql_connection, P},
    case cancel_or_await(Broker, C) of
        cancelled ->
            connect(Broker, Options),
            {noreply, State};
        {go, Ref, Pid, _, _} ->
            Mon = erlang:monitor(process, Pid),
            {noreply, State#state{refs = Refs#{Ref => {Mon, C}},
                                  monitors = Monitors#{Mon => {Ref, C}}}};
        {drop, _} ->
            connect(Broker, Options),
            {noreply, State}
    end;
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

connect(Broker, Options) ->
    C = {pgsql_connection, Pid} = pgsql_connection:open(Options),
    erlang:link(Pid),
    ask(Broker, C).

ask(Broker, C) ->
    sbroker:async_ask_r(Broker, {self(), C}, C).

cancel_or_await(Broker, C) ->
    case sbroker:cancel(Broker, C, 5000) of
        false ->
            sbroker:await(C, 0);
        1 ->
            cancelled
    end.
