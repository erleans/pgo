-module(pgo_notifications).

-behaviour(gen_statem).

-export([start_link/1,
         start_link/2,
         listen/2,
         unlisten/2,
         report_cb/1]).

-export([init/1,
         callback_mode/0,
         connected/3,
         disconnected/3,
         terminate/3]).

-include("pgo_internal.hrl").
-include_lib("kernel/include/logger.hrl").

-type channel() :: unicode:unicode_binary().

-record(data, {conn :: #conn{} | undefined,
               config :: pgo:pool_config(),
               backoff :: backoff:backoff(),

               listeners :: #{reference() => {channel(), pid()}},
               listener_channels :: #{channel() => #{reference() => pid()}}}).

-spec start_link(pgo:pool_config()) -> gen_statem:start_ret().
start_link(Config) ->
    gen_statem:start_link(?MODULE, [Config], []).

-spec start_link(gen_statem:server_name(), pgo:pool_config()) -> gen_statem:start_ret().
start_link(ServerName, Config) ->
    gen_statem:start_link(ServerName, ?MODULE, [Config], []).

-spec listen(gen_statem:server_ref(), channel()) -> {ok, reference()}.
listen(ServerRef, Channel) when is_binary(Channel) ->
    gen_statem:call(ServerRef, {listen, Channel}).

-spec unlisten(gen_statem:server_ref(), reference()) -> ok.
unlisten(ServerRef, Ref) ->
    gen_statem:call(ServerRef, {unlisten, Ref}).

init([Config]) ->
    erlang:process_flag(trap_exit, true),
    B = backoff:init(1000, 10000),
    {ok, disconnected, #data{backoff=B,
                             config=Config,
                             listeners=#{},
                             listener_channels=#{}},
     {next_event, internal, connect}
    }.

callback_mode() ->
    [state_functions, state_enter].

disconnected(enter, _, _Data) ->
    keep_state_and_data;
disconnected(EventType, _, Data=#data{backoff=B,
                                      config=Config}) when EventType =:= internal
                                                           ; EventType =:= timeout
                                                           ; EventType =:= state_timeout ->
    try pgo_handler:open(undefined, Config) of
        {ok, Conn} ->
            {_, B1} = backoff:succeed(B),
            {next_state, connected, Data#data{conn=Conn,
                                              backoff=B1}};
        {error, Error} ->
            ?LOG_DEBUG("full error connecting to database: ~p", [Error]),
            ?LOG_INFO(#{at => connecting,
                        reason => Error},
                      #{report_cb => fun ?MODULE:report_cb/1}),
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, Data#data{backoff=B1},
             [{state_timeout, Backoff, connect}]}
    catch
        throw:Reason ->
            ?LOG_INFO(#{at => connecting,
                        reason => Reason},
                      #{report_cb => fun ?MODULE:report_cb/1}),
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, Data#data{backoff=B1},
             [{state_timeout, Backoff, connect}]}
    end;
disconnected({call, {Pid, _}=From}, {listen, Channel}, Data=#data{listeners=Listeners,
                                                                  listener_channels=ListenerChannels}) ->
    Ref = erlang:monitor(process, Pid),
    ListenerChannels1 = maps:update_with(Channel, fun(ChannelListeners) ->
                                                          ChannelListeners#{Ref => Pid}
                                                  end, #{Ref => Pid}, ListenerChannels),
    Listeners1 = Listeners#{Ref => {Channel, Pid}},
    {keep_state, Data#data{listeners=Listeners1,
                           listener_channels=ListenerChannels1}, [{reply, From, {eventually, Ref}},
                                                                  {next_event, internal, connect}]};
disconnected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

connected(enter, _, #data{conn=Conn,
                          listener_channels=ListenerChannels}) ->
    case map_size(ListenerChannels) > 0 of
        true ->
            Channels = maps:keys(ListenerChannels),
            ListenStatements = [["LISTEN ", Channel, "; "] || Channel <- Channels],
            Query = ["DO $$BEGIN ", ListenStatements, " END$$"],
            #{command := do} = pgo_handler:extended_query(Conn, Query, []),
            keep_state_and_data;
        false ->
            keep_state_and_data
    end;
connected({call, {Pid, _}=From}, {listen, Channel}, Data=#data{conn=Conn,
                                                               listeners=Listeners,
                                                               listener_channels=ListenerChannels}) ->
    Ref = erlang:monitor(process, Pid),
    ListenerChannels1 = maps:update_with(Channel, fun(Channels) ->
                                                          Channels#{Ref => Pid}
                                                  end, #{Ref => Pid}, ListenerChannels),
    Listeners1 = Listeners#{Ref => {Channel, Pid}},
    case erlang:map_size(maps:get(Channel, ListenerChannels1)) of
        1 ->
            %% first time listening on this channel
            case pgo_handler:extended_query(Conn, ["LISTEN ", Channel], []) of
                #{command := listen} ->
                    {keep_state, Data#data{listeners=Listeners1,
                                           listener_channels=ListenerChannels1}, [{reply, From, {ok, Ref}}]};
                _ ->
                    {keep_state_and_data, [{reply, From, error}]}
            end;
        _ ->
            {keep_state, Data#data{listeners=Listeners1,
                                   listener_channels=ListenerChannels1}, [{reply, From, {ok, Ref}}]}
    end;
connected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

handle_event({call, From}, {unlisten, Ref}, Data=#data{conn=Conn,
                                                       listeners=Listeners,
                                                       listener_channels=ListenerChannels}) ->
    {Listeners1, ListenerChannels1} = unlisten(Ref, Listeners, ListenerChannels, Conn),
    {keep_state, Data#data{listeners=Listeners1,
                           listener_channels=ListenerChannels1}, [{reply, From, ok}]};
handle_event(info, {'DOWN', Ref, process, _, _}, Data=#data{conn=Conn,
                                                            listeners=Listeners,
                                                            listener_channels=ListenerChannels}) ->
    {Listeners1, ListenerChannels1} = unlisten(Ref, Listeners, ListenerChannels, Conn),
    {keep_state, Data#data{listeners=Listeners1,
                           listener_channels=ListenerChannels1}};
handle_event(info, {Tag, Socket, Binary}, Data=#data{conn=Conn=#conn{socket=Socket},
                                                     listener_channels=ListenerChannels}) when Tag =:= tcp orelse Tag =:= ssl ->
    Conn = pgo_handler:process_active_data(Binary, Conn, fun(Notification) ->
                                                                 notify(ListenerChannels, Notification)
                                                         end),
    _ = inet:setopts(Socket, [{active, once}]),
    {keep_state, Data#data{conn=Conn}};
handle_event(info, {'EXIT', Socket, _Reason}, Data=#data{conn=#conn{socket=Socket}}) ->
    %% socket died, go to disconnected state
    close_and_reopen(Data);
%% ignore `EXIT' for a different Socket -- means it is an old message
handle_event(info, {'EXIT', _, _Reason}, _Data) ->
    keep_state_and_data;
%% nothing to do for `ssl_closed' -- it should be handled by the `EXIT' handling
handle_event(info, {ssl_closed, _}, _Data) ->
    keep_state_and_data;
handle_event(_, _, _) ->
    keep_state_and_data.

terminate(_Reason, _, #data{conn=undefined}) ->
    ok;
terminate(_Reason, _, #data{conn=Conn}) ->
    pgo_handler:close(Conn),
    ok.

%%

close_and_reopen(Data=#data{conn=Conn}) ->
    pgo_handler:close(Conn),
    {next_state, disconnected, Data#data{conn=undefined},
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


unlisten(Ref, Listeners, ListenerChannels, Conn) ->
    erlang:demonitor(Ref, [flush]),
    case maps:find(Ref, Listeners) of
        {ok, {Channel, _Pid}} ->
            Listeners1 = maps:remove(Ref, Listeners),
            PerChannel = maps:remove(Ref, maps:get(Channel, ListenerChannels)),
            case erlang:map_size(PerChannel) of
                0 ->
                    ListenerChannels1 =maps:remove(Channel, ListenerChannels),
                    #{command := unlisten} = pgo_handler:extended_query(Conn, ["UNLISTEN ", Channel], []),
                    {Listeners1, ListenerChannels1};
                _ ->
                    {Listeners1, ListenerChannels#{Channel => PerChannel}}
            end;
        error ->
            {Listeners, ListenerChannels}
    end.

notify(ListenerChannels, #notification_response{channel=Channel,
                                                payload=Payload}) ->
    case maps:find(Channel, ListenerChannels) of
        {ok, Listeners} ->
            maps:foreach(fun(Ref, Pid) ->
                                 Pid ! {notification, self(), Ref, Channel, Payload}
                         end, Listeners);
        error ->
            ok
    end.
