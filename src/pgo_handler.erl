%% Mostly from the pgsql_connection module in https://github.com/semiocast/pgsql
-module(pgo_handler).

-export([open/2,
         extended_query/3,
         extended_query/4,
         extended_query/5,
         ping/1,
         simple_query/2,
         close/1]).

-include("pgo_internal.hrl").
-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 5432).
-define(DEFAULT_USER, "postgres").
-define(DEFAULT_PASSWORD, "").

% driver options.
%% -type open_option() ::
%%         {host, inet:ip_address() | inet:hostname()} % default: ?DEFAULT_HOST
%%     |   {port, integer()}                       % default: ?DEFAULT_PORT
%%     |   {database, iodata()}                    % default: user
%%     |   {user, iodata()}                        % default: ?DEFAULT_USER
%%     |   {password, iodata()}                    % default: none

%%     |   {ssl, boolean()}                        % default: false
%%     |   {ssl_options, [ssl:ssl_option()]}       % default: []

%%     |   {application_name, atom() | iodata()}   % default: node()
%%     |   {timezone, iodata() | undefined}.        % default: undefined (not set)
%% |   {fetch_oid_map, boolean()}              % default: true
%% |   {reconnect, boolean()}                  % default: true
%% |   {async, pid()}                          % subscribe to notifications (default: no)
%% |   proplists:property().                   % undocumented !

-define(MESSAGE_HEADER_SIZE, 5).

% pgsql extended query states.
%% -type extended_query_mode() :: all | batch | {cursor, non_neg_integer()}.
-type extended_query_loop_state() ::
        % expect parse_complete message
        parse_complete
    |   {parse_complete_with_params, [any()]}
        % expect parameter_description
    |   {parameter_description_with_params, [any()]}
        % expect row_description or no_data
    |   pre_bind_row_description
        % expect bind_complete
    |   bind_complete
        % expect row_description or no_data
    |   row_description
        % expect data_row or command_complete
    |   {rows, [#row_description_field{}]}
        % expect command_complete
    |   no_data
        % expect ready_for_query
    |   {result, any()}.

-spec extended_query(#conn{}, iodata(), list()) -> pgo:result().
extended_query(Socket, Query, Parameters) ->
    extended_query(Socket, Query, Parameters, [], #{queue_time => undefined}).

-spec extended_query(#conn{}, iodata(), list(), map()) -> pgo:result().
extended_query(Socket, Query, Parameters, Timings) when is_map(Timings) ->
    extended_query(Socket, Query, Parameters, [], Timings).

-spec extended_query(#conn{}, iodata(), list(), pgo:decode_opts(), map()) -> pgo:result().
extended_query(Socket=#conn{pool=Pool}, Query, Parameters, DecodeOptions, Timings) ->
    Start = erlang:monotonic_time(),
    DecodeFun = proplists:get_value(decode_fun, DecodeOptions, undefined),
    Result = extended_query(Socket, Query, Parameters, DecodeOptions, DecodeFun, []),
    Latency = erlang:monotonic_time() - Start,
    telemetry:execute([pgo, query], #{latency => Latency}, Timings#{pool => Pool,
                                                                    query => Query,
                                                                    query_time => Latency,
                                                                    result => Result}),
    Result.

-spec ping(#conn{}) -> ok | {error, term()}.
ping(Conn=#conn{socket=Socket,
                socket_module=SocketModule}) ->
    SocketModule:send(Socket, pgo_protocol:encode_sync_message()),
    flush_until_ready_for_query(ok, Conn).

close(undefined) ->
    ok;
close(#conn{socket=Socket,
            socket_module=ssl}) ->
    ssl:shutdown(Socket, read_write);
close(#conn{socket=Socket}) ->
    unlink(Socket),
    exit(Socket, shutdown).

%%--------------------------------------------------------------------
%% @doc Actually open (or re-open) the connection.
%%
-spec open(atom(), pgo:pool_config()) -> {ok, pgo_pool:conn()} | {error, any()}.
open(Pool, PoolConfig) ->
    Host = maps:get(host, PoolConfig, ?DEFAULT_HOST),
    Port = maps:get(port, PoolConfig, ?DEFAULT_PORT),
    TraceDefault = maps:get(trace, PoolConfig, false),
    QueueDefault = maps:get(queue, PoolConfig, true),
    DefaultDecodeOpts = maps:get(decode_opts, PoolConfig, []),
    User = maps:get(user, PoolConfig, ?DEFAULT_USER),
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}]) of
        {ok, Socket} ->
            Conn = #conn{pool=Pool,
                         owner=self(),
                         socket=Socket,
                         parameters=#{},
                         trace=TraceDefault,
                         trace_attributes=[{<<"db.system">>, <<"postgresql">>},
                                           {<<"db.name">>, maps:get(database, PoolConfig, User)},
                                           %% {<<"db.connection_string">>, <<"">>},
                                           {<<"db.user">>, User},
                                           {<<"net.peer.name">>, Host},
                                           {<<"net.peer.port">>, Port},
                                           {<<"net.peer.transport">>, <<"IP.TCP">>}],
                         queue=QueueDefault,
                         socket_module=case maps:get(ssl, PoolConfig, undefined) of
                                           true -> ssl;
                                           _ ->    gen_tcp
                                       end,
                         decode_opts=DefaultDecodeOpts},
            setup(Conn, PoolConfig);
        {error, _} = ConnectError ->
            ConnectError
    end.

%%--------------------------------------------------------------------
%% @doc Setup the connection, handling the authentication handshake.
%%
setup(Conn, Options) ->
    case maps:get(ssl, Options, undefined) of
        false ->
            setup_startup(Conn#conn{socket_module=gen_tcp}, Options);
        undefined ->
            setup_startup(Conn#conn{socket_module=gen_tcp}, Options);
        true ->
            setup_ssl(Conn#conn{socket_module=ssl}, Options)
    end.

setup_ssl(Conn=#conn{socket=Socket}, Options) ->
    SSLRequestMessage = pgo_protocol:encode_ssl_request_message(),
    case gen_tcp:send(Socket, SSLRequestMessage) of
        ok ->
            case gen_tcp:recv(Socket, 1) of
                {ok, <<$S>>} ->
                    % upgrade socket.
                    SSLOptions = maps:get(ssl_options, Options, []),
                    case ssl:connect(Socket, [binary, {packet, raw}, {active, false} | SSLOptions]) of
                        {ok, SSLSocket} ->
                            setup_startup(Conn#conn{socket=SSLSocket}, Options);
                        {error, _} = SSLConnectErr ->
                            SSLConnectErr
                    end;
                {ok, <<$N>>} ->
                    % server is unwilling
                    {error, ssl_refused}
            end;
        {error, _} = SendSSLRequestError ->
            SendSSLRequestError
    end.

setup_startup(Conn=#conn{socket_module=SocketModule,
                        socket=Socket,
                        pool=Pool}, Options) ->
    % Send startup packet connection packet.
    User = maps:get(user, Options, ?DEFAULT_USER),
    Database = maps:get(database, Options, User),
    ConnectionParams = maps:get(connection_parameters, Options, []),
    ConnectionParams1 = lists:keymerge(1, ConnectionParams,
                                       [{<<"application_name">>, atom_to_binary(node(), utf8)}]),
    StartupMessage =
        pgo_protocol:encode_startup_message([{<<"user">>, User},
                                             {<<"database">>, Database}
                                             | ConnectionParams1]),
    case SocketModule:send(Socket, StartupMessage) of
        ok ->
            case receive_message(SocketModule, Socket, Pool, [no_reload_types]) of
                {ok, #error_response{fields = Fields}} ->
                    {error, {pgo_error, Fields}};
                {ok, #authentication_ok{}} ->
                    setup_finish(Conn);
                {ok, #authentication_kerberos_v5{}} ->
                    {error, {unimplemented, authentication_kerberos_v5}};
                {ok, #authentication_cleartext_password{}} ->
                    setup_authenticate_cleartext_password(Conn, Options);
                {ok, #authentication_md5_password{salt = Salt}} ->
                    setup_authenticate_md5_password(Conn, Salt, Options);
                {ok, #authentication_scm_credential{}} ->
                    {error, {unimplemented, authentication_scm}};
                {ok, #authentication_gss{}} ->
                    {error, {unimplemented, authentication_gss}};
                {ok, #authentication_sspi{}} ->
                    {error, {unimplemented, authentication_sspi}};
                {ok, #authentication_gss_continue{}} ->
                    {error, {unimplemented, authentication_sspi}};
                {ok, Message} ->
                    {error, {unexpected_message, Message}};
                {error, _} = ReceiveError -> ReceiveError
            end;
        {error, _} = SendError -> SendError
    end.

setup_authenticate_cleartext_password(Conn, Options) ->
    Password = maps:get(password, Options, ?DEFAULT_PASSWORD),
    setup_authenticate_password(Conn, Password).

setup_authenticate_md5_password(Conn, Salt, Options) ->
    User = maps:get(user, Options, ?DEFAULT_USER),
    Password = maps:get(password, Options, ?DEFAULT_PASSWORD),
    % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    <<MD51Int:128>> = crypto:hash(md5, [Password, User]),
    MD51Hex = io_lib:format("~32.16.0b", [MD51Int]),
    <<MD52Int:128>> = crypto:hash(md5, [MD51Hex, Salt]),
    MD52Hex = io_lib:format("~32.16.0b", [MD52Int]),
    MD5ChallengeResponse = ["md5", MD52Hex],
    setup_authenticate_password(Conn, MD5ChallengeResponse).

setup_authenticate_password(Conn=#conn{socket_module=SocketModule,
                                       socket=Socket,
                                       pool=Pool}, Password) ->
    Message = pgo_protocol:encode_password_message(Password),
    case SocketModule:send(Socket, Message) of
        ok ->
            case receive_message(SocketModule, Socket, Pool, []) of
                {ok, #error_response{fields = Fields}} ->
                    {error, {pgo_error, Fields}};
                {ok, #authentication_ok{}} ->
                    setup_finish(Conn);
                {ok, UnexpectedMessage} ->
                    {error, {unexpected_message, UnexpectedMessage}};
                {error, _} = ReceiveError -> ReceiveError
            end;
        {error, _} = SendError -> SendError
    end.

setup_finish(Conn=#conn{socket_module=SocketModule,
                        socket=Socket,
                        pool=Pool,
                        parameters=Parameters}) ->
    case receive_message(SocketModule, Socket, Pool, []) of
        {ok, #parameter_status{name=Name, value=Value}} ->
            setup_finish(Conn#conn{parameters=Parameters#{Name => Value}});
        {ok, #backend_key_data{procid = _ProcID, secret = _Secret}} ->
            setup_finish(Conn);
        {ok, #ready_for_query{}} ->
            {ok, Conn};
        {ok, #error_response{fields = Fields}} ->
            {error, {pgo_error, Fields}};
        {ok, Message} ->
            {error, {unexpected_message, Message}};
        {error, _} = ReceiveError ->
            ReceiveError
    end.

% This function should always return true as set or reset may only fail because
% we are within a failed transaction.
% If set failed because the transaction was aborted, the query will fail
% (unless it is a rollback).
% If set succeeded within a transaction, but the query failed, the reset may
% fail but set only applies to the transaction anyway.
%% -spec set_succeeded_or_within_failed_transaction({set, []} | {error, pgo_error:pgo_error()}) -> boolean().
%% set_succeeded_or_within_failed_transaction({set, []}) -> true;
%% set_succeeded_or_within_failed_transaction({error, {error, _} = Error}) ->
%%     error:is_in_failed_sql_transaction(Error).

extended_query(Conn=#conn{socket=Socket,
                          socket_module=SocketModule,
                          pool=Pool}, Query, Parameters, DecodeOptions, PerRowFun, Acc0) ->
    put(query, Query),
    ParseMessage = pgo_protocol:encode_parse_message("", Query, []),
    %% We ask for a description of parameters only if required.
    PacketT = case pgo_query_cache:lookup(Pool, Query) of
                  DataTypes when is_list(DataTypes) ->
                      case encode_bind_describe_execute(Conn, Parameters, DataTypes) of
                          {ok, BindExecute} ->
                              {ok, [ParseMessage, BindExecute], parse_complete};
                          {_, _} = Error ->
                              Error
                      end;
                  not_found ->
                      DescribeStatementMessage = pgo_protocol:encode_describe_message(statement, ""),
                      FlushMessage = pgo_protocol:encode_flush_message(),
                      LoopState0 = {parse_complete_with_params, Parameters},
                      {ok, [ParseMessage, DescribeStatementMessage, FlushMessage], LoopState0}

              end,
    case PacketT of
        {ok, SinglePacket, LoopState} ->
            case SocketModule:send(Socket, SinglePacket) of
                ok ->
                    try
                        receive_loop(LoopState,
                                     PerRowFun,
                                     Acc0,
                                     DecodeOptions,
                                     Conn)
                    catch
                        Class:Reason:Stacktrace ->
                            %% flush so we can reuse this connection then reraise exception
                            flush_until_ready_for_query(error, Conn),
                            erlang:raise(Class, Reason, Stacktrace)
                    end;
                {error, _} = SendSinglePacketError ->
                    SendSinglePacketError
            end;
        {_, _} ->
            PacketT
    end.

-spec encode_bind_describe_execute(pgo_pool:conn(), [any()], [oid()]) -> {ok, iodata()} | {term(), any()}.
encode_bind_describe_execute(Conn, Parameters, ParameterDataTypes) ->
    DescribeMessage = pgo_protocol:encode_describe_message(portal, ""),
    ExecuteMessage = pgo_protocol:encode_execute_message("", 0),
    SyncOrFlushMessage = pgo_protocol:encode_sync_message(),
    try
        BindMessage = pgo_protocol:encode_bind_message(Conn, "", "", Parameters, ParameterDataTypes),
        SinglePacket = [BindMessage, DescribeMessage, ExecuteMessage, SyncOrFlushMessage],
        {ok, SinglePacket}
    catch
        Class:Exception ->
            {Class, Exception}
    end.

%% requires_statement_description(_Parameters) ->
%%     true. %pgo_protocol:bind_requires_statement_description(Parameters).

-spec receive_loop(extended_query_loop_state(), pgo:decode_fun(), list(), list(), pgo:conn())
                  -> pgo:result().
receive_loop(LoopState, DecodeFun, Acc0, DecodeOptions, Conn=#conn{socket=Socket,
                                                                   socket_module=SocketModule}) ->
    case receive_message(SocketModule, Socket, Conn, DecodeOptions) of
        {ok, Message} ->
            receive_loop0(Message, LoopState, DecodeFun, Acc0, DecodeOptions, Conn);
        {error, _} = ReceiveError ->
            ReceiveError
    end.

receive_loop0(#parameter_status{name=_Name, value=_Value}, LoopState, DecodeFun, Acc0, DecodeOptions, Conn) ->
    %% State1 = handle_parameter(Name, Value, Conn),
    receive_loop(LoopState, DecodeFun, Acc0, DecodeOptions, Conn);
receive_loop0(#parse_complete{}, parse_complete, DecodeFun, Acc0, DecodeOptions, Conn) ->
    receive_loop(bind_complete, DecodeFun, Acc0, DecodeOptions, Conn);

%% Path where we ask the backend about what it expects.
%% We ignore row descriptions sent before bind as the format codes are null.
receive_loop0(#parse_complete{}, {parse_complete_with_params, Parameters}, DecodeFun, Acc0, DecodeOptions, Conn) ->
    receive_loop({parameter_description_with_params, Parameters}, DecodeFun, Acc0, DecodeOptions, Conn);
receive_loop0(#parameter_description{data_types=ParameterDataTypes},
              {parameter_description_with_params, Parameters}, DecodeFun,
              Acc0, DecodeOptions, Conn=#conn{socket=Socket,
                                              socket_module=SocketModule,
                                              pool=Pool}) ->
    pgo_query_cache:insert(Pool, get(query), ParameterDataTypes),
    %% oob_update_oid_map_if_required(Conn, ParameterDataTypes, DecodeOptions),
    PacketT = encode_bind_describe_execute(Conn, Parameters, ParameterDataTypes),
    case PacketT of
        {ok, SinglePacket} ->
            case SocketModule:send(Socket, SinglePacket) of
                ok ->
                    receive_loop(pre_bind_row_description, DecodeFun, Acc0, DecodeOptions, Conn);
                {error, _} = SendError ->
                    SendError
            end;
        {_, _} = Error ->
            case SocketModule:send(Socket, pgo_protocol:encode_sync_message()) of
                ok -> flush_until_ready_for_query(Error, Conn);
                {error, _} = SendSyncPacketError -> SendSyncPacketError
            end
    end;
receive_loop0(#row_description{}, pre_bind_row_description, DecodeFun, Acc0, DecodeOptions, Conn) ->
    receive_loop(bind_complete, DecodeFun, Acc0, DecodeOptions, Conn);
receive_loop0(#no_data{}, pre_bind_row_description, DecodeFun, Acc0, DecodeOptions, Conn) ->
    receive_loop(bind_complete, DecodeFun, Acc0, DecodeOptions, Conn);

%% Common paths after bind.
receive_loop0(#bind_complete{}, bind_complete, DecodeFun, Acc0, DecodeOptions, Conn) ->
    receive_loop(row_description, DecodeFun, Acc0, DecodeOptions, Conn);
receive_loop0(#no_data{}, row_description, DecodeFun, Acc0, DecodeOptions, Conn) ->
    receive_loop(no_data, DecodeFun, Acc0, DecodeOptions, Conn);
receive_loop0(#row_description{fields = Fields}, row_description, DecodeFun, Acc0, DecodeOptions, Conn) ->
    %% oob_update_oid_map_from_fields_if_required(Conn, Fields, DecodeOptions),
    receive_loop({rows, Fields}, DecodeFun, Acc0, DecodeOptions, Conn);
receive_loop0(#data_row{values = Values}, {rows, Fields} = LoopState, undefined=DecodeFun,
              Acc0, DecodeOptions, Conn=#conn{pool=Pool}) ->
    DecodedRow = pgo_protocol:decode_row(Fields, Values, Pool, DecodeOptions),
    receive_loop(LoopState, DecodeFun, [DecodedRow | Acc0], DecodeOptions, Conn);
receive_loop0(#data_row{values = Values}, {rows, Fields} = LoopState, DecodeFun, Acc0, DecodeOptions, Conn=#conn{pool=Pool}) ->
    DecodedRow = pgo_protocol:decode_row(Fields, Values, Pool, DecodeOptions),
    receive_loop(LoopState, DecodeFun, [DecodeFun(DecodedRow, Fields) | Acc0], DecodeOptions, Conn);
receive_loop0(#command_complete{command_tag = Tag}, _LoopState, DecodeFun, Acc0, DecodeOptions, Conn) ->
    {Command, NumRows} = decode_tag(Tag),
    receive_loop({result, #{command => Command,
                            num_rows => NumRows,
                            rows => lists:reverse(Acc0)}}, DecodeFun, Acc0, DecodeOptions, Conn);
%% receive_loop0(#portal_suspended{}, LoopState, DecodeFun, Acc0, DecodeOptions, Conn={_,S}) ->
%%     ExecuteMessage = pgo_protocol:encode_execute_message("", 0),
%%     FlushMessage = pgo_protocol:encode_flush_message(),
%%     SinglePacket = [ExecuteMessage, FlushMessage],
%%     case gen_tcp:send(S, SinglePacket) of
%%         ok -> receive_loop(LoopState, DecodeFun, Acc0, DecodeOptions, Conn);
%%         {error, _} = SendSinglePacketError ->
%%             SendSinglePacketError
%%     end;
receive_loop0(#ready_for_query{}, {result, Result}, _Fun, _Acc0, _DecodeOptions, __Socket) ->
    Result;
receive_loop0(#error_response{fields = Fields}, LoopState, _Fun, _Acc0, _DecodeOptions,
              Conn=#conn{socket=Socket,
                         socket_module=SocketModule}) ->
    Error = {error, {pgsql_error, Fields}},
    % We already sent a Sync except when we sent a Flush :-)
    % - when we asked for the statement description
    % - when MaxRowsStep > 0
    NeedSync = case LoopState of
                   {parse_complete_with_params, _Args} -> true;
                   {parameter_description_with_params, _Parameters} -> true;
                   _ -> false
               end,
    case NeedSync of
        true ->
            case SocketModule:send(Socket, pgo_protocol:encode_sync_message()) of
                ok -> flush_until_ready_for_query(Error, Conn);
                {error, _} = SendSyncPacketError -> SendSyncPacketError
            end;
        false ->
            flush_until_ready_for_query(Error, Conn)
    end;
receive_loop0(#ready_for_query{} = Message, _LoopState, _Fun, _Acc0, _DecodeOptions, _Conn) ->
    {error, {unexpected_message, Message}};
receive_loop0(Message, _LoopState, _Fun, _Acc0, _DecodeOptions, Conn=#conn{socket=Socket,
                                                                           socket_module=SocketModule}) ->
    SocketModule:send(Socket, pgo_protocol:encode_sync_message()),
    Error = {error, {unexpected_message, Message}},
    flush_until_ready_for_query(Error, Conn).

flush_until_ready_for_query(Result, Conn=#conn{socket=Socket,
                                               socket_module=SocketModule}) ->
    case receive_message(SocketModule, Socket, Conn, []) of
        {ok, #parameter_status{name = _Name, value = _Value}} ->
            flush_until_ready_for_query(Result, Conn);
        {ok, #ready_for_query{}} ->
            Result;
        {ok, _OtherMessage} ->
            flush_until_ready_for_query(Result, Conn);
        {error, _} = ReceiveError ->
            ReceiveError
    end.

%%--------------------------------------------------------------------
%% @doc Receive a single packet (in passive mode). Notifications and
%% notices are broadcast to subscribers.
%%
receive_message(SocketModule, Socket, Pool, DecodeOpts) ->
    Result0 = case SocketModule:recv(Socket, ?MESSAGE_HEADER_SIZE) of
                  {ok, <<Code:8/integer, Size:32/integer>>} ->
                      Payload = Size - 4,
                      case Payload of
                          0 ->
                              pgo_protocol:decode_message(Code, <<>>, Pool, DecodeOpts);
                          _ ->
                              case SocketModule:recv(Socket, Payload) of
                                  {ok, Rest} ->
                                      pgo_protocol:decode_message(Code, Rest, Pool, DecodeOpts);
                                  {error, _} = ErrorRecvPacket ->
                                      ErrorRecvPacket
                              end
                      end;
                  {error, _} = ErrorRecvPacketHeader ->
                      ErrorRecvPacketHeader
              end,
    case Result0 of
        {ok, #notification_response{} = _Notification} ->
            receive_message(SocketModule, Socket, Pool, DecodeOpts);
        {ok, #notice_response{} = _Notice} ->
            receive_message(SocketModule, Socket, Pool, DecodeOpts);
        _ ->
            Result0
    end.

%%--------------------------------------------------------------------
%% @doc Decode a command complete tag and result rows and form a result
%% according to the current API.
%%
decode_tag(<<"SELECT ", Num/binary>>) ->
    {select, binary_to_integer(Num)};
decode_tag(<<"INSERT ", Rest/binary>>) ->
    [_Oid, NumRows] = binary:split(Rest, <<" ">>),
    {insert, binary_to_integer(NumRows)};
decode_tag(<<"UPDATE ", Num/binary>>) ->
    {update, binary_to_integer(Num)};
decode_tag(<<"DELETE ", Num/binary>>) ->
    {delete, binary_to_integer(Num)};
decode_tag(<<"FETCH ", Num/binary>>) ->
    {fetch, binary_to_integer(Num)};
decode_tag(<<"MOVE ", Num/binary>>) ->
    {move, binary_to_integer(Num)};
decode_tag(<<"COPY ", Num/binary>>) ->
    {copy, binary_to_integer(Num)};
decode_tag(<<"BEGIN">>) ->
    {'begin', nil};
decode_tag(<<"COMMIT">>) ->
    {commit, nil};
decode_tag(<<"ROLLBACK">>) ->
    {rollback, nil};
decode_tag(Tag) ->
    case binary:split(Tag, <<" ">>) of
        [Verb, Object] ->
            VerbDecoded = decode_verb(Verb),
            ObjectL = decode_object(Object),
            list_to_tuple([VerbDecoded | ObjectL]);
        [Verb] -> decode_verb(Verb)
    end.

decode_verb(Verb) ->
    VerbStr = binary_to_list(Verb),
    VerbLC = string:to_lower(VerbStr),
    list_to_atom(VerbLC).

decode_object(<<FirstByte, _/binary>> = Object) when FirstByte =< $9 andalso FirstByte >= $0 ->
    Words = binary:split(Object, <<" ">>, [global]),
    [list_to_integer(binary_to_list(Word)) || Word <- Words];
decode_object(Object) ->
    ObjectUStr = re:replace(Object, <<" ">>, <<"_">>, [global, {return, list}]),
    ObjectULC = string:to_lower(ObjectUStr),
    [list_to_atom(ObjectULC)].

%% only for the type server

simple_query(Conn=#conn{socket_module=SocketModule,
                        socket=Socket}, Query) ->
    case SocketModule:send(Socket, pgo_protocol:encode_query_message(Query)) of
        ok ->
            simple_query_loop(Conn, []);
        {error, _}=Error ->
            Error
    end.


%% % This function should always return true as set or reset may only fail because
%% % we are within a failed transaction.
%% % If set failed because the transaction was aborted, the query will fail
%% % (unless it is a rollback).
%% % If set succeeded within a transaction, but the query failed, the reset may
%% % fail but set only applies to the transaction anyway.
%% -spec set_succeeded_or_within_failed_transaction({set, []} | {error, pgsql_error:pgsql_error()}) -> boolean().
%% set_succeeded_or_within_failed_transaction({set, []}) -> true;
%% set_succeeded_or_within_failed_transaction({error, {pgsql_error, _} = Error}) ->
%%     pgsql_error:is_in_failed_sql_transaction(Error).

simple_query_loop(#conn{socket=Socket}=Conn, Acc) ->
    case simple_receive_message(Socket, Conn, []) of
        {ok, row_description} ->
            simple_query_loop(Conn, Acc);
        {ok, #data_row{values = Values}} ->
            simple_query_loop(Conn, [Values | Acc]);
        {ok, command_complete} ->
            simple_query_loop(Conn, Acc);
        {ok, #ready_for_query{}} ->
            {ok, Acc};
        {error, _} = ReceiveError ->
            ReceiveError
    end.

simple_receive_message(Socket, _Conn=#conn{socket_module=SocketModule}, _DecodeOpts) ->
    case SocketModule:recv(Socket, ?MESSAGE_HEADER_SIZE) of
        {ok, <<Code:8/integer, Size:32/integer>>} ->
            case SocketModule:recv(Socket, Size - 4) of
                {ok, Rest} ->
                    case Code of
                        $T ->
                            {ok, row_description};
                        $D ->
                            decode_data_row_message(Rest);
                        $C ->
                            {ok, command_complete};
                        $Z ->
                            decode_ready_for_query_message(Rest)
                    end;
                {error, _} = ErrorRecvPacket ->
                    ErrorRecvPacket
            end;
        {error, _} = ErrorRecvPacketHeader ->
            ErrorRecvPacketHeader
    end.

decode_data_row_message(<<N:16/integer, Rest/binary>> = Payload) ->
    case decode_data_row_values(N, Rest) of
        {ok, Values} ->
            {ok, #data_row{values=Values}};
        {error, _} ->
            {error, {unknow_message, data_row, Payload}}
    end;
decode_data_row_message(Payload) ->
    {error, {unknow_message, data_row, Payload}}.

decode_data_row_values(Columns, Binary) ->
    decode_data_row_values0(Binary, Columns, []).

decode_data_row_values0(<<>>, 0, Acc) -> {ok, lists:reverse(Acc)};
decode_data_row_values0(<<-1:32/signed-integer, Rest/binary>>, N, Acc) when N > 0 ->
    decode_data_row_values0(Rest, N - 1, [null | Acc]);
decode_data_row_values0(<<ValueLen:32/integer, ValueBin:ValueLen/binary, Rest/binary>>, N, Acc) when N > 0 ->
    decode_data_row_values0(Rest, N - 1, [ValueBin | Acc]);
decode_data_row_values0(<<_/binary>>, _N, _Acc) -> {error, invalid_value_len}.

decode_ready_for_query_message(<<$I>>) ->
    {ok, #ready_for_query{transaction_status=idle}};
decode_ready_for_query_message(<<$T>>) ->
    {ok, #ready_for_query{transaction_status=transaction}};
decode_ready_for_query_message(<<$E>>) ->
    {ok, #ready_for_query{transaction_status=error}};
decode_ready_for_query_message(Payload) ->
    {error, {unknown_message, ready_for_query, Payload}}.
