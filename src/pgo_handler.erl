%% Mostly from the pgsql_connection module in https://github.com/semiocast/pgsql
-module(pgo_handler).

-include("pgo_internal.hrl").

-export([pgsql_open/2,
         simple_query/2,
         extended_query/3,
         close/1]).

-define(REQUEST_TIMEOUT, infinity).
-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 5432).
-define(DEFAULT_USER, "postgres").
-define(DEFAULT_PASSWORD, "").
-define(DEFAULT_MAX_ROWS_STEP, 1000).

-define(TIMEOUT_GEN_SERVER_CALL_DELTA, 5000).

-type prim_socket() :: port() | tuple().
-type socket_module() :: gen_tcp | ssl.
-type socket() :: {socket_module(), prim_socket()}.

% driver options.
-type open_option() ::
        {host, inet:ip_address() | inet:hostname()} % default: ?DEFAULT_HOST
    |   {port, integer()}                       % default: ?DEFAULT_PORT
    |   {database, iodata()}                    % default: user
    |   {user, iodata()}                        % default: ?DEFAULT_USER
    |   {password, iodata()}                    % default: none
    |   {fetch_oid_map, boolean()}              % default: true
    |   {ssl, boolean()}                        % default: false
    |   {ssl_options, [ssl:ssl_option()]}       % default: []
    |   {reconnect, boolean()}                  % default: true
    |   {application_name, atom() | iodata()}   % default: node()
    |   {timezone, iodata() | undefined}        % default: undefined (not set)
    |   {async, pid()}                          % subscribe to notifications (default: no)
    |   proplists:property().                   % undocumented !

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

%%--------------------------------------------------------------------
%% @doc Perform a simple query.
%%
%% -spec simple_query(iodata(), pgsql_connection()) ->
%%     result_tuple() | {error, any()} | [result_tuple() | {error, any()}].
simple_query(Conn=#conn{socket=Socket}, Query) ->
    QueryMessage = pgo_protocol:encode_query_message(Query),
    case gen_tcp:send(Socket, QueryMessage) of
        ok ->
            pgsql_simple_query_loop([], [], [], Conn);
        {error, _} = _SendQueryError ->
            ok
    end.

pgsql_simple_query_loop(Result0, Acc, QueryOptions, Conn=#conn{pool=Pool,
                                                               socket=Socket}) ->
    case receive_message(Socket) of
        {ok, #parameter_status{name = _Name, value = _Value}} ->
            %% State1 = handle_parameter(Name, Value, State0),
            pgsql_simple_query_loop(Result0, Acc, QueryOptions, Conn);
        {ok, #row_description{fields = Fields}} when Result0 =:= [] ->
            pgsql_simple_query_loop({rows, Fields, []}, Acc, QueryOptions, Conn);
        {ok, #data_row{values = Values}} when is_tuple(Result0) andalso element(1, Result0) =:= rows ->
            {rows, Fields, AccRows0} = Result0,
            DecodedRow = pgo_protocol:decode_row(Fields, Values, Pool, QueryOptions),
            AccRows1 = [DecodedRow | AccRows0],
            pgsql_simple_query_loop({rows, Fields, AccRows1}, Acc, QueryOptions, Conn);
        {ok, #command_complete{command_tag = Tag}} ->
            {Command, NumRows} = decode_tag(Tag),
            ResultRows = case Result0 of
                {rows, _Descs, AccRows} -> lists:reverse(AccRows);
                [] -> []
            end,
            Acc1 = [#{command => Command, num_rows => NumRows, rows => ResultRows} | Acc],
            pgsql_simple_query_loop([], Acc1, QueryOptions, Conn);
        {ok, #empty_query_response{}} ->
            pgsql_simple_query_loop(Result0, Acc, QueryOptions, Conn);
        {ok, #error_response{fields = Fields}} ->
            Error = {error, {pgo_error, Fields}},
            Acc1 = [Error | Acc],
            pgsql_simple_query_loop([], Acc1, QueryOptions, Conn);
        {ok, #ready_for_query{}} ->
            case Acc of
                [SingleResult] -> SingleResult;
                MultipleResults -> MultipleResults
            end;
        {ok, Message} ->
            {error, {unexpected_message, Message}};
        {error, _} = ReceiveError ->
            ReceiveError
    end.

extended_query(Socket, Query, Parameters) ->
    pgsql_extended_query(Socket, Query, Parameters, fun(R, _) -> R end, []).

close(undefined) ->
    ok;
close(#conn{socket=Socket}) ->
    unlink(Socket),
    exit(Socket, shutdown).

%%--------------------------------------------------------------------
%% @doc Actually open (or re-open) the connection.
%%
-spec pgsql_open(atom(), [open_option()]) -> {ok, socket()} | {error, any()}.
pgsql_open(Pool, Options) ->
    Host = proplists:get_value(host, Options, ?DEFAULT_HOST),
    Port = proplists:get_value(port, Options, ?DEFAULT_PORT),
    % First open a TCP connection
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}]) of
        {ok, Socket} ->
            case pgsql_setup(Socket, Options) of
                ok ->
                    {ok, #conn{owner=self(),
                               pool=Pool,
                               socket=Socket}};
                {error, _} = SetupError ->
                    SetupError
            end;
        {error, _} = ConnectError ->
            ConnectError
    end.

%%--------------------------------------------------------------------
%% @doc Setup the connection, handling the authentication handshake.
%%
pgsql_setup(Socket, Options) ->
    case proplists:get_bool(ssl, Options) of
        false ->
            pgsql_setup_startup(Socket, Options);
        true ->
            pgsql_setup_ssl(Socket, Options)
    end.

pgsql_setup_ssl(Socket, Options) ->
    SSLRequestMessage = pgo_protocol:encode_ssl_request_message(),
    case gen_tcp:send(Socket, SSLRequestMessage) of
        ok ->
            case gen_tcp:recv(Socket, 1) of
                {ok, <<$S>>} ->
                    % upgrade socket.
                    SSLOptions = proplists:get_value(ssl_options, Options, []),
                    case ssl:connect(Socket, [binary, {packet, raw}, {active, false}] ++ SSLOptions) of
                        {ok, SSLSocket} ->
                            pgsql_setup_startup(SSLSocket, Options);
                        {error, _} = SSLConnectErr -> SSLConnectErr
                    end;
                {ok, <<$N>>} ->
                    % server is unwilling
                    {error, ssl_refused}
            end;
        {error, _} = SendSSLRequestError -> SendSSLRequestError
    end.

pgsql_setup_startup(Socket, Options) ->
    % Send startup packet connection packet.
    User = proplists:get_value(user, Options, ?DEFAULT_USER),
    Database = proplists:get_value(database, Options, User),
    ApplicationName = case proplists:get_value(application_name, Options, node()) of
        ApplicationNameAtom when is_atom(ApplicationNameAtom) -> atom_to_binary(ApplicationNameAtom, utf8);
        ApplicationNameString -> ApplicationNameString
    end,
    TZOpt = case proplists:get_value(timezone, Options, undefined) of
        undefined -> [];
        Timezone -> [{<<"timezone">>, Timezone}]
    end,
    StartupMessage = pgo_protocol:encode_startup_message([{<<"user">>, User},
                                                            {<<"database">>, Database},
                                                            {<<"application_name">>, ApplicationName} | TZOpt]),
    case gen_tcp:send(Socket, StartupMessage) of
        ok ->
            case receive_message(Socket) of
                {ok, #error_response{fields = Fields}} ->
                    {error, {pgo_error, Fields}};
                {ok, #authentication_ok{}} ->
                    pgsql_setup_finish(Socket, Options);
                {ok, #authentication_kerberos_v5{}} ->
                    {error, {unimplemented, authentication_kerberos_v5}};
                {ok, #authentication_cleartext_password{}} ->
                    pgsql_setup_authenticate_cleartext_password(Socket, Options);
                {ok, #authentication_md5_password{salt = Salt}} ->
                    pgsql_setup_authenticate_md5_password(Socket, Salt, Options);
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

pgsql_setup_authenticate_cleartext_password(Socket, Options) ->
    Password = proplists:get_value(password, Options, ?DEFAULT_PASSWORD),
    pgsql_setup_authenticate_password(Socket, Password, Options).

pgsql_setup_authenticate_md5_password(Socket, Salt, Options) ->
    User = proplists:get_value(user, Options, ?DEFAULT_USER),
    Password = proplists:get_value(password, Options, ?DEFAULT_PASSWORD),
    % concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    <<MD51Int:128>> = crypto:hash(md5, [Password, User]),
    MD51Hex = io_lib:format("~32.16.0b", [MD51Int]),
    <<MD52Int:128>> = crypto:hash(md5, [MD51Hex, Salt]),
    MD52Hex = io_lib:format("~32.16.0b", [MD52Int]),
    MD5ChallengeResponse = ["md5", MD52Hex],
    pgsql_setup_authenticate_password(Socket, MD5ChallengeResponse, Options).

pgsql_setup_authenticate_password(Socket, Password, Options) ->
    Message = pgo_protocol:encode_password_message(Password),
    case gen_tcp:send(Socket, Message) of
        ok ->
            case receive_message(Socket) of
                {ok, #error_response{fields = Fields}} ->
                    {error, {pgo_error, Fields}};
                {ok, #authentication_ok{}} ->
                    pgsql_setup_finish(Socket, Options);
                {ok, UnexpectedMessage} ->
                    {error, {unexpected_message, UnexpectedMessage}};
                {error, _} = ReceiveError -> ReceiveError
            end;
        {error, _} = SendError -> SendError
    end.

pgsql_setup_finish(Socket, Options) ->
    case receive_message(Socket) of
        {ok, #parameter_status{name = _Name, value = _Value}} ->
            %% State1 = handle_parameter(Name, Value, sync, Options),
            pgsql_setup_finish(Socket, Options);
        {ok, #backend_key_data{procid = _ProcID, secret = _Secret}} ->
            pgsql_setup_finish(Socket, Options);
        {ok, #ready_for_query{}} ->
            ok;
        {ok, #error_response{fields = Fields}} ->
            {error, {pgo_error, Fields}};
        {ok, Message} ->
            {error, {unexpected_message, Message}};
        {error, _} = ReceiveError -> ReceiveError
    end.

% This function should always return true as set or reset may only fail because
% we are within a failed transaction.
% If set failed because the transaction was aborted, the query will fail
% (unless it is a rollback).
% If set succeeded within a transaction, but the query failed, the reset may
% fail but set only applies to the transaction anyway.
%% -spec set_succeeded_or_within_failed_transaction({set, []} | {error, pgo_error:pgo_error()}) -> boolean().
%% set_succeeded_or_within_failed_transaction({set, []}) -> true;
%% set_succeeded_or_within_failed_transaction({error, {pgsql_error, _} = Error}) ->
%%     pgsql_error:is_in_failed_sql_transaction(Error).

pgsql_extended_query(Conn=#conn{socket=Socket,
                                pool=Pool}, Query, Parameters, PerRowFun, Acc0) ->
    put(query, Query),
    IntegerDateTimes = true,
    QueryOptions = [],
    ParseMessage = pgo_protocol:encode_parse_message("", Query, []),
    % We ask for a description of parameters only if required.
    PacketT = case pgo_query_cache:lookup(Pool, Query) of
        not_found ->
            DescribeStatementMessage = pgo_protocol:encode_describe_message(statement, ""),
            FlushMessage = pgo_protocol:encode_flush_message(),
            LoopState0 = {parse_complete_with_params, Parameters},
            {ok, [ParseMessage, DescribeStatementMessage, FlushMessage], LoopState0};
        DataTypes ->
            case encode_bind_describe_execute(Parameters, DataTypes, Pool, IntegerDateTimes) of
                {ok, BindExecute} ->
                    {ok, [ParseMessage, BindExecute], parse_complete};
                {error, _} = Error ->
                    Error
            end
    end,
    case PacketT of
        {ok, SinglePacket, LoopState} ->
            case gen_tcp:send(Socket, SinglePacket) of
                ok ->
                    pgsql_extended_query_receive_loop(LoopState,
                                                      PerRowFun,
                                                      Acc0,
                                                      QueryOptions,
                                                      Conn);
                {error, _} = SendSinglePacketError ->
                    SendSinglePacketError
            end;
        {error, _} ->
            PacketT
    end.

-spec encode_bind_describe_execute([any()], [pgsql_oid()], atom(), boolean()) -> {ok, iodata()} | {error, any()}.
encode_bind_describe_execute(Parameters, ParameterDataTypes, Pool, IntegerDateTimes) ->
    DescribeMessage = pgo_protocol:encode_describe_message(portal, ""),
    ExecuteMessage = pgo_protocol:encode_execute_message("", 0),
    SyncOrFlushMessage = pgo_protocol:encode_sync_message(),
    try
        BindMessage = pgo_protocol:encode_bind_message("", "", Parameters, ParameterDataTypes, Pool, IntegerDateTimes),
        SinglePacket = [BindMessage, DescribeMessage, ExecuteMessage, SyncOrFlushMessage],
        {ok, SinglePacket}
    catch throw:Exception ->
        {error, Exception}
    end.

%% requires_statement_description(_Parameters) ->
%%     true. %pgo_protocol:bind_requires_statement_description(Parameters).

-spec pgsql_extended_query_receive_loop(extended_query_loop_state(), fun(), list(), list(), pgo:conn())
                                       -> pgo:result().
pgsql_extended_query_receive_loop(LoopState, Fun, Acc0, QueryOptions, Conn=#conn{socket=Socket}) ->
    case receive_message(Socket) of
        {ok, Message} ->
            pgsql_extended_query_receive_loop0(Message, LoopState, Fun, Acc0, QueryOptions, Conn);
        {error, _} = ReceiveError ->
            ReceiveError
    end.

pgsql_extended_query_receive_loop0(#parameter_status{name=_Name, value=_Value}, LoopState, Fun, Acc0, QueryOptions, Conn) ->
    %% State1 = handle_parameter(Name, Value, Conn),
    pgsql_extended_query_receive_loop(LoopState, Fun, Acc0, QueryOptions, Conn);
pgsql_extended_query_receive_loop0(#parse_complete{}, parse_complete, Fun, Acc0, QueryOptions, Conn) ->
    pgsql_extended_query_receive_loop(bind_complete, Fun, Acc0, QueryOptions, Conn);

%% Path where we ask the backend about what it expects.
%% We ignore row descriptions sent before bind as the format codes are null.
pgsql_extended_query_receive_loop0(#parse_complete{}, {parse_complete_with_params, Parameters}, Fun, Acc0, QueryOptions, Conn) ->
    pgsql_extended_query_receive_loop({parameter_description_with_params, Parameters}, Fun, Acc0, QueryOptions, Conn);
pgsql_extended_query_receive_loop0(#parameter_description{data_types=ParameterDataTypes}, {parameter_description_with_params, Parameters}, Fun, Acc0, QueryOptions, Conn=#conn{socket=Socket, pool=Pool}) ->
    pgo_query_cache:insert(Pool, get(query), ParameterDataTypes),
    oob_update_oid_map_if_required(Conn, ParameterDataTypes),
    PacketT = encode_bind_describe_execute(Parameters, ParameterDataTypes, Pool, true),
    case PacketT of
        {ok, SinglePacket} ->
            case gen_tcp:send(Socket, SinglePacket) of
                ok ->
                    pgsql_extended_query_receive_loop(pre_bind_row_description, Fun, Acc0, QueryOptions, Conn);
                {error, _} = SendError ->
                    SendError
            end;
        {error, _} = Error ->
            case gen_tcp:send(Socket, pgo_protocol:encode_sync_message()) of
                ok -> flush_until_ready_for_query(Error, Conn);
                {error, _} = SendSyncPacketError -> SendSyncPacketError
            end
    end;
pgsql_extended_query_receive_loop0(#row_description{}, pre_bind_row_description, Fun, Acc0, QueryOptions, Conn) ->
    pgsql_extended_query_receive_loop(bind_complete, Fun, Acc0, QueryOptions, Conn);
pgsql_extended_query_receive_loop0(#no_data{}, pre_bind_row_description, Fun, Acc0, QueryOptions, Conn) ->
    pgsql_extended_query_receive_loop(bind_complete, Fun, Acc0, QueryOptions, Conn);

% Common paths after bind.
pgsql_extended_query_receive_loop0(#bind_complete{}, bind_complete, Fun, Acc0, QueryOptions, Conn) ->
    pgsql_extended_query_receive_loop(row_description, Fun, Acc0, QueryOptions, Conn);
pgsql_extended_query_receive_loop0(#no_data{}, row_description, Fun, Acc0, QueryOptions, Conn) ->
    pgsql_extended_query_receive_loop(no_data, Fun, Acc0, QueryOptions, Conn);
pgsql_extended_query_receive_loop0(#row_description{fields = Fields}, row_description, Fun, Acc0, QueryOptions, Conn) ->
    oob_update_oid_map_from_fields_if_required(Conn, Fields),
    pgsql_extended_query_receive_loop({rows, Fields}, Fun, Acc0, QueryOptions, Conn);
pgsql_extended_query_receive_loop0(#data_row{values = Values}, {rows, Fields} = LoopState, Fun, Acc0, QueryOptions, Conn=#conn{pool=Pool}) ->
    DecodedRow = pgo_protocol:decode_row(Fields, Values, Pool, QueryOptions),
    pgsql_extended_query_receive_loop(LoopState, Fun, [Fun(DecodedRow, Fields) | Acc0], QueryOptions, Conn);
pgsql_extended_query_receive_loop0(#command_complete{command_tag = Tag}, _LoopState, Fun, Acc0, QueryOptions, Conn) ->
    {Command, NumRows} = decode_tag(Tag),
    pgsql_extended_query_receive_loop({result, #{command => Command,
                                                 num_rows => NumRows,
                                                 rows => lists:reverse(Acc0)}}, Fun, Acc0, QueryOptions, Conn);
%% pgsql_extended_query_receive_loop0(#portal_suspended{}, LoopState, Fun, Acc0, QueryOptions, Conn={_,S}) ->
%%     ExecuteMessage = pgo_protocol:encode_execute_message("", 0),
%%     FlushMessage = pgo_protocol:encode_flush_message(),
%%     SinglePacket = [ExecuteMessage, FlushMessage],
%%     case gen_tcp:send(S, SinglePacket) of
%%         ok -> pgsql_extended_query_receive_loop(LoopState, Fun, Acc0, QueryOptions, Conn);
%%         {error, _} = SendSinglePacketError ->
%%             SendSinglePacketError
%%     end;
pgsql_extended_query_receive_loop0(#ready_for_query{}, {result, Result}, _Fun, _Acc0, _QueryOptions, __Socket) ->
    Result;
pgsql_extended_query_receive_loop0(#error_response{fields = Fields}, LoopState, _Fun, _Acc0, _QueryOptions, Conn=#conn{socket=Socket}) ->
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
            case gen_tcp:send(Socket, pgo_protocol:encode_sync_message()) of
                ok -> flush_until_ready_for_query(Error, Conn);
                {error, _} = SendSyncPacketError -> SendSyncPacketError
            end;
        false ->
            flush_until_ready_for_query(Error, Conn)
    end;
pgsql_extended_query_receive_loop0(#ready_for_query{} = Message, _LoopState, _Fun, _Acc0, _QueryOptions, _Conn) ->
    Result = {error, {unexpected_message, Message}},
    Result;
pgsql_extended_query_receive_loop0(Message, _LoopState, _Fun, _Acc0, _QueryOptions, Conn=#conn{socket=Socket}) ->
    gen_tcp:send(Socket, pgo_protocol:encode_sync_message()),
    Error = {error, {unexpected_message, Message}},
    flush_until_ready_for_query(Error, Conn).

flush_until_ready_for_query(Result, Conn=#conn{socket=Socket}) ->
    case receive_message(Socket) of
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
receive_message(Socket) ->
    Result0 = case gen_tcp:recv(Socket, ?MESSAGE_HEADER_SIZE) of
                  {ok, <<Code:8/integer, Size:32/integer>>} ->
                      Payload = Size - 4,
                      case Payload of
                          0 ->
                              pgo_protocol:decode_message(Code, <<>>);
                          _ ->
                              case gen_tcp:recv(Socket, Payload) of
                                  {ok, Rest} ->
                                      pgo_protocol:decode_message(Code, Rest);
                                  {error, _} = ErrorRecvPacket ->
                                      ErrorRecvPacket
                              end
                      end;
                  {error, _} = ErrorRecvPacketHeader ->
                      ErrorRecvPacketHeader
              end,
    case Result0 of
        {ok, #notification_response{} = _Notification} ->
            receive_message(Socket);
        {ok, #notice_response{} = _Notice} ->
            receive_message(Socket);
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
    {commit, nil};
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

%%--------------------------------------------------------------------
%% @doc Update the OID Map out of band, opening a new connection.
%%
oob_update_oid_map_from_fields_if_required(Conn, Fields) ->
    OIDs = [OID || #row_description_field{data_type_oid = OID} <- Fields],
    oob_update_oid_map_if_required(Conn, OIDs).

oob_update_oid_map_if_required(Conn=#conn{pool=Pool}, OIDs) ->
    Required = lists:any(fun(OID) ->
                                 not ets:member(Pool, OID)
                         end, OIDs),
    case Required of
        true -> pgo_connection:reload_types(Conn);
        false -> ok
    end.
