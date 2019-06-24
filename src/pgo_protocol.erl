%% Mostly from the pgsql_protocol module in https://github.com/semiocast/pgsql
-module(pgo_protocol).

-include("pgo_internal.hrl").

-export([encode_startup_message/1,
         encode_ssl_request_message/0,
         encode_password_message/1,
         encode_query_message/1,
         encode_parse_message/3,
         encode_bind_message/5,
         encode_describe_message/2,
         encode_execute_message/2,
         encode_sync_message/0,
         encode_flush_message/0,
         encode_cancel_message/2,
         encode_copy_data_message/1,
         encode_copy_done/0,
         encode_copy_fail/1,
         decode_message/4,
         decode_row/4,
         bind_requires_statement_description/1,
         format_error/1]).

-define(PROTOCOL_VERSION_MAJOR, <<3:16/integer>>).
-define(PROTOCOL_VERSION_MINOR, <<0:16/integer>>).

format_error({parameters, Needed, Given}) ->
    io_lib:format("parameters needed for query not equal to number of arguments ~b != ~b", [Needed, Given]).

-spec encode_startup_message([{iodata(), iodata()}]) -> iolist().
encode_startup_message(Parameters) ->
    EncodedParams = [[Key, 0, Value, 0] || {Key, Value} <- Parameters],
    Packet = [?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR, EncodedParams, 0],
    Size = iolist_size(Packet) + 4,
    [<<Size:32/integer>>, Packet].

%%--------------------------------------------------------------------
%% @doc Encode the ssl request message.
%%
-spec encode_ssl_request_message() -> binary().
encode_ssl_request_message() ->
    <<8:32/integer, 1234:16/integer, 5679:16/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a password.
%%
-spec encode_password_message(iodata()) -> iolist().
encode_password_message(Password) ->
    encode_string_message($p, Password).

%%--------------------------------------------------------------------
%% @doc Encode a query.
%%
-spec encode_query_message(iodata()) -> iolist().
encode_query_message(Query) ->
    encode_string_message($Q, Query).

%%--------------------------------------------------------------------
%% @doc Encode a data segment of a COPY operation
%%
-spec encode_copy_data_message(iodata()) -> iolist().
encode_copy_data_message(Message) ->
    MessageLen = iolist_size(Message) + 4,
    [<<$d, MessageLen:32/integer>>, Message].

%%--------------------------------------------------------------------
%% @doc Encode the end of a COPY operation
%%
-spec encode_copy_done() -> binary().
encode_copy_done() ->
    <<$c, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode the cancellation of a COPY operation with the given
%%      failure message
%%
-spec encode_copy_fail(iodata()) -> iolist().
encode_copy_fail(ErrorMessage) ->
    encode_string_message($f, ErrorMessage).

%%--------------------------------------------------------------------
%% @doc Encode a parse message.
%%
-spec encode_parse_message(iodata(), iodata(), [pg_types:oid()]) -> iolist().
encode_parse_message(PreparedStatementName, Query, DataTypes) ->
    DataTypesBin = [<<DataTypeOid:32/integer>> || DataTypeOid <- DataTypes],
    DataTypesCount = length(DataTypes),
    Packet = [PreparedStatementName, <<0>>, Query, <<0>>,
              <<DataTypesCount:16/integer>>, DataTypesBin],
    PacketLen = iolist_size(Packet) + 4,
    [<<$P, PacketLen:32/integer>>, Packet].

%%--------------------------------------------------------------------
%% @doc Encode a bind message.
%%
-spec encode_bind_message(pg_pool:conn(), iodata(), iodata(), [any()], [pg_types:oid()]) -> iolist().
encode_bind_message(Conn=#conn{pool=Pool}, PortalName, StatementName, Parameters, ParametersDataTypes) ->
    ParametersCount = length(Parameters),
    ParametersCountBin = <<ParametersCount:16/integer>>,
    ParametersWithTypes =
        try lists:zipwith(fun(P, Oid) ->
                                  TypeInfo =
                                      case pg_types:lookup_type_info(Pool, Oid) of
                                          unknown_oid ->
                                              pgo_connection:reload_types(Conn),
                                              pg_types:lookup_type_info(Pool, Oid);
                                          T ->
                                              T
                                      end,
                                  {P, TypeInfo}
                          end, Parameters, ParametersDataTypes)
        catch
            error:function_clause:Stacktrace ->
                case length(ParametersDataTypes) =/= length(Parameters) of
                    true ->
                        error({?MODULE, {parameters, length(ParametersDataTypes), length(Parameters)}});
                    false ->
                        erlang:raise(error, function_clause, Stacktrace)
                end
        end,

    ParametersValues = [encode_parameter(Parameter, TypeInfo)
                        || {Parameter, TypeInfo} <- ParametersWithTypes],
    ParametersFormatsBin = [ParametersCountBin | [<<1:16/integer>> || _ <- ParametersValues]],
    Results = <<1:16/integer, 1:16/integer>>,   % We want all results in binary format.
    Packet = [PortalName, 0, StatementName, 0, ParametersFormatsBin,
              ParametersCountBin, ParametersValues, Results],
    PacketLen = iolist_size(Packet) + 4,
    [<<$B, PacketLen:32/integer>> | Packet].

%%--------------------------------------------------------------------
%% @doc Encode a parameter.
%% All parameters are currently encoded in text format except binaries that are
%% encoded as binaries.
%%
-spec encode_parameter(any(), pg_types:oid() | undefined) -> iodata().
encode_parameter(null, _Type) ->
    <<-1:32/integer>>;
encode_parameter(Parameter, TypeInfo) ->
    pg_types:encode(Parameter, TypeInfo).

%%--------------------------------------------------------------------
%% @doc Determine if we need the statement description with these parameters.
%% We currently only require statement descriptions if we have arrays of
%% binaries.
-spec bind_requires_statement_description([any()]) -> boolean().
bind_requires_statement_description([]) -> false;
bind_requires_statement_description([{array, [{array, SubArrayElems} | SubArrayT]} | Tail]) ->
    bind_requires_statement_description([{array, SubArrayElems}, {array, SubArrayT} | Tail]);
bind_requires_statement_description([{array, [ArrayElem | _]} | _]) when is_binary(ArrayElem) -> true;
bind_requires_statement_description([{array, [null | ArrayElemsT]} | Tail]) ->
    bind_requires_statement_description([{array, ArrayElemsT} | Tail]);
bind_requires_statement_description([{array, []} | Tail]) ->
    bind_requires_statement_description(Tail);
bind_requires_statement_description([_OtherParam | Tail]) ->
    bind_requires_statement_description(Tail).

%%--------------------------------------------------------------------
%% @doc Encode a describe message.
%%
-spec encode_describe_message(portal | statement, iodata()) -> iolist().
encode_describe_message(PortalOrStatement, Name) ->
    MessageLen = iolist_size(Name) + 6,
    WhatByte = case PortalOrStatement of
        portal -> $P;
        statement -> $S
    end,
    [<<$D, MessageLen:32/integer, WhatByte>>, Name, <<0>>].

%%--------------------------------------------------------------------
%% @doc Encode an execute message.
%%
-spec encode_execute_message(iodata(), non_neg_integer()) -> iolist().
encode_execute_message(PortalName, MaxRows) ->
    MessageLen = iolist_size(PortalName) + 9,
    [<<$E, MessageLen:32/integer>>, PortalName, <<0, MaxRows:32/integer>>].

%%--------------------------------------------------------------------
%% @doc Encode a sync message.
%%
-spec encode_sync_message() -> binary().
encode_sync_message() ->
    <<$S, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a flush message.
%%
-spec encode_flush_message() -> binary().
encode_flush_message() ->
    <<$H, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a flush message.
%%
-spec encode_cancel_message(integer(), integer()) -> binary().
encode_cancel_message(ProcID, Secret) ->
    <<16:32/integer, 80877102:32/integer, ProcID:32/integer, Secret:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a string message.
%%
-spec encode_string_message(byte(), iodata()) -> iolist().
encode_string_message(Identifier, String) ->
    MessageLen = iolist_size(String) + 5,
    [<<Identifier, MessageLen:32/integer>>, String, <<0>>].

%%--------------------------------------------------------------------
%% @doc Decode a message.
%%
-spec decode_message(byte(), binary(), pgo_pool:conn(), [pgo:decode_option()])
                    -> {ok, pgsql_backend_message()} | {error, any()}.
decode_message($R, Payload, _,  _DecodeOpts) -> decode_authentication_message(Payload);
decode_message($K, Payload, _,  _DecodeOpts) -> decode_backend_key_data_message(Payload);
decode_message($2, Payload, _,  _DecodeOpts) -> decode_bind_complete_message(Payload);
decode_message($3, Payload, _,  _DecodeOpts) -> decode_close_complete_message(Payload);
decode_message($C, Payload, _,  _DecodeOpts) -> decode_command_complete_message(Payload);
decode_message($d, Payload, _,  _DecodeOpts) -> decode_copy_data_message(Payload);
decode_message($c, Payload, _,  _DecodeOpts) -> decode_copy_done_message(Payload);
decode_message($G, Payload, _,  _DecodeOpts) -> decode_copy_in_response_message(Payload);
decode_message($H, Payload, _,  _DecodeOpts) -> decode_copy_out_response_message(Payload);
decode_message($W, Payload, _,  _DecodeOpts) -> decode_copy_both_response_message(Payload);
decode_message($D, Payload, _,  _DecodeOpts) -> decode_data_row_message(Payload);
decode_message($I, Payload, _,  _DecodeOpts) -> decode_empty_query_response_message(Payload);
decode_message($E, Payload, _,  _DecodeOpts) -> decode_error_response_message(Payload);
decode_message($V, Payload, _,  _DecodeOpts) -> decode_function_call_response_message(Payload);
decode_message($n, Payload, _,  _DecodeOpts) -> decode_no_data_message(Payload);
decode_message($N, Payload, _,  _DecodeOpts) -> decode_notice_response_message(Payload);
decode_message($A, Payload, _,  _DecodeOpts) -> decode_notification_response_message(Payload);
decode_message($t, Payload, _,  _DecodeOpts) -> decode_parameter_description_message(Payload);
decode_message($S, Payload, _,  _DecodeOpts) -> decode_parameter_status_message(Payload);
decode_message($1, Payload, _,  _DecodeOpts) -> decode_parse_complete_message(Payload);
decode_message($s, Payload, _,  _DecodeOpts) -> decode_portal_suspended_message(Payload);
decode_message($Z, Payload, _,  _DecodeOpts) -> decode_ready_for_query_message(Payload);
decode_message($T, Payload, Conn, DecodeOpts) -> decode_row_description_message(Payload, Conn, DecodeOpts);
decode_message(Other, _, _, _) ->
    {error, {unknown_message_type, Other}}.

decode_authentication_message(<<0:32/integer>>) ->
    {ok, #authentication_ok{}};
decode_authentication_message(<<2:32/integer>>) ->
    {ok, #authentication_kerberos_v5{}};
decode_authentication_message(<<3:32/integer>>) ->
    {ok, #authentication_cleartext_password{}};
decode_authentication_message(<<5:32/integer, Salt:4/binary>>) ->
    {ok, #authentication_md5_password{salt = Salt}};
decode_authentication_message(<<6:32/integer>>) ->
    {ok, #authentication_scm_credential{}};
decode_authentication_message(<<7:32/integer>>) ->
    {ok, #authentication_gss{}};
decode_authentication_message(<<9:32/integer>>) ->
    {ok, #authentication_sspi{}};
decode_authentication_message(<<8:32/integer, Rest/binary>>) ->
    {ok, #authentication_gss_continue{data = Rest}};
decode_authentication_message(Payload) ->
    {error, {unknown_message, authentication, Payload}}.

decode_backend_key_data_message(<<ProcID:32/integer, Secret:32/integer>>) ->
    {ok, #backend_key_data{procid = ProcID, secret = Secret}};
decode_backend_key_data_message(Payload) ->
    {error, {unknown_message, backend_key_data, Payload}}.

decode_bind_complete_message(<<>>) -> {ok, #bind_complete{}};
decode_bind_complete_message(Payload) ->
    {error, {unknown_message, bind_complete, Payload}}.

decode_close_complete_message(<<>>) -> {ok, #close_complete{}};
decode_close_complete_message(Payload) ->
    {error, {unknown_message, close_complete, Payload}}.

decode_command_complete_message(Payload) ->
    case decode_string(Payload) of
        {ok, String, <<>>} -> {ok, #command_complete{command_tag = String}};
        _ -> {error, {unknown_message, command_complete, Payload}}
    end.

decode_copy_data_message(Payload) -> {ok, #copy_data{data = Payload}}.

decode_copy_done_message(<<>>) -> {ok, #copy_done{}};
decode_copy_done_message(Payload) ->
    {error, {unknown_message, copy_done, Payload}}.

decode_copy_in_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} ->
            {ok, #copy_in_response{format=OverallFormat,
                                   columns=N,
                                   column_formats=ColumnFormats}};
        {error, _} ->
            {error, {unknow_message, copy_in_response, Payload}}
    end.

decode_copy_out_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} ->
            {ok, #copy_out_response{format=OverallFormat,
                                    columns=N,
                                    column_formats=ColumnFormats}};
        {error, _} ->
            {error, {unknow_message, copy_out_response, Payload}}
    end.

decode_copy_both_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} ->
            {ok, #copy_both_response{format=OverallFormat,
                                     columns=N,
                                     column_formats=ColumnFormats}};
        {error, _} ->
            {error, {unknow_message, copy_both_response, Payload}}
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

decode_empty_query_response_message(<<>>) -> {ok, #empty_query_response{}};
decode_empty_query_response_message(Payload) ->
    {error, {unknown_message, empty_query_response, Payload}}.

decode_error_response_message(Payload) ->
    case decode_error_and_notice_message_fields(Payload) of
        {ok, Fields} -> {ok, #error_response{fields = Fields}};
        {error, _} -> {error, {unknown_message, error_response, Payload}}
    end.

decode_function_call_response_message(<<-1:32/signed-integer>>) ->
    {ok, #function_call_response{value = null}};
decode_function_call_response_message(<<Len:32/integer, Value:Len/binary>>) ->
    {ok, #function_call_response{value = Value}};
decode_function_call_response_message(Payload) ->
    {error, {unknown_message, function_call_response, Payload}}.

decode_no_data_message(<<>>) -> {ok, #no_data{}};
decode_no_data_message(Payload) ->
    {error, {unknown_message, no_data, Payload}}.

decode_notice_response_message(Payload) ->
    case decode_error_and_notice_message_fields(Payload) of
        {ok, Fields} -> {ok, #notice_response{fields = Fields}};
        {error, _} -> {error, {unknown_message, notice_response, Payload}}
    end.

decode_notification_response_message(<<ProcID:32/integer, Rest0/binary>> = Payload) ->
    case decode_string(Rest0) of
        {ok, Channel, Rest1} ->
            case decode_string(Rest1) of
                {ok, PayloadStr, <<>>} ->
                    {ok, #notification_response{procid=ProcID,
                                                channel=Channel,
                                                payload=PayloadStr}};
                {error, _} ->
                    {error, {unknown_message, notification_response, Payload}}
            end;
        {error, _} ->
            {error, {unknown_message, notification_response, Payload}}
    end;
decode_notification_response_message(Payload) ->
    {error, {unknown_message, notification_response, Payload}}.

decode_parameter_description_message(<<Count:16/integer, Rest/binary>> = Payload) ->
    ParameterDataTypes = decode_parameter_data_types(Rest),
    if
        Count =:= length(ParameterDataTypes) ->
            {ok, #parameter_description{count=Count,
                                        data_types=ParameterDataTypes}};
        true ->
            {error, {unknown_message, parameter_description, Payload}}
    end;
decode_parameter_description_message(Payload) ->
    {error, {unknown_message, parameter_description, Payload}}.

decode_parameter_status_message(Payload) ->
    case decode_string(Payload) of
        {ok, Name, Rest0} ->
            case decode_string(Rest0) of
                {ok, Value, <<>>} ->
                    {ok, #parameter_status{name=Name,
                                           value=Value}};
                {error, _} ->
                    {error, {unknown_message, parameter_status, Payload}}
            end;
        {error, _} ->
            {error, {unknown_message, parameter_status, Payload}}
    end.

decode_parse_complete_message(<<>>) -> {ok, #parse_complete{}};
decode_parse_complete_message(Payload) ->
    {error, {unknown_message, parse_complete, Payload}}.

decode_portal_suspended_message(<<>>) -> {ok, #portal_suspended{}};
decode_portal_suspended_message(Payload) ->
    {error, {unknown_message, portal_suspended, Payload}}.

decode_ready_for_query_message(<<$I>>) -> {ok, #ready_for_query{transaction_status=idle}};
decode_ready_for_query_message(<<$T>>) -> {ok, #ready_for_query{transaction_status=transaction}};
decode_ready_for_query_message(<<$E>>) -> {ok, #ready_for_query{transaction_status=error}};
decode_ready_for_query_message(Payload) ->
    {error, {unknown_message, ready_for_query, Payload}}.

decode_row_description_message(<<Count:16/integer, Rest/binary>> = Payload, Conn, DecodeOpts) when Count >= 0 ->
    case decode_row_description_message0(Count, Rest, Conn, DecodeOpts, []) of
        {ok, Fields} ->
            {ok, #row_description{count = Count, fields = Fields}};
        {error, _} ->
            {error, {unknown_message, row_description, Payload}}
    end;
decode_row_description_message(Payload, _, _) ->
    {error, {unknown_message, row_description, Payload}}.

decode_row_description_message0(0, <<>>, _, _DecodeOpts, Acc) -> {ok, lists:reverse(Acc)};
decode_row_description_message0(Count, Binary, Conn=#conn{pool=Pool}, DecodeOpts, Acc) ->
    case decode_string(Binary) of
        {ok, FieldName, <<TableOid:32/integer, AttrNum:16/integer, DataTypeOid:32/integer,
                          DataTypeSize:16/integer, TypeModifier:32/integer, FormatCode:16/integer,
                          Tail/binary>>} ->
            case decode_format_code(FormatCode) of
                {ok, Format} ->
                    TypeInfo = case pg_types:lookup_type_info(Pool, DataTypeOid) of
                                   unknown_oid ->
                                       pgo_connection:reload_types(Conn),
                                       pg_types:lookup_type_info(Pool, DataTypeOid);
                                   T ->
                                       T
                               end,
                    Field = #row_description_field{
                        name = case proplists:get_value(column_name_as_atom, DecodeOpts, false) of
                                   true -> binary_to_atom(FieldName, utf8);
                                   _ -> FieldName
                               end,
                        type_info = TypeInfo,
                        table_oid = TableOid,
                        attr_number = AttrNum,
                        data_type_oid = DataTypeOid,
                        data_type_size = DataTypeSize,
                        type_modifier = TypeModifier,
                        format = Format},
                    decode_row_description_message0(Count - 1, Tail, Conn, DecodeOpts, [Field | Acc]);
                {error, _} = Error -> Error
            end;
        {error, _} = Error -> Error;
        _ -> {error, unknown_message}
    end.

%%% Helper functions.

decode_copy_response_message(<<Format:8/integer, N:16/integer, Rest/binary>>) when Format =:= 0
                                                                                   orelse Format =:= 1 ->
    {ok, OverallFormat} = decode_format_code(Format),
    if
        byte_size(Rest) =:= N * 2 ->
            case decode_format_codes(Rest) of
                {ok, ColumnFormats} ->
                    {ok, {OverallFormat, N, ColumnFormats}};
                {error, _} -> {error, column_formats}
            end;
        true ->
            {error, column_formats_size}
    end;
decode_copy_response_message(Payload) ->
    {error, {unknown_message, copy_response, Payload}}.

decode_error_and_notice_message_fields(Binary) ->
    decode_error_and_notice_message_fields0(Binary, []).

decode_error_and_notice_message_fields0(<<0>>, Acc) ->
    {ok, maps:from_list(Acc)};

decode_error_and_notice_message_fields0(<<FieldType, Rest0/binary>>, Acc) ->
    case decode_string(Rest0) of
        {ok, FieldString, Rest1} ->
            FieldTypeSym = decode_error_and_mention_field_type(FieldType),
            Field = {FieldTypeSym, FieldString},
            NewAcc = [Field | Acc],
            decode_error_and_notice_message_fields0(Rest1, NewAcc);
        {error, _} = Error -> Error
    end;
decode_error_and_notice_message_fields0(Bin, _Acc) -> {error, {badarg, Bin}}.

-spec decode_error_and_mention_field_type(byte()) -> pgsql_error:pgsql_error_and_mention_field_type().
decode_error_and_mention_field_type($S) -> severity;
decode_error_and_mention_field_type($C) -> code;
decode_error_and_mention_field_type($M) -> message;
decode_error_and_mention_field_type($D) -> detail;
decode_error_and_mention_field_type($H) -> hint;
decode_error_and_mention_field_type($P) -> position;
decode_error_and_mention_field_type($p) -> internal_position;
decode_error_and_mention_field_type($q) -> internal_query;
decode_error_and_mention_field_type($W) -> where;
decode_error_and_mention_field_type($s) -> schema;
decode_error_and_mention_field_type($t) -> table;
decode_error_and_mention_field_type($c) -> column;
decode_error_and_mention_field_type($d) -> data_type;
decode_error_and_mention_field_type($n) -> constraint;
decode_error_and_mention_field_type($F) -> file;
decode_error_and_mention_field_type($L) -> line;
decode_error_and_mention_field_type($R) -> routine;
decode_error_and_mention_field_type(Other) -> {unknown, Other}.

decode_parameter_data_types(Binary) ->
    decode_parameter_data_types0(Binary, []).

decode_parameter_data_types0(<<>>, Acc) -> lists:reverse(Acc);
decode_parameter_data_types0(<<Oid:32/integer, Tail/binary>>, Acc) ->
    decode_parameter_data_types0(Tail, [Oid | Acc]).

-spec decode_format_code(integer()) -> {ok, pgsql_format()} | {error, any()}.
decode_format_code(0) -> {ok, text};
decode_format_code(1) -> {ok, binary};
decode_format_code(_Other) -> {error, unknown_format_code}.

-spec decode_format_codes(binary()) -> {ok, [pgsql_format()]} | {error, any()}.
decode_format_codes(Binary) ->
    decode_format_codes0(Binary, []).

decode_format_codes0(<<FormatCode:16/integer, Tail/binary>>, Acc) ->
    case decode_format_code(FormatCode) of
        {ok, Format} ->
            decode_format_codes0(Tail, [Format | Acc]);
        {error, _} = Error -> Error
    end;
decode_format_codes0(<<>>, Acc) -> {ok, lists:reverse(Acc)}.

-spec decode_string(binary()) -> {ok, binary(), binary()} | {error, not_null_terminated}.
decode_string(Binary) ->
    case binary:match(Binary, <<0>>) of
        nomatch -> {error, not_null_terminated};
        {Position, 1} ->
            {String, <<0, Rest/binary>>} = split_binary(Binary, Position),
            {ok, String, Rest}
    end.

%%--------------------------------------------------------------------
%% @doc Decode a row format.
%%
-spec decode_row([#row_description_field{}], [binary()], atom(), proplists:proplist()) -> tuple().
decode_row(Descs, Values, OIDMap, DecodeOptions) ->
    case proplists:get_bool(return_rows_as_maps, DecodeOptions) of
        false ->
            decode_row0(Descs, Values, OIDMap, DecodeOptions, []);
        true ->
            decode_row_as_map(Descs, Values, OIDMap, DecodeOptions, #{})
    end.

decode_row_as_map([Desc=#row_description_field{name=Name} | DescsT], [Value | ValuesT], OIDMap, DecodeOptions, Acc) ->
    DecodedValue = decode_value(Desc, Value, OIDMap, DecodeOptions),
    decode_row_as_map(DescsT, ValuesT, OIDMap, DecodeOptions, Acc#{Name => DecodedValue});
decode_row_as_map([], [], _OIDMap, _DecodeOptions, Acc) ->
    Acc.

decode_row0([Desc | DescsT], [Value | ValuesT], OIDMap, DecodeOptions, Acc) ->
    DecodedValue = decode_value(Desc, Value, OIDMap, DecodeOptions),
    decode_row0(DescsT, ValuesT, OIDMap, DecodeOptions, [DecodedValue | Acc]);
decode_row0([], [], _OIDMap, _DecodeOptions, Acc) ->
    list_to_tuple(lists:reverse(Acc)).

decode_value(_Desc, null, _OIDMap, _DecodeOptions) -> null;
decode_value(#row_description_field{type_info=TypeInfo,
                                    data_type_oid=_DataTypeOID,
                                    format=binary}, Value, _OIDMap, _DecodeOptions) ->
    pg_types:decode(Value, TypeInfo);
decode_value(#row_description_field{format = text}, _Value, _OIDMap, _DecodeOptions) ->
    throw(no_text_format_support).
